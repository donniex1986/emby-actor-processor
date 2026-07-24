# routes/media.py

from flask import Blueprint, request, jsonify, Response, stream_with_context, send_file
import logging
import os
import json
import requests

import handler.emby as emby
import config_manager
import constants
import task_manager
import extensions
from database import custom_collection_db, media_db, user_db, request_db, settings_db
import handler.moviepilot as moviepilot
from extensions import admin_required, processor_ready_required
from handler.hdhive_client import HDHiveClient
from handler.shared_center_client import shared_center_enabled
from urllib.parse import urlparse, urlsplit, urlunsplit, parse_qsl, urlencode

# --- 蓝图 1：用于所有 /api/... 的路由 ---
media_api_bp = Blueprint('media_api', __name__, url_prefix='/api')

# --- 蓝图 2：用于不需要 /api 前缀的路由 ---
media_proxy_bp = Blueprint('media_proxy', __name__)

logger = logging.getLogger(__name__)


def _disable_mp_subscribe_assistant() -> None:
    config = settings_db.get_setting('mp_config') or {}
    assistant = config.get('subscribe_assistant')
    if isinstance(assistant, dict) and assistant.get('enabled'):
        assistant['enabled'] = False
        settings_db.save_setting('mp_config', config)


def _available_subscription_sources() -> set:
    sources = set()
    mp_config = settings_db.get_setting('mp_config') or {}
    if mp_config.get('moviepilot_url'):
        sources.add('mp')
    try:
        if HDHiveClient().ping():
            sources.add('hdhive')
    except Exception:
        pass
    tg_cfg = settings_db.get_setting('tg_userbot_config') or {}
    if tg_cfg.get('enabled') and tg_cfg.get('channels'):
        sources.add('tg_channel')
    try:
        if shared_center_enabled():
            sources.add('shared_pool')
    except Exception:
        pass
    return sources

def _mask_url_query_secret(url: str) -> str:
    try:
        parts = urlsplit(str(url or ''))
        query = urlencode(
            [(key, '********' if key.lower() in {'api_key', 'x-emby-token'} else value)
             for key, value in parse_qsl(parts.query, keep_blank_values=True)]
        )
        return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))
    except Exception:
        return str(url or '').replace('api_key=', 'api_key=********')

@media_api_bp.route('/search_emby_library', methods=['GET'])
@processor_ready_required
def api_search_emby_library():
    query = request.args.get('query', '')
    if not query.strip():
        return jsonify({"error": "搜索词不能为空"}), 400

    try:
        from database import connection
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # ★★★ 核心修复：从 emby_item_ids_json 提取 ID，并用 LEFT JOIN 关联资产信息 ★★★
            # 使用 WITH ORDINALITY 按数组索引对齐 ID 和 资产，完美兼容电影多版本和剧集容器
            sql = """
                SELECT 
                    m.tmdb_id, 
                    m.item_type, 
                    m.title, 
                    m.release_year,
                    eid.emby_id,
                    a.asset->>'resolution_display' as resolution,
                    a.asset->>'quality_display' as quality
                FROM media_metadata m
                -- 1. 展开 emby_item_ids_json 获取真实的 Emby ID 和 索引
                JOIN LATERAL jsonb_array_elements_text(
                    CASE WHEN jsonb_typeof(m.emby_item_ids_json) = 'array' THEN m.emby_item_ids_json ELSE '[]'::jsonb END
                ) WITH ORDINALITY AS eid(emby_id, idx) ON true
                -- 2. 左连接 asset_details_json 获取对应的分辨率信息 (按索引匹配)
                LEFT JOIN LATERAL (
                    SELECT asset FROM jsonb_array_elements(
                        CASE WHEN jsonb_typeof(m.asset_details_json) = 'array' THEN m.asset_details_json ELSE '[]'::jsonb END
                    ) WITH ORDINALITY AS arr(asset, asset_idx)
                    WHERE arr.asset_idx = eid.idx
                ) AS a ON true
                WHERE m.title ILIKE %s OR m.original_title ILIKE %s
                LIMIT 50
            """
            search_term = f"%{query}%"
            cursor.execute(sql, (search_term, search_term))
            rows = cursor.fetchall()

        formatted_results = []
        for row in rows:
            if not row['emby_id']: continue
            
            # 拼接版本信息到名字里，例如：阿凡达 [4k BluRay] (剧集因为没有 resolution，所以不会拼接)
            version_tag = f" [{row['resolution']} {row['quality']}]" if row['resolution'] else ""
            
            formatted_results.append({
                "item_id": row['emby_id'],
                "item_name": f"{row['title']}{version_tag}",
                "item_type": row['item_type'],
                "failed_at": None,
                "error_message": f"本地数据库搜索结果 (年份: {row['release_year']})",
                "score": None,
                "provider_ids": {"Tmdb": row['tmdb_id']} 
            })
        
        return jsonify({
            "items": formatted_results,
            "total_items": len(formatted_results)
        })

    except Exception as e:
        logger.error(f"API /api/search_emby_library Error: {e}", exc_info=True)
        return jsonify({"error": "搜索时发生未知服务器错误"}), 500

@media_api_bp.route('/media_for_editing/<item_id>', methods=['GET'])
@admin_required
@processor_ready_required
def api_get_media_for_editing(item_id):
    # 直接调用 core_processor 的新方法
    data_for_editing = extensions.media_processor_instance.get_cast_for_editing(item_id)
    
    if data_for_editing:
        return jsonify(data_for_editing)
    else:
        return jsonify({"error": f"无法获取项目 {item_id} 的编辑数据，请检查日志。"}), 404

@media_api_bp.route('/update_media_cast_sa/<item_id>', methods=['POST'])
@admin_required
@processor_ready_required
def api_update_edited_cast_sa(item_id):
    from tasks.media import task_manual_update
    data = request.json
    if not data or "cast" not in data or not isinstance(data["cast"], list):
        return jsonify({"error": "请求体中缺少有效的 'cast' 列表"}), 400
    
    edited_cast = data["cast"]
    item_name = data.get("item_name", f"未知项目(ID:{item_id})")

    task_manager.submit_task(
        task_manual_update, # 传递包装函数
        f"手动更新: {item_name}",
        processor_type='media',
        item_id=item_id,
        manual_cast_list=edited_cast,
        item_name=item_name
        
    )
    
    return jsonify({"message": "手动更新任务已在后台启动。"}), 202

# ▼▼▼ 通用外部图片代理接口 ▼▼▼
@media_api_bp.route('/image_proxy', methods=['GET'])
def proxy_external_image():
    """
    一个安全的通用外部图片代理。
    【V3 - 代理适配版】增加了对系统全局代理的支持，解决 TMDb 图片连接重置问题。
    """
    cached_hash = request.args.get('cache')
    if cached_hash:
        from handler.media_image_cache import get_cached_image

        cached = get_cached_image(cached_hash)
        if not cached:
            return jsonify({"error": "图片缓存不存在"}), 404
        response = send_file(
            cached["path"],
            mimetype=cached.get("mime_type") or "application/octet-stream",
            conditional=True,
        )
        response.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
        return response

    external_url = request.args.get('url')
    if not external_url:
        return jsonify({"error": "缺少 'url' 参数"}), 400

    try:
        # 1. 获取程序配置
        current_config = config_manager.APP_CONFIG
        user_agent = current_config.get('user_agent', 'Mozilla/5.0')

        # 2. ★★★ 核心修复：获取系统配置的代理 ★★★
        proxies = config_manager.get_proxies_for_requests()

        # 3. 构造请求头
        parsed_url = urlparse(external_url)
        headers = {
            'User-Agent': user_agent,
            'Referer': f"{parsed_url.scheme}://{parsed_url.netloc}/"
        }
        
        logger.debug(f"  ➜ 代理请求外部图片: URL='{external_url}', 使用代理={bool(proxies)}")

        # 4. 带着代理和伪装头去请求
        # 增加 verify=False 可以防止某些代理抓包导致的 SSL 报错（可选）
        response = requests.get(
            external_url, 
            stream=True, 
            timeout=15, 
            headers=headers, 
            proxies=proxies
        )

        response.raise_for_status()

        return Response(
            stream_with_context(response.iter_content(chunk_size=8192)),
            content_type=response.headers.get('Content-Type'),
            status=response.status_code
        )
    except requests.exceptions.RequestException as e:
        # 这里的报错就是你看到的那个
        logger.error(f"通用图片代理错误: 无法获取 URL '{external_url}'. 错误: {e}")
        # 返回一个占位图，省得前端裂图
        return Response(
            b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82',
            mimetype='image/png',
            status=404
        )

# 图片代理路由
@media_proxy_bp.route('/image_proxy/<path:image_path>')
@processor_ready_required
def proxy_emby_image(image_path):
    """
    一个安全的、动态的 Emby 图片代理。
    【V2 - 完整修复版】确保 api_key 作为 URL 参数传递，适用于所有图片类型。
    """
    try:
        emby_url = extensions.media_processor_instance.emby_url.rstrip('/')
        emby_api_key = extensions.media_processor_instance.emby_api_key

        # 1. 构造基础 URL，包含路径和原始查询参数
        query_string = request.query_string.decode('utf-8')
        target_url = f"{emby_url}/{image_path}"
        if query_string:
            target_url += f"?{query_string}"
        
        # 2. ★★★ 核心修复：将 api_key 作为 URL 参数追加 ★★★
        # 判断是使用 '?' 还是 '&' 来追加 api_key
        separator = '&' if '?' in target_url else '?'
        target_url_with_key = f"{target_url}{separator}api_key={emby_api_key}"
        
        logger.trace(f"代理图片请求 (最终URL): {_mask_url_query_secret(target_url_with_key)}")

        # 3. 发送请求
        emby_response = requests.get(target_url_with_key, stream=True, timeout=20)
        emby_response.raise_for_status()

        # 4. 将 Emby 的响应流式传输回浏览器
        return Response(
            stream_with_context(emby_response.iter_content(chunk_size=8192)),
            content_type=emby_response.headers.get('Content-Type'),
            status=emby_response.status_code
        )
    except Exception as e:
        logger.error(f"代理 Emby 图片时发生严重错误: {e}", exc_info=True)
        # 返回一个1x1的透明像素点作为占位符，避免显示大的裂图图标
        return Response(
            b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82',
            mimetype='image/png'
        )
    
# ✨✨✨ 一键翻译 ✨✨✨
@media_api_bp.route('/actions/translate_cast_sa', methods=['POST']) # 注意路径不同
@admin_required
@processor_ready_required
def api_translate_cast_sa():
    data = request.json
    current_cast = data.get('cast')
    if not isinstance(current_cast, list):
        return jsonify({"error": "请求体必须包含 'cast' 列表。"}), 400

    # 【★★★ 从请求中获取所有需要的上下文信息 ★★★】
    title = data.get('title')
    year = data.get('year')

    try:
        # 【★★★ 调用新的、需要完整上下文的函数 ★★★】
        translated_list = extensions.media_processor_instance.translate_cast_list_for_editing(
            cast_list=current_cast,
            title=title,
            year=year,
        )
        return jsonify(translated_list)
    except Exception as e:
        logger.error(f"一键翻译演员列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在翻译时发生内部错误。"}), 500
    
# ✨✨✨ 预览处理后的演员表 ✨✨✨
@media_api_bp.route('/preview_processed_cast/<item_id>', methods=['POST'])
@processor_ready_required
def api_preview_processed_cast(item_id):
    """
    一个轻量级的API，用于预览单个媒体项经过核心处理器处理后的演员列表。
    它只返回处理结果，不执行任何数据库更新或Emby更新。
    """
    logger.info(f"API: 收到为 ItemID {item_id} 预览处理后演员的请求。")

    # 步骤 1: 获取当前媒体的 Emby 详情
    try:
        item_details = emby.get_emby_item_details(
            item_id,
            extensions.media_processor_instance.emby_url,
            extensions.media_processor_instance.emby_api_key,
            extensions.media_processor_instance.emby_user_id
        )
        if not item_details:
            return jsonify({"error": "无法获取当前媒体的Emby详情"}), 404
    except Exception as e:
        logger.error(f"API /preview_processed_cast: 获取Emby详情失败 for ID {item_id}: {e}", exc_info=True)
        return jsonify({"error": f"获取Emby详情时发生错误: {e}"}), 500

    # 步骤 2: 调用核心处理方法
    try:
        current_emby_cast_raw = item_details.get("People", [])
        
        # 直接调用 MediaProcessor 的核心方法
        processed_cast_result = extensions.media_processor_instance._process_cast_list(
            current_emby_cast_people=current_emby_cast_raw,
            media_info=item_details
        )
        
        # 步骤 3: 将处理结果转换为前端友好的格式
        # processed_cast_result 的格式是内部格式，我们需要转换为前端期望的格式
        # (embyPersonId, name, role, imdbId, doubanId, tmdbId)
        
        cast_for_frontend = []
        for actor_data in processed_cast_result:
            cast_for_frontend.append({
                "embyPersonId": actor_data.get("EmbyPersonId"),
                "name": actor_data.get("Name"),
                "role": actor_data.get("Role"),
                "imdbId": actor_data.get("ImdbId"),
                "doubanId": actor_data.get("DoubanCelebrityId"),
                "tmdbId": actor_data.get("TmdbPersonId"),
                "matchStatus": "已刷新" # 可以根据 actor_data['_source_comment'] 提供更详细的状态
            })

        logger.info(f"API: 成功为 ItemID {item_id} 预览了处理后的演员列表，返回 {len(cast_for_frontend)} 位演员。")
        return jsonify(cast_for_frontend)

    except Exception as e:
        logger.error(f"API /preview_processed_cast: 调用 _process_cast_list 时发生错误 for ID {item_id}: {e}", exc_info=True)
        return jsonify({"error": "在服务器端处理演员列表时发生内部错误"}), 500   
    
# --- 获取emby媒体库 ---
@media_api_bp.route('/emby_libraries')
def api_get_emby_libraries():
    if not extensions.media_processor_instance or \
       not extensions.media_processor_instance.emby_url or \
       not extensions.media_processor_instance.emby_api_key:
        return jsonify({"error": "Emby配置不完整或服务未就绪"}), 500

    # 调用通用的函数，它会返回完整的列表
    full_libraries_list = emby.get_emby_libraries(
        extensions.media_processor_instance.emby_url,
        extensions.media_processor_instance.emby_api_key,
        extensions.media_processor_instance.emby_user_id
    )

    if full_libraries_list is not None:
        # 过滤掉不需要的类型：音乐(music)、合集(boxsets)、播放列表(playlists)
        excluded_types = ['music', 'boxsets', 'playlists']
        simplified_libraries = [
            {'Name': item.get('Name'), 'Id': item.get('Id')}
            for item in full_libraries_list
            if item.get('Name') and item.get('Id') and item.get('CollectionType') not in excluded_types
        ]
        return jsonify(simplified_libraries)
    else:
        return jsonify({"error": "无法获取Emby媒体库列表，请检查连接和日志"}), 500
    
# --- 获取emby媒体库（反代用） ---
@media_api_bp.route('/emby/user/<user_id>/views', methods=['GET'])
def api_get_emby_user_views(user_id):
    """
    从真实Emby服务器获取指定用户的所有原生媒体库（Views）。
    需要在请求头或查询参数中携带 API Key。
    """
    if not extensions.media_processor_instance or \
       not extensions.media_processor_instance.emby_url:
        logger.warning("/api/emby/user/<user_id>/views: Emby配置不完整或服务未就绪。")
        return jsonify({"error": "Emby配置不完整或服务未就绪"}), 500
    
    # 尝试从请求头和查询参数获取用户令牌。配置页拿到的敏感字段可能是脱敏占位符，
    # 这种情况下必须回退到服务端真实配置，不能把 ******** 透传给 Emby。
    user_token = str(request.headers.get('X-Emby-Token') or request.args.get('api_key') or '').strip()
    if user_token and set(user_token) == {'*'}:
        user_token = ''
    if not user_token:
        user_token = str(getattr(extensions.media_processor_instance, 'emby_api_key', '') or '').strip()
    if not user_token:
        user_token = str((config_manager.APP_CONFIG or {}).get(constants.CONFIG_OPTION_EMBY_API_KEY) or '').strip()
    
    if not user_token:
        return jsonify({"error": "缺少用户访问令牌(api_key或X-Emby-Token)"}), 400
    
    base_url = extensions.media_processor_instance.emby_url.rstrip('/')
    real_views_url = f"{base_url}/emby/Users/{user_id}/Views"
    
    try:
        # 复制请求头，剔除不必要的
        headers = {k: v for k, v in request.headers if k.lower() not in ['host', 'accept-encoding']}
        headers['Host'] = urlparse(base_url).netloc
        headers['Accept-Encoding'] = 'identity'
        headers['X-Emby-Token'] = user_token  # 确保Token传递
        
        params = request.args.to_dict()
        params['api_key'] = user_token  # 兼容api_key参数
        
        resp = requests.get(real_views_url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        
        views_data = resp.json()
        return jsonify(views_data)
    
    except requests.exceptions.RequestException as e:
        logger.error(f"/api/emby/user/{user_id}/views 调用真实Emby失败: {e}")
        return jsonify({"error": "无法从真实Emby服务器获取数据"}), 502
    except Exception as e:
        logger.error(f"/api/emby/user/{user_id}/views 发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500 

# ★★★ 提供工作室远程搜索的API ★★★
@media_api_bp.route('/search_studios', methods=['GET'])
@admin_required
def api_search_studios():
    """
    根据查询参数 'q' 动态搜索工作室列表。
    """
    search_term = request.args.get('q', '').strip()
    
    if not search_term:
        return jsonify([])
        
    try:
        studios = custom_collection_db.search_unique_studios(search_term)
        return jsonify(studios)
    except Exception as e:
        logger.error(f"搜索工作室时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# ======================================================================
# ★★★ 通用状态操作 API ★★★
# ======================================================================

@media_api_bp.route('/subscription/status', methods=['POST'])
@admin_required
def api_unified_subscription_status():
    """
    统一处理所有媒体项状态变更的唯一入口。
    """
    data = request.json
    requests_list = data.get('requests')

    # 参数校验
    if not isinstance(requests_list, list) or not requests_list:
        return jsonify({"error": "'requests' 必须是一个非空列表"}), 400

    processed_count = 0
    errors = []
    
    # 定义允许的状态
    ALLOWED_STATUSES = ['WANTED', 'SUBSCRIBED', 'IGNORED', 'NONE', 'PENDING_RELEASE']

    for req in requests_list:
        tmdb_id = req.get('tmdb_id')
        item_type = req.get('item_type')
        new_status = req.get('new_status')
        
        if not all([tmdb_id, item_type, new_status]):
            errors.append(f"无效请求项，缺少 tmdb_id, item_type 或 new_status: {req}")
            continue
            
        if new_status.upper() not in ALLOWED_STATUSES:
            errors.append(f"无效的状态 '{new_status}' for TMDb ID {tmdb_id}")
            continue

        try:
            # ==================================================================
            # ★★★ 核心修复：统一处理 MoviePilot 的取消订阅逻辑 ★★★
            # 无论是转为 NONE 还是 IGNORED，只要之前是 SUBSCRIBED，都要取消
            # ==================================================================
            if new_status.upper() in ['NONE', 'IGNORED']:
                # 1. 先查当前状态
                media_details_map = media_db.get_media_details_by_tmdb_ids([tmdb_id])
                current_details = media_details_map.get(tmdb_id, {})
                current_status = current_details.get('subscription_status')
                
                # 2. 如果当前是已订阅，则执行取消操作
                if current_status == 'SUBSCRIBED':
                    logger.info(f"  ➜ 检测到已订阅项 (TMDb ID: {tmdb_id}) 转为 {new_status}，正在取消 MoviePilot 订阅...")
                    
                    # 智能判断要发给 MoviePilot 的真实 ID
                    id_for_mp = tmdb_id 
                    season_for_mp = None 

                    if item_type == 'Season':
                        parent_id = current_details.get('parent_series_tmdb_id')
                        season_num = current_details.get('season_number')
                        
                        if parent_id and season_num is not None:
                            id_for_mp = parent_id
                            season_for_mp = season_num
                        else:
                            error_msg = f"处理季 (TMDb ID: {tmdb_id}) 失败：无法找到父剧集ID或季号。"
                            errors.append(error_msg)
                            logger.error(f"API /subscription/status: {error_msg}")
                            continue 
                    
                    # 检查是否配置了 MP
                    mp_config = settings_db.get_setting('mp_config') or {}
                    if mp_config.get('moviepilot_url'):
                        config = config_manager.APP_CONFIG
                        if not moviepilot.cancel_subscription(id_for_mp, item_type, config, season_for_mp):
                            error_msg = f"处理 TMDb ID {tmdb_id} 失败：MoviePilot 取消订阅失败。"
                            errors.append(error_msg)
                            logger.error(f"API /subscription/status: {error_msg}")
                            continue
                        else:
                            logger.info(f"  ➜ MoviePilot 订阅已取消 (ID: {id_for_mp})")
                    else:
                        logger.info(f"  ➜ 未配置 MoviePilot，跳过取消 MoviePilot 订阅 (ID: {id_for_mp})")

            # ==================================================================
            # 本地数据库状态更新
            # ==================================================================
            if new_status.upper() == 'NONE':
                request_db.set_media_status_none(
                    tmdb_ids=[tmdb_id], item_type=item_type
                )
                processed_count += 1

            elif new_status.upper() == 'IGNORED':
                source = req.get('source', {"type": "manual_ignore"})
                ignore_reason = req.get('ignore_reason')
                if not ignore_reason:
                    ignore_reason = '手动忽略'

                request_db.set_media_status_ignored(
                    tmdb_ids=[tmdb_id], item_type=item_type, source=source, media_info_list=[req],
                    ignore_reason=ignore_reason
                )
                processed_count += 1

            elif new_status.upper() == 'WANTED':
                source = req.get('source', {"type": "manual_add"})
                force_unignore = req.get('force_unignore', False)
                request_db.set_media_status_wanted(
                    tmdb_ids=[tmdb_id], item_type=item_type, source=source, media_info_list=[req],
                    force_unignore=force_unignore
                )
                processed_count += 1

            elif new_status.upper() == 'SUBSCRIBED':
                source = req.get('source', {"type": "manual_subscribe"})
                request_db.set_media_status_subscribed(
                    tmdb_ids=[tmdb_id], item_type=item_type, source=source, media_info_list=[req]
                )
                # 尝试恢复 MP 订阅状态 (S -> R)
                try:
                    mp_config = settings_db.get_setting('mp_config') or {}
                    if mp_config.get('moviepilot_url'):
                        config = config_manager.APP_CONFIG
                        mp_tmdb_id = tmdb_id
                        mp_season = None
                        
                        media_details_map = media_db.get_media_details_by_tmdb_ids([tmdb_id])
                        details = media_details_map.get(tmdb_id, {})
                        
                        if item_type == 'Season':
                            if details.get('parent_series_tmdb_id'):
                                mp_tmdb_id = details['parent_series_tmdb_id']
                                mp_season = details.get('season_number')

                        if not moviepilot.update_subscription_status(int(mp_tmdb_id), mp_season, 'R', config):
                            logger.warning(f"  ➜ [状态同步] 切换 MP 状态失败，尝试重新提交订阅...")
                            payload = {
                                "tmdbid": int(mp_tmdb_id),
                                "type": "电影" if item_type == 'Movie' else "电视剧"
                            }
                            if mp_season is not None:
                                payload['season'] = mp_season
                                # ★★★ 核心修复：补充剧集名称 ★★★
                                series_name = media_db.get_series_title_by_tmdb_id(str(mp_tmdb_id))
                                if series_name:
                                    payload['name'] = series_name
                            elif item_type == 'Movie':
                                payload['name'] = details.get('title', '')
                                
                            moviepilot.subscribe_with_custom_payload(payload, config)
                        else:
                            logger.info(f"  ➜ [状态同步] 已通知 MP 恢复搜索: {mp_tmdb_id}")
                    else:
                        logger.info(f"  ➜ 未配置 MoviePilot，跳过恢复 MoviePilot 订阅状态 (ID: {tmdb_id})")

                except Exception as e_sync:
                    logger.error(f"  ➜ [状态同步] 恢复 MoviePilot 订阅状态时出错: {e_sync}")
                
                processed_count += 1

        except Exception as e:
            error_msg = f"处理 TMDb ID {tmdb_id} 状态变更时发生错误: {e}"
            errors.append(error_msg)
            logger.error(f"API /subscription/status 发生错误: {error_msg}", exc_info=True)

    if processed_count > 0:
        message = f"已成功提交 {processed_count} 个媒体项的状态变更请求。"
        if errors:
            message += f" 但有 {len(errors)} 个请求处理失败。"
        return jsonify({"message": message, "errors": errors}), 200
    else:
        return jsonify({"error": "没有有效的媒体项被成功处理。", "errors": errors}), 400

@media_api_bp.route('/subscriptions/all', methods=['GET'])
@admin_required
def api_get_all_subscriptions_for_management():
    """
    为前端“统一订阅”页面提供所有有订阅状态媒体项的数据。
    """
    try:
        # 1. 从数据库获取原始数据
        items = media_db.get_all_subscriptions()

        # 遍历每个媒体项，处理其来源信息
        for item in items:
            sources = item.get('subscription_sources_json')
            if isinstance(sources, list):
                for source in sources:
                    # 如果来源是用户请求，并且有 user_id
                    if source.get('type') == 'user_request' and (user_id := source.get('user_id')):
                        # 根据 user_id 查询用户名，并将其添加到 source 字典中
                        # 使用 'user' 作为键名，以匹配前端已有的逻辑
                        source['user'] = user_db.get_username_by_id(user_id) or '未知用户'

        # 3. 返回增强后的数据
        return jsonify(items)
    except Exception as e:
        logger.error(f"API /subscriptions/all 获取数据失败: {e}", exc_info=True)
        return jsonify({"error": "获取订阅列表时发生服务器内部错误"}), 500

@media_api_bp.route('/media/batch_delete', methods=['POST'])
@admin_required
def api_batch_delete_media():
    """
    接收包含 {tmdb_id, item_type} 的列表，从数据库物理删除这些记录。
    """
    data = request.json
    items_to_delete = data.get('items')

    if not isinstance(items_to_delete, list) or not items_to_delete:
        return jsonify({"error": "请求体必须包含非空的 'items' 列表"}), 400

    try:
        deleted_count = media_db.delete_media_metadata_batch(items_to_delete)
        logger.info(f"  ➜ 已物理删除 {deleted_count} 条媒体元数据记录。")
        return jsonify({
            "message": f"成功删除了 {deleted_count} 条记录。",
            "deleted_count": deleted_count
        })
    except Exception as e:
        logger.error(f"API /media/batch_delete 发生错误: {e}", exc_info=True)
        return jsonify({"error": "删除操作发生内部错误"}), 500
    
@media_api_bp.route('/subscription/strategy', methods=['GET'])
@admin_required
def api_get_subscription_strategy():
    """获取订阅策略配置"""
    try:
        from database import settings_db
        config = settings_db.get_setting('subscription_strategy_config')
        
        # 默认配置
        default_config = {
            'subscription_sources': ['shared_pool', 'hdhive', 'tg_channel', 'mp']
        }
        
        if not config:
            config = default_config
        else:
            # 兼容老数据，补全缺失字段
            for k, v in default_config.items():
                if k not in config:
                    config[k] = v
                
        return jsonify(config)
    except Exception as e:
        logger.error(f"获取订阅策略失败: {e}")
        return jsonify({"error": "获取配置失败"}), 500

@media_api_bp.route('/subscription/strategy', methods=['POST'])
@admin_required
def api_save_subscription_strategy():
    """保存订阅策略配置"""
    try:
        from database import settings_db
        data = request.json
        # 简单的校验
        if not isinstance(data, dict):
            return jsonify({"error": "无效的配置格式"}), 400

        sources = data.get('subscription_sources')
        if isinstance(sources, list):
            available_sources = _available_subscription_sources()
            data['subscription_sources'] = [
                source for source in sources
                if source in available_sources
            ]
            if 'mp' not in data['subscription_sources']:
                _disable_mp_subscribe_assistant()
            
        settings_db.save_setting('subscription_strategy_config', data)
        return jsonify({"message": "策略配置已保存"})
    except Exception as e:
        logger.error(f"保存订阅策略失败: {e}")
        return jsonify({"error": "保存配置失败"}), 500
    
@media_api_bp.route('/auto_tagging/rules', methods=['GET'])
@admin_required
def get_tagging_rules():
    rules = settings_db.get_setting('auto_tagging_rules') or []
    return jsonify(rules)

@media_api_bp.route('/auto_tagging/rules', methods=['POST'])
@admin_required
def save_tagging_rules():
    rules = request.json
    settings_db.save_setting('auto_tagging_rules', rules)
    return jsonify({"message": "配置已保存"})

@media_api_bp.route('/auto_tagging/run_now', methods=['POST'])
@admin_required
@processor_ready_required
def run_tagging_now():
    from tasks.media import task_bulk_auto_tag
    data = request.json
    lib_ids = data.get('library_ids') 
    tags = data.get('tags')
    # ★★★ 新增：接收分级筛选参数 ★★★
    rating_filters = data.get('rating_filters') 
    lib_name_display = data.get('library_name') or ("所有库" if not lib_ids else "多个库")

    # ★★★ 修改：不再校验 lib_ids 是否为空，只校验 tags ★★★
    if not tags:
        return jsonify({"error": "标签不能为空"}), 400

    task_manager.submit_task(
        task_bulk_auto_tag,
        task_name=f"手动补打标签: {lib_name_display}",
        processor_type='media',
        library_ids=lib_ids,
        tags=tags,
        # ★★★ 传递给任务 ★★★
        rating_filters=rating_filters 
    )
    return jsonify({"message": "批量打标任务已启动"})

@media_api_bp.route('/auto_tagging/clear_now', methods=['POST'])
@admin_required
@processor_ready_required
def clear_tagging_now():
    from tasks.media import task_bulk_remove_tags
    data = request.json
    lib_ids = data.get('library_ids')
    tags = data.get('tags')
    # ★★★ 新增：接收分级筛选参数 ★★★
    rating_filters = data.get('rating_filters')
    lib_name_display = data.get('library_name', "多个库")

    task_manager.submit_task(
        task_bulk_remove_tags,
        task_name=f"手动移除标签: {lib_name_display}",
        processor_type='media',
        library_ids=lib_ids,
        tags=tags,
        # ★★★ 传递给任务 ★★★
        rating_filters=rating_filters
    )
    return jsonify({"message": "批量移除任务已启动"})

# ✨✨✨ 手动替换媒体图片 (海报/Logo/背景) ✨✨✨
@media_api_bp.route('/update_media_image/<item_id>', methods=['POST'])
@admin_required
@processor_ready_required
def api_update_media_image(item_id):
    """
    接收前端传来的图片 URL 或 图片文件，直接覆盖物理文件并刷新 Emby。
    支持 multipart/form-data (文件上传) 或 application/json (URL)。
    """
    try:
        image_type = None
        image_url = None
        image_bytes = None
        image_content_type = None

        # 1. 解析请求数据 (兼容 JSON 和 FormData)
        if request.is_json:
            data = request.json
            image_type = data.get('image_type')
            image_url = data.get('image_url')
        else:
            image_type = request.form.get('image_type')
            image_url = request.form.get('image_url')
            file = request.files.get('file')
            if file and file.filename:
                image_content_type = file.content_type
                image_bytes = file.read()

        # 2. 基础校验
        if not image_type:
            return jsonify({"error": "缺少 image_type 参数 (可选值: poster, clearlogo, fanart, landscape)"}), 400
            
        if not image_url and not image_bytes:
            return jsonify({"error": "必须提供 image_url 或上传 file"}), 400

        # 3. 调用核心处理器执行物理替换
        success, message = extensions.media_processor_instance.update_media_image_manually(
            item_id=item_id,
            image_type=image_type,
            image_url=image_url,
            image_bytes=image_bytes,
            content_type=image_content_type,
        )

        if success:
            return jsonify({"message": message}), 200
        else:
            return jsonify({"error": message}), 500

    except Exception as e:
        logger.error(f"API /update_media_image 发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500

# ✨✨✨ 获取 TMDb 备选图片列表 ✨✨✨
@media_api_bp.route('/tmdb_images/<item_id>', methods=['GET'])
@processor_ready_required
def api_get_tmdb_images(item_id):
    """
    获取指定媒体在 TMDb 上的所有备选图片（海报、背景、Logo）。
    """
    try:
        # 1. 获取 Emby 详情以拿到 TMDb ID
        item_details = emby.get_emby_item_details(
            item_id,
            extensions.media_processor_instance.emby_url,
            extensions.media_processor_instance.emby_api_key,
            extensions.media_processor_instance.emby_user_id
        )
        
        if not item_details:
            return jsonify({"error": "无法获取 Emby 媒体详情"}), 404
            
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_type = item_details.get("Type")
        
        if not tmdb_id:
            return jsonify({"error": "该媒体缺少 TMDb ID，无法获取图片"}), 400

        # 2. 调用 TMDb API 获取图片
        import handler.tmdb as tmdb
        api_key = extensions.media_processor_instance.tmdb_api_key
        
        # 尽可能多地获取图片（中文、英文、无文字）
        img_lang = "zh-CN,zh-TW,zh,en,null,ja,ko" 
        
        if item_type == "Movie":
            data = tmdb.get_movie_details(int(tmdb_id), api_key, append_to_response="images", include_image_language=img_lang)
        elif item_type == "Series":
            data = tmdb.get_tv_details(int(tmdb_id), api_key, append_to_response="images", include_image_language=img_lang)
        else:
            return jsonify({"error": "不支持的媒体类型"}), 400

        if not data or "images" not in data:
            return jsonify({"error": "未在 TMDb 找到图片数据"}), 404

        images = data["images"]
        
        # 3. 格式化返回数据 (提供预览小图和下载原图)
        base_preview_url = "https://image.tmdb.org/t/p/w500"
        base_original_url = "https://image.tmdb.org/t/p/original"
        
        def format_images(img_list):
            return [{
                "preview": f"{base_preview_url}{img['file_path']}",
                "original": f"{base_original_url}{img['file_path']}",
                "aspect_ratio": img.get("aspect_ratio", 1),
                "width": img.get("width"),    # ★★★ 新增：把宽度传给前端
                "height": img.get("height")   # ★★★ 新增：把高度传给前端
            } for img in img_list]

        result = {
            "posters": format_images(images.get("posters", [])),
            "backdrops": format_images(images.get("backdrops", [])),
            "logos": format_images(images.get("logos", []))
        }

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"API /tmdb_images 发生错误: {e}", exc_info=True)
        return jsonify({"error": "获取 TMDb 图片失败"}), 500
    
# ======================================================================
# ★★★ 媒体信息 (MediaInfo) 编辑 API ★★★
# ======================================================================
def _json_array_value(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []
    return value if isinstance(value, list) else []


def _resolve_media_info_edit_context(item_id):
    item_id = str(item_id or "").strip()
    if not item_id:
        return None, ("缺少 Emby ItemID", 400)

    from database.connection import get_db_connection
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT title, item_type, parent_series_tmdb_id, season_number, episode_number,
                       emby_item_ids_json, asset_details_json, file_sha1_json
                FROM media_metadata
                WHERE emby_item_ids_json @> %s::jsonb
                LIMIT 1
            """, (json.dumps([item_id]),))
            row = cursor.fetchone()

    if not row:
        return None, ("数据库中未找到该媒体项", 404)

    asset_details = _json_array_value(row.get("asset_details_json"))
    file_sha1_list = _json_array_value(row.get("file_sha1_json"))
    emby_item_ids = [str(x or "").strip() for x in _json_array_value(row.get("emby_item_ids_json"))]

    target_index = -1
    for index, asset in enumerate(asset_details):
        if isinstance(asset, dict) and str(asset.get("emby_item_id") or "").strip() == item_id:
            target_index = index
            break
    if target_index < 0 and item_id in emby_item_ids:
        target_index = emby_item_ids.index(item_id)
    if target_index < 0 and len(file_sha1_list) == 1:
        target_index = 0

    sha1 = None
    if 0 <= target_index < len(file_sha1_list):
        sha1 = str(file_sha1_list[target_index] or "").strip().upper()
    if not sha1 and len(file_sha1_list) == 1:
        sha1 = str(file_sha1_list[0] or "").strip().upper()
    if not sha1:
        return None, ("未找到该媒体项对应的 SHA1", 404)

    mediainfo_json = media_db.get_mediainfo_by_sha1(sha1)
    if not mediainfo_json:
        return None, ("未找到该媒体的格式化媒体信息缓存", 404)

    return {
        "item_id": item_id,
        "sha1": sha1,
        "mediainfo": mediainfo_json,
        "title": row.get("title"),
        "item_type": row.get("item_type"),
        "parent_series_tmdb_id": row.get("parent_series_tmdb_id"),
        "season_number": row.get("season_number"),
        "episode_number": row.get("episode_number"),
    }, None


def _mediainfo_root(mediainfo):
    if isinstance(mediainfo, list) and mediainfo and isinstance(mediainfo[0], dict):
        return mediainfo[0]
    return mediainfo if isinstance(mediainfo, dict) else None


def _ticks_to_seconds(ticks):
    try:
        ticks = int(float(ticks))
        return round(ticks / 10_000_000, 3) if ticks >= 0 else None
    except Exception:
        return None


def _seconds_to_ticks(value, label):
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label} 必须填写有效秒数")
    if seconds < 0:
        raise ValueError(f"{label} 不能小于 0")
    return int(round(seconds * 10_000_000))


def _extract_marker_seconds(mediainfo):
    root = _mediainfo_root(mediainfo)
    chapters = root.get("Chapters") if isinstance(root, dict) else []
    if not isinstance(chapters, list):
        chapters = []
    values = {
        "intro_start_seconds": None,
        "intro_end_seconds": None,
        "credits_start_seconds": None,
    }
    for chapter in chapters:
        if not isinstance(chapter, dict):
            continue
        marker = str(chapter.get("MarkerType") or "").strip()
        seconds = _ticks_to_seconds(chapter.get("StartPositionTicks"))
        if seconds is None:
            continue
        if marker == "IntroStart":
            values["intro_start_seconds"] = seconds
        elif marker == "IntroEnd":
            values["intro_end_seconds"] = seconds
        elif marker == "CreditsStart":
            values["credits_start_seconds"] = seconds

    runtime_ticks = None
    source = root.get("MediaSourceInfo") if isinstance(root, dict) and isinstance(root.get("MediaSourceInfo"), dict) else root
    if isinstance(source, dict):
        runtime_ticks = source.get("RunTimeTicks") or (root or {}).get("RunTimeTicks")
    values["runtime_seconds"] = _ticks_to_seconds(runtime_ticks)
    values["intro_enabled"] = values["intro_start_seconds"] is not None and values["intro_end_seconds"] is not None
    values["credits_enabled"] = values["credits_start_seconds"] is not None
    return values


def _merge_marker_chapters(mediainfo, intro_enabled, intro_start_ticks, intro_end_ticks, credits_enabled, credits_start_ticks):
    root = _mediainfo_root(mediainfo)
    if root is None:
        raise ValueError("媒体信息格式无效")
    existing = root.get("Chapters")
    if not isinstance(existing, list):
        existing = []
    marker_types = {"IntroStart", "IntroEnd", "CreditsStart"}
    kept = [
        item for item in existing
        if not (isinstance(item, dict) and str(item.get("MarkerType") or "") in marker_types)
    ]
    additions = []
    if intro_enabled:
        additions.extend([
            {
                "StartPositionTicks": int(intro_start_ticks),
                "Name": "片头",
                "MarkerType": "IntroStart",
                "ChapterIndex": len(kept),
            },
            {
                "StartPositionTicks": int(intro_end_ticks),
                "Name": "片头结束",
                "MarkerType": "IntroEnd",
                "ChapterIndex": len(kept) + 1,
            },
        ])
    if credits_enabled:
        additions.append({
            "StartPositionTicks": int(credits_start_ticks),
            "Name": "片尾",
            "MarkerType": "CreditsStart",
            "ChapterIndex": len(kept) + len(additions),
        })
    root["Chapters"] = kept + additions
    return mediainfo


def _reset_intro_detection_failures(context, reset_intro, reset_credits):
    kinds = []
    if reset_intro:
        kinds.append("intro")
    if reset_credits:
        kinds.append("credits")
    if not kinds:
        return 0
    keys = [f"episode:{context.get('sha1')}"]
    series_tmdb_id = str(context.get("parent_series_tmdb_id") or "").strip()
    try:
        season_number = int(context.get("season_number") or 0)
    except (TypeError, ValueError):
        season_number = 0
    if series_tmdb_id:
        keys.append(f"season:{series_tmdb_id}:{season_number}")
    try:
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM p115_intro_detection_failures
                    WHERE failure_key = ANY(%s)
                      AND kind = ANY(%s)
                    """,
                    (keys, kinds),
                )
                deleted = cursor.rowcount
            conn.commit()
        return deleted
    except Exception as exc:
        logger.debug("清理片头片尾失败标记失败: %s", exc, exc_info=True)
        return 0


@media_api_bp.route('/media_info/edit/<item_id>', methods=['GET'])
@processor_ready_required
def api_get_media_info_for_edit(item_id):
    """获取指定媒体的底层 MediaInfo JSON 数据（直接从数据库查）"""
    try:
        context, error = _resolve_media_info_edit_context(item_id)
        if error:
            message, status = error
            return jsonify({"error": message}), status

        return jsonify({
            "sha1": context["sha1"],
            "mediainfo": context["mediainfo"]
        })

    except Exception as e:
        logger.error(f"获取媒体信息失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500


@media_api_bp.route('/media_info/chapters/<item_id>', methods=['GET'])
@processor_ready_required
def api_get_intro_credit_chapters(item_id):
    """读取当前媒体项的片头/片尾章节。"""
    try:
        context, error = _resolve_media_info_edit_context(item_id)
        if error:
            message, status = error
            return jsonify({"error": message}), status
        return jsonify({
            "sha1": context["sha1"],
            **_extract_marker_seconds(context["mediainfo"]),
        })
    except Exception as e:
        logger.error(f"读取片头片尾失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500


@media_api_bp.route('/media_info/chapters/<item_id>', methods=['POST'])
@admin_required
@processor_ready_required
def api_save_intro_credit_chapters(item_id):
    """保存片头/片尾章节到本地缓存，并通过桥接插件写入 Emby。"""
    data = request.json or {}
    try:
        context, error = _resolve_media_info_edit_context(item_id)
        if error:
            message, status = error
            return jsonify({"error": message}), status
        if str(data.get("sha1") or "").strip().upper() != context["sha1"]:
            return jsonify({"error": "当前媒体信息已变化，请重新打开后再保存"}), 409

        intro_enabled = bool(data.get("intro_enabled"))
        credits_enabled = bool(data.get("credits_enabled"))
        intro_start_ticks = intro_end_ticks = credits_start_ticks = None

        if intro_enabled:
            intro_start_ticks = _seconds_to_ticks(data.get("intro_start_seconds"), "片头开始")
            intro_end_ticks = _seconds_to_ticks(data.get("intro_end_seconds"), "片头结束")
            if intro_end_ticks <= intro_start_ticks:
                return jsonify({"error": "片头结束必须大于片头开始"}), 400
        if credits_enabled:
            credits_start_ticks = _seconds_to_ticks(data.get("credits_start_seconds"), "片尾开始")
        current_markers = _extract_marker_seconds(context["mediainfo"])
        runtime_seconds = current_markers.get("runtime_seconds")
        if runtime_seconds is not None and runtime_seconds > 0:
            runtime_ticks = _seconds_to_ticks(runtime_seconds, "视频时长")
            if intro_enabled and intro_end_ticks >= runtime_ticks:
                return jsonify({"error": "片头结束不能超过视频时长"}), 400
            if credits_enabled and credits_start_ticks >= runtime_ticks:
                return jsonify({"error": "片尾开始不能超过视频时长"}), 400

        mediainfo_json = _merge_marker_chapters(
            context["mediainfo"],
            intro_enabled,
            intro_start_ticks,
            intro_end_ticks,
            credits_enabled,
            credits_start_ticks,
        )

        from database import connection
        from psycopg2.extras import Json
        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE p115_mediainfo_cache SET mediainfo_json = %s WHERE sha1 = %s",
                    (Json(mediainfo_json, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)), context["sha1"]),
                )
                if cursor.rowcount != 1:
                    return jsonify({"error": "媒体信息缓存不存在"}), 404
            conn.commit()

        processor = extensions.media_processor_instance
        result = emby.update_etk_chapters(
            str(item_id),
            processor.emby_url,
            processor.emby_api_key,
            intro_start_ticks=intro_start_ticks,
            intro_end_ticks=intro_end_ticks,
            credits_start_ticks=credits_start_ticks,
            clear_intro=not intro_enabled,
            clear_credits=not credits_enabled,
        )
        if result is None:
            return jsonify({"error": "缓存已保存，但写入 Emby 失败，请确认桥接插件已更新"}), 502

        reset_count = _reset_intro_detection_failures(
            context,
            reset_intro=not intro_enabled,
            reset_credits=not credits_enabled,
        )
        return jsonify({
            "message": "片头片尾已更新并写入 Emby",
            "sha1": context["sha1"],
            "result": result,
            "failure_reset_count": reset_count,
            **_extract_marker_seconds(mediainfo_json),
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"保存片头片尾失败: {e}", exc_info=True)
        return jsonify({"error": f"保存失败: {str(e)}"}), 500

@media_api_bp.route('/media_info/edit/<item_id>', methods=['POST'])
@admin_required
@processor_ready_required
def api_save_media_info_for_edit(item_id):
    """保存修改后的 MediaInfo，并通过桥接插件写入 Emby。"""
    data = request.json
    sha1 = data.get("sha1")
    new_mediainfo = data.get("mediainfo")
    
    if not sha1 or not new_mediainfo:
        return jsonify({"error": "参数不完整"}), 400
        
    try:
        # 1. 更新数据库 p115_mediainfo_cache
        from database import connection
        from psycopg2.extras import Json
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE p115_mediainfo_cache SET mediainfo_json = %s WHERE sha1 = %s",
                (Json(new_mediainfo, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)), str(sha1).upper())
            )
            if cursor.rowcount != 1:
                return jsonify({"error": "媒体信息缓存不存在"}), 404
            conn.commit()

        processor = extensions.media_processor_instance
        result = emby.apply_etk_mediainfo(
            item_id,
            new_mediainfo,
            processor.emby_url,
            processor.emby_api_key,
        )
        if result is None:
            return jsonify({"error": "缓存已保存，但写入 Emby 失败，请确认桥接插件已安装"}), 502

        washing_priority = None
        washing_priority_error = None
        if settings_db.get_washing_conflict_mode() == 'replace':
            try:
                from tasks.p115 import recalculate_washing_priority_for_sha1
                washing_priority = recalculate_washing_priority_for_sha1(sha1)
            except Exception as exc:
                washing_priority_error = str(exc)
                logger.warning(f"媒体信息已保存，但洗版优先级重算失败: {exc}", exc_info=True)
        
        return jsonify({
            "message": "媒体信息已更新并写入 Emby",
            "result": result,
            "washing_priority": washing_priority,
            "washing_priority_error": washing_priority_error,
        })
        
    except Exception as e:
        logger.error(f"保存媒体信息失败: {e}", exc_info=True)
        return jsonify({"error": f"保存失败: {str(e)}"}), 500
    
@media_api_bp.route('/media_info/series/<series_id>/episodes', methods=['GET'])
@processor_ready_required
def api_get_series_episodes_for_edit(series_id):
    """获取指定剧集下所有在库的分集，用于前端选择编辑"""
    try:
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 1. 先通过 Emby ID 找到该剧集的 TMDb ID
                cursor.execute("""
                    SELECT tmdb_id FROM media_metadata
                    WHERE emby_item_ids_json @> %s::jsonb AND item_type = 'Series'
                    LIMIT 1
                """, (json.dumps([str(series_id)]),))
                series_row = cursor.fetchone()

                if not series_row:
                    return jsonify({"error": "未在数据库中找到该剧集信息"}), 404

                series_tmdb_id = series_row['tmdb_id']

                # 2. 通过剧集的 TMDb ID 查找所有在库的集 (Episode)
                cursor.execute("""
                    SELECT title, season_number, episode_number, emby_item_ids_json
                    FROM media_metadata
                    WHERE parent_series_tmdb_id = %s AND item_type = 'Episode' AND in_library = TRUE
                    ORDER BY season_number ASC, episode_number ASC
                """, (series_tmdb_id,))
                episodes = cursor.fetchall()

        result = []
        for ep in episodes:
            emby_ids = ep.get('emby_item_ids_json') or []
            if emby_ids:
                # 取第一个 Emby ID 作为编辑目标
                result.append({
                    "emby_id": emby_ids[0],
                    "season_number": ep.get('season_number'),
                    "episode_number": ep.get('episode_number'),
                    "title": ep.get('title')
                })

        return jsonify(result)

    except Exception as e:
        logger.error(f"获取剧集分集列表失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
