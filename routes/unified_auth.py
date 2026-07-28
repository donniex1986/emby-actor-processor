# routes/unified_auth.py

import logging
import secrets
import time
from flask import Blueprint, request, jsonify, session
import config_manager
import constants
import handler.emby as emby
from database import user_db

unified_auth_bp = Blueprint('unified_auth_bp', __name__, url_prefix='/api/auth')
logger = logging.getLogger(__name__)

# --- 内存存储：灾难恢复令牌 ---
# 结构: { 'token_string': expiry_timestamp }
RECOVERY_TOKENS = {}

def clean_expired_tokens():
    """清理过期的令牌"""
    now = time.time()
    expired = [t for t, exp in RECOVERY_TOKENS.items() if now > exp]
    for t in expired:
        del RECOVERY_TOKENS[t]


def _sync_and_start_session(user_info):
    user_id = user_info.get('Id')
    try:
        user_db.upsert_emby_users_batch([user_info])
    except Exception as e:
        logger.warning(f"登录时同步用户信息失败: {e}")

    session.clear()
    session['emby_user_id'] = user_id
    session['emby_username'] = user_info.get('Name')
    session['emby_is_admin'] = user_info.get('Policy', {}).get('IsAdministrator', False)
    session.permanent = True


def _authorize_emby_service(url, username, password):
    normalized_url = str(url or '').strip().rstrip('/')
    username = str(username or '').strip()
    if not normalized_url or not username:
        return None, "Emby URL 和管理员用户名不能为空"

    auth_result = emby.authenticate_emby_user(
        username,
        password or '',
        base_url=normalized_url,
        device_id="emby-toolkit-service",
    )
    if not auth_result:
        return None, "管理员认证失败，请检查账号、密码和服务器地址"

    access_token = str(auth_result.get('AccessToken') or '').strip()
    user_info = auth_result.get('User') or {}
    user_id = str(user_info.get('Id') or '').strip()
    if not access_token or not user_id:
        return None, "Emby 认证响应缺少 AccessToken 或用户 ID"

    if not user_info.get('Policy', {}).get('IsAdministrator', False):
        refreshed_user = emby.get_user_info_from_server(normalized_url, access_token, user_id)
        if refreshed_user:
            user_info = refreshed_user
            auth_result['User'] = user_info
    if not user_info.get('Policy', {}).get('IsAdministrator', False):
        return None, "必须使用 Emby 管理员账号完成服务授权"

    test_result = emby.test_connection(normalized_url, access_token)
    if not test_result.get('success'):
        return None, f"服务 Token 验证失败: {test_result.get('error')}"

    auth_result['_normalized_url'] = normalized_url
    return auth_result, None


def _save_emby_service_authorization(auth_result):
    user_info = auth_result['User']
    new_config = {
        constants.CONFIG_OPTION_EMBY_SERVER_URL: auth_result['_normalized_url'],
        constants.CONFIG_OPTION_EMBY_API_KEY: auth_result['AccessToken'],
        constants.CONFIG_OPTION_EMBY_USER_ID: user_info['Id'],
        constants.CONFIG_OPTION_EMBY_AUTH_MODE: "user_token",
    }
    from web_app import save_config_and_reload
    save_config_and_reload(new_config)


def _etk_origin_for_plugin():
    from handler.emby_discovery import resolve_etk_service_url, validate_internal_service_url

    manual = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_ETK_SERVER_URL_MANUAL)
    if str(manual or '').strip():
        return validate_internal_service_url(manual)
    return resolve_etk_service_url(config_manager.APP_CONFIG)


def _install_etk_plugin_after_authorization(auth_result):
    from handler.etk_plugin_update import ensure_etk_plugin_installed
    return ensure_etk_plugin_installed(
        auth_result['_normalized_url'],
        auth_result['AccessToken'],
        _etk_origin_for_plugin(),
    )


def _configure_libraries_after_plugin_install(auth_result, plugin_install):
    if not plugin_install.get('ok'):
        return {
            'ok': False,
            'status': 'skipped',
            'message': 'ETK 插件未就绪，已跳过媒体库加速配置',
        }
    return emby.configure_etk_library_defaults(
        auth_result['_normalized_url'],
        auth_result['AccessToken'],
    )


@unified_auth_bp.route('/check_status', methods=['GET'])
def check_system_status():
    """
    【前端入口检查】
    前端 App.vue 加载时首先调用此接口，决定跳转到哪个页面。
    """
    # 1. 检查系统是否已配置
    if not config_manager.is_system_configured():
        return jsonify({
            "status": "setup_required", 
            "message": "系统未配置"
        }), 200
    
    # 2. 检查是否已登录
    if 'emby_user_id' in session:
        return jsonify({
            "status": "logged_in",
            "user": {
                "id": session['emby_user_id'],
                "name": session.get('emby_username'),
                "is_admin": session.get('emby_is_admin', False)
            }
        }), 200

    # 3. 既已配置又未登录 -> 需要登录
    return jsonify({"status": "login_required"}), 200

@unified_auth_bp.route('/login', methods=['POST'])
def emby_only_login():
    """
    【纯 Emby 登录接口】
    """
    data = request.json
    username = data.get('username')
    password = data.get('password')

    # 双重检查：如果系统没配置，不允许登录，强制去设置
    if not config_manager.is_system_configured():
        return jsonify({
            "status": "error", 
            "code": "SETUP_REQUIRED", 
            "message": "系统尚未配置 Emby 连接"
        }), 428 # 428 Precondition Required

    # 调用 Emby 验证
    auth_result = emby.authenticate_emby_user(username, password)
    
    if not auth_result:
        return jsonify({
            "status": "error", 
            "message": "登录失败：用户名/密码错误，或无法连接 Emby 服务器"
        }), 401

    user_info = auth_result.get('User', {})
    user_id = user_info.get('Id')
    _sync_and_start_session(user_info)

    logger.info(f"Emby 用户 '{session['emby_username']}' 登录成功。")
    
    # 获取用户权限信息
    can_subscribe = user_db.get_user_subscription_permission(user_id)
    
    return jsonify({
        "status": "ok",
        "user": {
            "id": user_id,
            "name": session['emby_username'],
            "is_admin": session['emby_is_admin'],
            "allow_unrestricted_subscriptions": can_subscribe,
            "user_type": "emby_user" # 前端兼容字段
        }
    }), 200

# ==========================================
#  设置与灾难恢复逻辑
# ==========================================

@unified_auth_bp.route('/request_recovery', methods=['POST'])
def request_recovery_token():
    """
    【步骤1】用户请求重置连接。
    生成一个 Token 打印到日志，不返回给前端。
    """
    clean_expired_tokens()
    
    # 生成 6 位随机字符作为令牌
    token = secrets.token_hex(3).upper() 
    # 有效期 5 分钟
    RECOVERY_TOKENS[token] = time.time() + 300 
    
    logger.critical("=" * 60)
    logger.critical(f"【安全警告】收到重置连接配置的请求。")
    logger.critical(f"若这是您本人的操作，请在页面输入以下安全令牌以进入设置模式:")
    logger.critical(f"安全令牌:  {token}")
    logger.critical(f"令牌有效期: 5 分钟")
    logger.critical("=" * 60)
    
    return jsonify({
        "status": "ok", 
        "message": "安全令牌已发送至服务器控制台日志(Docker Logs)，请查阅并输入。"
    }), 200

@unified_auth_bp.route('/verify_recovery', methods=['POST'])
def verify_recovery_token():
    """
    【步骤2】验证令牌。
    如果通过，给予临时 Session 权限进入设置页面。
    """
    data = request.json
    token = data.get('token', '').strip().upper()
    
    clean_expired_tokens()
    
    if token in RECOVERY_TOKENS:
        del RECOVERY_TOKENS[token] # 一次性使用
        session['is_setup_mode'] = True # 标记：允许访问 setup 接口
        return jsonify({"status": "ok", "message": "验证成功"}), 200
    
    return jsonify({"status": "error", "message": "令牌无效或已过期"}), 403

@unified_auth_bp.route('/setup', methods=['POST'])
def save_emby_config():
    """
    【步骤3】保存 Emby 配置。
    仅在 (系统未配置) 或 (拥有 setup_mode 权限) 时允许调用。
    """
    # 权限检查
    is_configured = config_manager.is_system_configured()
    has_setup_permission = session.get('is_setup_mode')
    
    if is_configured and not has_setup_permission:
        return jsonify({"status": "error", "message": "系统已配置，且无重置权限"}), 403

    data = request.json
    url = data.get('url')
    username = data.get('username')
    password = data.get('password')

    auth_result, error = _authorize_emby_service(url, username, password)
    if error:
        return jsonify({"status": "error", "message": error}), 400

    try:
        _save_emby_service_authorization(auth_result)
    except Exception as e:
        logger.error(f"保存配置失败: {e}")
        return jsonify({"status": "error", "message": "保存配置失败，请检查日志"}), 500

    plugin_install = _install_etk_plugin_after_authorization(auth_result)
    library_config = _configure_libraries_after_plugin_install(auth_result, plugin_install)
    _sync_and_start_session(auth_result['User'])
    logger.info("Emby 管理员服务授权已保存，账号密码未持久化。")
    message = "Emby 服务授权成功"
    if plugin_install.get('ok'):
        message += "，" + plugin_install.get('message', 'ETK 插件已就绪')
        if library_config.get('ok'):
            message += "，" + library_config.get('message', '媒体库加速配置已应用')
        else:
            message += "；媒体库加速配置失败：" + library_config.get('message', '未知错误')
    else:
        message += "；ETK 插件自动安装失败：" + plugin_install.get('message', '未知错误')
    return jsonify({
        "status": "ok",
        "message": message,
        "plugin_install": plugin_install,
        "library_config": library_config,
        "user": {
            "id": session['emby_user_id'],
            "name": session['emby_username'],
            "is_admin": True,
        },
    }), 200


@unified_auth_bp.route('/service_status', methods=['GET'])
def service_authorization_status():
    if 'emby_user_id' not in session:
        return jsonify({"status": "error", "message": "需要先登录"}), 401
    authorized = config_manager.is_emby_service_authorized()
    if authorized:
        test_result = emby.test_connection(
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL),
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY),
        )
        authorized = bool(test_result.get('success'))
    return jsonify({
        "authorized": authorized,
        "url": config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL) or "",
    })


@unified_auth_bp.route('/reauthorize', methods=['POST'])
def reauthorize_emby_service():
    if 'emby_user_id' not in session:
        return jsonify({"status": "error", "message": "需要先登录"}), 401

    data = request.json or {}
    url = data.get('url') or config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    auth_result, error = _authorize_emby_service(url, data.get('username'), data.get('password'))
    if error:
        return jsonify({"status": "error", "message": error}), 400

    try:
        _save_emby_service_authorization(auth_result)
    except Exception as e:
        logger.error(f"重新授权 Emby 服务失败: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "保存服务授权失败，请检查日志"}), 500

    plugin_install = _install_etk_plugin_after_authorization(auth_result)
    library_config = _configure_libraries_after_plugin_install(auth_result, plugin_install)
    user_info = auth_result['User']
    message = "Emby 服务授权已更新"
    if plugin_install.get('ok'):
        message += "，" + plugin_install.get('message', 'ETK 插件已就绪')
        if library_config.get('ok'):
            message += "，" + library_config.get('message', '媒体库加速配置已应用')
        else:
            message += "；媒体库加速配置失败：" + library_config.get('message', '未知错误')
    else:
        message += "；ETK 插件自动安装失败：" + plugin_install.get('message', '未知错误')
    return jsonify({
        "status": "ok",
        "message": message,
        "plugin_install": plugin_install,
        "library_config": library_config,
        "authorization": {
            "url": auth_result['_normalized_url'],
            "user_id": user_info['Id'],
            "user_name": user_info.get('Name') or "",
            "auth_mode": "user_token",
        },
    })

@unified_auth_bp.route('/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({"status": "ok"})
