<!-- src/components/settings/MoviePilotConfigModal.vue -->
<template>
  <n-modal
    v-model:show="showModal"
    preset="card"
    title="配置 MoviePilot"
    style="width: min(1180px, 96vw);"
    class="custom-modal glass-modal"
  >
    <n-spin :show="loading">
      <div class="mp-config-panel">
        <div class="mp-config-grid">
          <n-form label-placement="left" label-width="132" class="mp-config-column">
            <n-divider title-placement="left" style="margin: 0 0 20px 0;">基础配置</n-divider>
            <n-form-item label="MoviePilot URL">
              <n-input v-model:value="formModel.moviepilot_url" placeholder="例如: http://192.168.1.100:3000" />
            </n-form-item>
            <n-form-item label="用户名">
              <n-input v-model:value="formModel.moviepilot_username" placeholder="登录用户名" />
            </n-form-item>
            <n-form-item label="密码">
              <n-input
                v-model:value="formModel.moviepilot_password"
                type="password"
                show-password-on="mousedown"
                placeholder="登录密码"
              />
            </n-form-item>
            <n-form-item label="辅助识别">
              <n-switch v-model:value="formModel.moviepilot_recognition" />
              <template #feedback>
                <n-text depth="3" style="font-size:0.8em;">
                  整理网盘资源时，正则无法识别文件名才调用 MP 接口辅助识别。
                </n-text>
              </template>
            </n-form-item>
            <div class="compact-settings-row">
              <div>
                <div class="sub-label">每日订阅上限</div>
                <n-input-number v-model:value="formModel.resubscribe_daily_cap" :min="1" style="width: 100%;" />
              </div>
              <div>
                <div class="sub-label">请求间隔 (秒)</div>
                <n-input-number
                  v-model:value="formModel.resubscribe_delay_seconds"
                  :min="0.1"
                  :step="0.1"
                  style="width: 100%;"
                />
              </div>
            </div>
          </n-form>

          <n-form label-placement="left" label-width="132" class="mp-config-column">
            <n-divider title-placement="left" style="margin: 0 0 20px 0;">电影策略</n-divider>
            <n-form-item label="搜索窗口期 (天)">
              <n-input-number v-model:value="formModel.movie_search_window_days" :min="1" style="width: 100%;" />
              <template #feedback>
                <n-text depth="3" style="font-size:0.8em;">电影订阅连续搜索的天数，到期未入库则暂停释放运行名额。</n-text>
              </template>
            </n-form-item>
            <n-form-item label="暂停周期 (天)">
              <n-input-number v-model:value="formModel.movie_pause_days" :min="1" style="width: 100%;" />
              <template #feedback>
                <n-text depth="3" style="font-size:0.8em;">暂停到期后自动复活继续搜索，适合老电影长期低占用尝试。</n-text>
              </template>
            </n-form-item>
            <n-form-item label="延迟订阅 (天)">
              <n-input-number v-model:value="formModel.delay_subscription_days" :min="0" style="width: 100%;" />
              <template #feedback>
                <n-text depth="3" style="font-size:0.8em;">电影上映后 N 天才允许订阅，0 表示不延迟。</n-text>
              </template>
            </n-form-item>
          </n-form>
        </div>

        <div class="assistant-hero">
          <div>
            <div class="assistant-title">剧集策略</div>
            <div class="assistant-desc">
              订阅助手 PLUS 全流程接管 MP 剧集订阅，电影订阅继续使用间隔搜索策略。
              <a
                class="assistant-wiki-link"
                href="https://hbq0405.github.io/emby-toolkit/zh/guide/subscribe-assistant"
                target="_blank"
                rel="noopener noreferrer"
              >查看使用详解</a>
            </div>
          </div>
          <n-switch v-model:value="formModel.subscribe_assistant.enabled" size="small">
            <template #checked>接管中</template>
            <template #unchecked>关闭</template>
          </n-switch>
        </div>

        <n-collapse-transition :show="formModel.subscribe_assistant.enabled">
          <div class="assistant-settings-grid">
            <div class="settings-col">
              <div class="settings-group-title">状态判定</div>
              <div class="settings-card">
                <div class="assistant-section">
                  <div class="assistant-section-title">完结守卫</div>
                  <n-grid :x-gap="12" :y-gap="12" :cols="3" responsive="screen">
                    <n-grid-item>
                      <div class="sub-label">守卫模式</div>
                      <n-select v-model:value="formModel.subscribe_assistant.guard_mode" :options="guardModeOptions" size="small" />
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">季冷却期</div>
                      <n-input-number v-model:value="formModel.subscribe_assistant.season_cooldown_days" size="small" :min="0">
                        <template #suffix>天</template>
                      </n-input-number>
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">集数波动窗口</div>
                      <n-input-number v-model:value="formModel.subscribe_assistant.volatility_window_days" size="small" :min="1">
                        <template #suffix>天</template>
                      </n-input-number>
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">启用波动判断</div>
                      <n-switch v-model:value="formModel.subscribe_assistant.volatility_enabled" size="small" />
                    </n-grid-item>
                  </n-grid>
                </div>

                <n-divider style="margin: 0" />

                <div class="assistant-section">
                  <div class="assistant-section-title">待定与暂停</div>
                  <n-grid :x-gap="12" :y-gap="12" :cols="3" responsive="screen">
                    <n-grid-item>
                      <div class="sub-label">增强待定</div>
                      <n-switch v-model:value="formModel.subscribe_assistant.pending_enhanced_enabled" size="small" />
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">参考波动</div>
                      <n-switch v-model:value="formModel.subscribe_assistant.pending_use_volatility" size="small" />
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">待定虚标集数</div>
                      <n-input-number v-model:value="formModel.subscribe_assistant.pending_fake_total_episodes" size="small" :min="1">
                        <template #suffix>集</template>
                      </n-input-number>
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">增强暂停</div>
                      <n-switch v-model:value="formModel.subscribe_assistant.pause_enhanced_enabled" size="small" />
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">开播前暂停</div>
                      <n-input-number v-model:value="formModel.subscribe_assistant.tv_air_pause_days" size="small" :min="0">
                        <template #suffix>天</template>
                      </n-input-number>
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">播出间隔暂停</div>
                      <n-input-number v-model:value="formModel.subscribe_assistant.airing_pause_days" size="small" :min="0">
                        <template #suffix>天</template>
                      </n-input-number>
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">无下载等待</div>
                      <n-input-number v-model:value="formModel.subscribe_assistant.tv_no_download_days" size="small" :min="0">
                        <template #suffix>天</template>
                      </n-input-number>
                    </n-grid-item>
                    <n-grid-item span="2">
                      <div class="sub-label">无下载动作</div>
                      <n-select
                        v-model:value="formModel.subscribe_assistant.no_download_actions"
                        :options="noDownloadActionOptions"
                        multiple
                        clearable
                        size="small"
                      />
                    </n-grid-item>
                  </n-grid>
                </div>
              </div>

            </div>

            <div class="settings-col">
              <div class="settings-group-title">洗版与维护</div>
              <div class="settings-card">
                <div class="assistant-section">
                  <div class="assistant-section-title">洗版编排</div>
                  <n-grid :x-gap="12" :y-gap="12" :cols="3" responsive="screen">
                    <n-grid-item>
                      <div class="sub-label">洗版模式</div>
                      <n-select v-model:value="formModel.subscribe_assistant.best_version_type" :options="bestVersionTypeOptions" size="small" />
                    </n-grid-item>
                    <n-grid-item v-if="formModel.subscribe_assistant.best_version_type !== 'no'">
                      <div class="sub-label">一致性校验</div>
                      <n-switch v-model:value="formModel.subscribe_assistant.best_version_full_consistency_check_enabled" size="small" />
                    </n-grid-item>
                    <n-grid-item v-if="formModel.subscribe_assistant.best_version_type === 'tv_episode'">
                      <div class="sub-label">分集转全集</div>
                      <n-switch v-model:value="formModel.subscribe_assistant.best_version_episode_to_full" size="small" />
                    </n-grid-item>
                    <n-grid-item v-if="formModel.subscribe_assistant.best_version_type !== 'no'">
                      <div class="sub-label">洗版超时</div>
                      <n-input-number v-model:value="formModel.subscribe_assistant.full_washing_timeout_hours" size="small" :min="0">
                        <template #suffix>小时</template>
                      </n-input-number>
                    </n-grid-item>
                  </n-grid>
                </div>

                <n-divider style="margin: 0" />

                <div class="assistant-section">
                  <div class="assistant-section-title">订阅清理</div>
                  <n-grid :x-gap="12" :y-gap="12" :cols="2" responsive="screen">
                    <n-grid-item>
                      <div class="sub-label">清理范围</div>
                      <n-select v-model:value="formModel.subscribe_assistant.subscription_cleanup_history_type" :options="cleanupTypeOptions" size="small" />
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">清理场景</div>
                      <n-select
                        v-model:value="formModel.subscribe_assistant.subscription_cleanup_history_scenes"
                        :options="cleanupSceneOptions"
                        multiple
                        size="small"
                        clearable
                      />
                    </n-grid-item>
                  </n-grid>
                </div>

                <n-divider style="margin: 0" />

                <div class="assistant-section">
                  <div class="assistant-section-title">自动纠错</div>
                  <n-grid :x-gap="12" :y-gap="12" :cols="3" responsive="screen">
                    <n-grid-item>
                      <div class="sub-label">启用纠错</div>
                      <n-switch v-model:value="formModel.subscribe_assistant.verify_enabled" size="small" />
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">纠错间隔</div>
                      <n-input-number v-model:value="formModel.subscribe_assistant.verify_interval_hours" size="small" :min="1">
                        <template #suffix>小时</template>
                      </n-input-number>
                    </n-grid-item>
                    <n-grid-item>
                      <div class="sub-label">快照保留</div>
                      <n-input-number v-model:value="formModel.subscribe_assistant.snapshot_retention_days" size="small" :min="1">
                        <template #suffix>天</template>
                      </n-input-number>
                    </n-grid-item>
                  </n-grid>
                </div>
              </div>
            </div>
          </div>
        </n-collapse-transition>

        <div class="settings-group-title">公用策略</div>
        <div class="settings-card">
          <div class="assistant-section">
            <div class="assistant-section-title">下载巡检</div>
            <n-grid :x-gap="12" :y-gap="12" :cols="3" responsive="screen">
              <n-grid-item>
                <div class="sub-label">下载巡检</div>
                <n-switch v-model:value="formModel.subscribe_assistant.download_monitor_enabled" size="small" />
              </n-grid-item>
              <n-grid-item>
                <div class="sub-label">手动删种监听</div>
                <n-switch v-model:value="formModel.subscribe_assistant.manual_delete_listen" size="small" />
              </n-grid-item>
              <n-grid-item>
                <div class="sub-label">Tracker 响应监听</div>
                <n-switch v-model:value="formModel.subscribe_assistant.tracker_response_listen" size="small" />
              </n-grid-item>
              <n-grid-item>
                <div class="sub-label">超时窗口</div>
                <n-input-number v-model:value="formModel.subscribe_assistant.download_timeout_minutes" size="small" :min="5">
                  <template #suffix>分钟</template>
                </n-input-number>
              </n-grid-item>
              <n-grid-item>
                <div class="sub-label">进度阈值</div>
                <n-input-number v-model:value="formModel.subscribe_assistant.download_progress_threshold" size="small" :min="0" :max="100">
                  <template #suffix>%</template>
                </n-input-number>
              </n-grid-item>
              <n-grid-item>
                <div class="sub-label">重试次数</div>
                <n-input-number v-model:value="formModel.subscribe_assistant.download_retry_limit" size="small" :min="1" />
              </n-grid-item>
              <n-grid-item>
                <div class="sub-label">删种后补搜</div>
                <n-switch v-model:value="formModel.subscribe_assistant.auto_search_when_delete" size="small" />
              </n-grid-item>
              <n-grid-item>
                <div class="sub-label">跳过近期删除</div>
                <n-switch v-model:value="formModel.subscribe_assistant.skip_deletion" size="small" />
              </n-grid-item>
              <n-grid-item>
                <div class="sub-label">删除记录保留</div>
                <n-input-number v-model:value="formModel.subscribe_assistant.delete_record_retention_hours" size="small" :min="1">
                  <template #suffix>小时</template>
                </n-input-number>
              </n-grid-item>
            </n-grid>
            <div class="assistant-wide-control">
              <div class="sub-label">排除标签</div>
              <n-dynamic-tags v-model:value="formModel.subscribe_assistant.delete_exclude_tags" />
            </div>
            <div class="assistant-wide-control">
              <div class="sub-label">Tracker 响应关键字</div>
              <n-dynamic-tags v-model:value="formModel.subscribe_assistant.tracker_keywords" />
            </div>
          </div>
        </div>
      </div>
    </n-spin>
    <template #footer>
      <n-space justify="end">
        <n-button @click="showModal = false">取消</n-button>
        <n-button type="primary" @click="saveConfig" :loading="saving">保存配置</n-button>
      </n-space>
    </template>
  </n-modal>
</template>

<script setup>
import { ref } from 'vue';
import { useMessage } from 'naive-ui';
import axios from 'axios';

const message = useMessage();
const showModal = ref(false);
const loading = ref(false);
const saving = ref(false);

const guardModeOptions = [
  { label: '平衡', value: 'balanced' },
  { label: '严格', value: 'strict' },
  { label: '宽松', value: 'loose' },
  { label: '关闭', value: 'off' }
];
const bestVersionTypeOptions = [
  { label: '关闭', value: 'no' },
  { label: '分集洗版', value: 'tv_episode' },
  { label: '完结洗版', value: 'completed_full' },
  { label: '全集洗版', value: 'tv' }
];
const cleanupTypeOptions = [
  { label: '保留历史', value: 'none' },
  { label: '仅当前剧集', value: 'current' },
  { label: '同 TMDb 全部季', value: 'tmdb' }
];
const cleanupSceneOptions = [
  { label: '洗版订阅', value: 'completed' },
  { label: '状态恢复', value: 'resumed' },
  { label: '手动删除', value: 'manual_delete' },
  { label: '超时重试', value: 'timeout' }
];
const noDownloadActionOptions = [
  { label: '暂停订阅', value: 'pause' },
  { label: '转待定', value: 'pending' },
  { label: '触发补搜', value: 'search' }
];

const defaultSubscribeAssistant = () => ({
  enabled: true,
  guard_mode: 'balanced',
  season_cooldown_days: 14,
  volatility_enabled: true,
  volatility_window_days: 14,
  pending_enhanced_enabled: true,
  pending_use_volatility: false,
  pending_fake_total_episodes: 99,
  pause_enhanced_enabled: true,
  tv_air_pause_days: 14,
  airing_pause_days: 30,
  tv_no_download_days: 0,
  no_download_actions: [],
  download_monitor_enabled: false,
  manual_delete_listen: true,
  tracker_response_listen: true,
  download_timeout_minutes: 120,
  download_progress_threshold: 10,
  download_retry_limit: 3,
  auto_search_when_delete: true,
  skip_deletion: true,
  delete_record_retention_hours: 24,
  delete_exclude_tags: ['H&R'],
  tracker_keywords: ['torrent not registered with this tracker', 'torrent banned'],
  best_version_type: 'tv_episode',
  best_version_episode_to_full: true,
  best_version_full_consistency_check_enabled: true,
  full_washing_timeout_hours: 72,
  subscription_cleanup_history_type: 'none',
  subscription_cleanup_history_scenes: ['completed'],
  verify_enabled: true,
  verify_interval_hours: 12,
  snapshot_retention_days: 180
});

const defaultFormModel = () => ({
  moviepilot_url: '',
  moviepilot_username: '',
  moviepilot_password: '',
  moviepilot_recognition: false,
  resubscribe_daily_cap: 10,
  resubscribe_delay_seconds: 2.0,
  movie_search_window_days: 1,
  movie_pause_days: 7,
  delay_subscription_days: 0,
  subscribe_assistant: defaultSubscribeAssistant()
});

const normalizeFormModel = (data = {}) => ({
  ...defaultFormModel(),
  ...data,
  subscribe_assistant: {
    ...defaultSubscribeAssistant(),
    ...(data.subscribe_assistant || {})
  }
});

const formModel = ref(defaultFormModel());

const open = async () => {
  showModal.value = true;
  loading.value = true;
  try {
    const res = await axios.get('/api/subscription/mp/config');
    if (res.data.success) {
      formModel.value = normalizeFormModel(res.data.data || {});
    }
  } catch (e) {
    message.error('获取配置失败');
  } finally {
    loading.value = false;
  }
};

const saveConfig = async () => {
  saving.value = true;
  try {
    const payload = normalizeFormModel(formModel.value);
    const res = await axios.post('/api/subscription/mp/config', payload);
    if (res.data.success) {
      formModel.value = normalizeFormModel(payload);
      message.success(res.data.message);
      showModal.value = false;
    }
  } catch (e) {
    message.error('保存配置失败');
  } finally {
    saving.value = false;
  }
};

defineExpose({ open });
</script>

<style scoped>
.mp-config-panel {
  display: grid;
  gap: 18px;
}

.mp-config-grid,
.assistant-settings-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: 24px;
  align-items: start;
}

.mp-config-column,
.settings-col {
  min-width: 0;
}

.assistant-hero {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  padding: 18px;
  border: 1px solid var(--card-border-color);
  border-radius: 8px;
  background: color-mix(in srgb, var(--card-bg-color) 84%, var(--accent-color) 16%);
  box-shadow: 0 10px 28px -20px var(--accent-glow-color);
}

.assistant-title {
  font-size: 15px;
  font-weight: 600;
  color: var(--n-text-color-1);
}

.assistant-desc {
  margin-top: 4px;
  font-size: 12px;
  line-height: 1.5;
  color: var(--n-text-color-3);
}

.assistant-wiki-link {
  margin-left: 8px;
  color: var(--accent-color);
  font-weight: 600;
  text-decoration: none;
}

.assistant-wiki-link:hover {
  text-decoration: underline;
}

.settings-group-title {
  font-size: 13px;
  font-weight: 600;
  color: var(--n-text-color-3);
  margin-bottom: 8px;
  padding-left: 4px;
}

.settings-card {
  background-color: var(--card-bg-color);
  border: 1px solid var(--card-border-color);
  border-radius: 8px;
  overflow: hidden;
  box-shadow: 0 8px 24px -18px var(--card-shadow-color);
}

.compact-settings-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: 12px;
}

.assistant-section {
  padding: 16px;
}

.assistant-section-title {
  font-size: 13px;
  font-weight: 600;
  color: var(--n-text-color-1);
  margin-bottom: 12px;
}

.assistant-wide-control {
  margin-top: 12px;
}

.assistant-wide-control :deep(.n-dynamic-tags) {
  width: 100%;
}

.sub-label {
  font-size: 12px;
  color: var(--n-text-color-2);
  margin-bottom: 4px;
}

:deep(.n-input),
:deep(.n-input-number),
:deep(.n-base-selection) {
  --n-color: color-mix(in srgb, var(--card-bg-color) 72%, transparent) !important;
  --n-color-focus: color-mix(in srgb, var(--card-bg-color) 86%, transparent) !important;
  --n-color-disabled: color-mix(in srgb, var(--card-bg-color) 52%, transparent) !important;
  --n-border: 1px solid color-mix(in srgb, var(--card-border-color) 82%, transparent) !important;
  --n-border-hover: 1px solid color-mix(in srgb, var(--accent-color) 42%, var(--card-border-color)) !important;
  --n-border-focus: 1px solid var(--accent-color) !important;
  --n-box-shadow-focus: 0 0 0 2px var(--accent-glow-color) !important;
}

:deep(.n-input-wrapper),
:deep(.n-input__input),
:deep(.n-input__suffix),
:deep(.n-base-selection-label) {
  background: transparent !important;
}

@media (max-width: 760px) {
  .mp-config-grid,
  .assistant-settings-grid,
  .compact-settings-row {
    grid-template-columns: 1fr;
    gap: 16px;
  }

  .assistant-hero {
    padding: 14px;
  }
}
</style>
