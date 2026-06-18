# NapCat 大文件传输插件

`napcat_large_file_transport` 用于在不修改 KiraAI 核心源码的情况下，绕开 QQ/NapCat 大文件通过 `base64://` 单包发送导致 WebSocket 断连的问题。

## 工作方式

- 透明接管：插件启动后包装 QQ/NapCat adapter 的 `send_group_message` 和 `send_direct_message`。当消息链中只有单个 `File` 或 `Video`，且文件达到 `stream_threshold_mb` 或属于危险大 base64/data_url 时，插件接管发送。
- 显式引导：插件注册 `<napcat_file type="file|video">path_or_url</napcat_file>`，并通过 `usage_prompt` 注入 LLM 请求，引导模型不要把大文件转成 base64 文本。
- 传输策略：本地文件优先 `upload_file_stream`，失败或不支持时使用临时 HTTP 下载 URL；仍失败时返回清晰错误，不回退到大 base64。
- 日志观察：开启 `debug_log` 后会记录文件大小、分片数量、最终 NapCat action、传输策略、耗时和错误摘要，不记录完整 HTTP token 或文件内容。

## Docker / 远程 NapCat

推荐 NapCat 使用 v4.10.x 及以上。插件不会硬卡版本，会在首次 stream 失败或返回 `1404` 时自动标记 stream 不可用并转入 HTTP 兜底。

如果 KiraAI 与 NapCat 不在同一网络命名空间，建议优先让 `upload_file_stream` 可用；HTTP 兜底需要配置 `public_base_url` 为 NapCat 可访问的 KiraAI 地址，例如：

```text
http://host.docker.internal:5267
https://your-kiraai.example.com
```

HTTP 下载地址形如：

```text
{public_base_url}/api/plugin/napcat_large_file_transport/download/{token}
```

token 默认 10 分钟过期，仅映射到插件确认过的文件路径。

## 主要配置

- `enabled`：启用插件。
- `intercept_existing_file_tag`：是否透明接管原有 `<file>` / `<file type="video">`。
- `usage_prompt`：可在 WebUI 修改的 LLM 使用提示词。
- `stream_enabled`：启用 `upload_file_stream`。
- `http_fallback_enabled`：启用 HTTP 兜底。
- `public_base_url`：HTTP 兜底时 NapCat 可访问的 KiraAI 外部地址。
- `stream_threshold_mb`：达到该大小后由插件接管，默认 8 MB。
- `base64_max_mb`：base64/data_url 放行上限，默认 20 MB。
- `chunk_size_kb`：stream 分片大小，默认 512 KB。
- `qq_adapter_names`：限定 adapter 名称，留空自动匹配所有 QQ/NapCat adapter。

## 验收建议

1. QQ 普通文本、图片和转发消息不受影响。
2. `<file>` 发送 1 MB 文件仍走原 adapter 并成功。
3. `<file>` 发送 30 MB 本地文件不再触发大 base64 单包，也不导致 NapCat 断连。
4. Docker/远程 NapCat 下，`upload_file_stream` 成功时直接发送。
5. 禁用 stream 或 NapCat 不支持 stream 时，配置 `public_base_url` 后 HTTP 兜底成功。
6. 未配置 `public_base_url` 且 stream 不可用时，返回可读错误。
7. 插件禁用或卸载后，adapter 方法恢复原始行为。

## 日志颜色

插件使用独立 logger 名称 `napcat_large_file_transport`，颜色设置为 orange，便于和普通插件日志保持接近，同时保留独立前缀方便筛选。
