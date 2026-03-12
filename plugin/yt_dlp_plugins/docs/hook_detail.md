# 重要 Hook 点对照表

| **注入点** | **源码位置（文件/方法）** | **返回类型** | **说明 / 数据示例** |
| :--: | :-- | :--: | :-- |
| webpage | yt_dlp/extractor/youtube/_video.py<br>_initial_extract → _download_initial_webpage | string | webpage.html，初始 watch 页 HTML |
| webpage_ytcfg | yt_dlp/extractor/youtube/_video.py<br>_initial_extract → extract_ytcfg | json | webpage_ytcfg.json，页面级 ytcfg |
| initial_data | yt_dlp/extractor/youtube/_video.py<br>_initial_extract → _download_initial_data | json | initial_data.json，含推荐流/播放器节点 |
| _get_requested_clients | yt_dlp/extractor/youtube/_video.py<br>_initial_extract | list | 客户端选择结果。默认集：<br>有 JS：('android_sdkless','web','web_safari')<br>无 JS：('android_sdkless',)<br>登陆：('tv_downgraded','web','web_safari') |
| initial_pr | yt_dlp/extractor/youtube/_video.py<br>_extract_player_responses → _search_json | json | initial_pr.json，初始 player response |
| player_ytcfg | yt_dlp/extractor/youtube/_video.py<br>_extract_player_responses → _download_ytcfg | json | 非 web 客户端时从对应 HTML 抽取（如 tv.html → tv-ytcfg.json）。仅对 mweb / web / web_music / web_embedded / tv 重置。 |
| js code | yt_dlp/extractor/youtube/_video.py<br>_load_player → _download_webpage | string | base.js，并设置 self._code_cache |
| sts | yt_dlp/extractor/youtube/_video.py<br>_extract_signature_timestamp | int | 签名时间戳，示例：20458 |
| 客户端响应示例 | _download_webpage_handle | string | tv → tv.txt；web_safari → web_safari.txt；android_sdkless → android_sdkless.txt；web → web.txt（通常从 webpage.html 提取） |
| index.m3u8 | yt_dlp/extractor/common.py<br>_extract_m3u8_formats_and_subtitles → _request_webpage | response | urllib.request.urlopen 的返回对象，便于在插件中注入代理/参数 |
| HttpFD 下载器 | yt_dlp/downloader/http.py<br>HttpFD.real_download | bool | 默认 HTTP 下载器，返回 download() 结果，可在插件中替换为自定义 HttpFD |