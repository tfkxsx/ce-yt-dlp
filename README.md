# ce-yt-dlp

## 项目简介

`ce-yt-dlp` 全称为 `Custom Extension yt-dlp`，是 `yt-dlp` 的外部插件集合。它提供可按需开启的定制能力，可在不改动核心库的前提下切换到增强版 `YoutubeDL` 以及自定义提取器。


## 目录

- [部署流程](#部署流程)
- [使用选项](#使用选项)
- [自定义功能](#自定义功能)
- [自定义提取器](#自定义提取器)


## 部署流程

### 环境要求

- 可运行的 `yt-dlp` 环境。
- 第三方请求库 `requests`。

安装 `yt-dlp` 依赖时，可根据需要执行以下命令之一：

```bash
pip install -e '.[default]'
pip install '.[default]'
```

### 接入步骤

1. 在 `yt-dlp` 项目根目录下放置 `yt_dlp_plugins` 插件项目。
2. 编写下载脚本时，将原生 `YoutubeDL` 替换为插件中的 `CustomYoutubeDL`。
3. 保持原有 `yt-dlp` 使用方式不变，通过配置项开启对应的自定义能力。

### 基础示例

```python
from yt_dlp_plugins.custom_youtube_dl import CustomYoutubeDL


ydl_opts = {
    "outtmpl": "downloads/%(id)s.%(format_id)s.%(ext)s",
    "format": "bestvideo*+bestaudio/best",
    "subtitleslangs": ["en", "en-orig", "zh-Hans", "zh-Hant"],
    "writeautomaticsub": True,
    "subtitlesformat": "json3",
}

VIDEO_URL = "https://www.youtube.com/watch?v=wSSmNUl9Snw"

with CustomYoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(VIDEO_URL, download=True)
```

## 使用选项

### 自定义功能选项


使用自定义功能前，必须先开启 `use_custom_youtube_dl` 总开关。

| 选项 | 默认值 | 说明 |
| :--- | :---: | :--- |
| `use_custom_youtube_dl` | `False` | 自定义功能总开关。关闭时使用原生 `YoutubeDL`，设置为 `True` 后启用自定义功能。 |
| `use_custom_writeautomaticsub` | `False` | 多字幕下载时，单条字幕写入失败仅告警并继续处理下一条。设置为 `True` 后启用。 |


### 自定义提取器选项


使用自定义提取器功能前，必须先开启 `use_custom_youtube_dl` 总开关。

| 选项 | 默认值 | 说明 |
| :--- | :---: | :--- |
| `use_custom_plugins` | `False` | 自定义插件总开关。关闭时使用原生 `YoutubeIE`，设置为 `True` 后启用自定义提取器能力。 |
| `use_custom_player_client` | 无 | 自定义播放器客户端列表，例如 `["tv", "web_safari", "android_sdkless"]`。该选项优先级高于原生 `extractor_args` 设置。 |
| `use_custom_download_webpage_handle` | `False` | 自定义网页下载程序开关。原生下载器异常时可切换使用，设置为 `True` 后启用。 |
| `use_custom_sabr` | 无 | 指定使用 `player` 生成 `potoken` 的客户端，例如 `["web_safari"]`。多个客户端使用逗号分隔。 |
| `use_custom_jsc` | `False` | 使用插件自带的 `yt.solver.core.js` 脚本对 `n/sig` 重签名。设置为 `True` 后启用。 |
| `use_custom_vimeo` | `False` | 使用自定义vimeo插件。设置为 `True` 后启用。 |
| `use_custom_instagram` | `False` | 使用自定义instagram插件 - 支持1080p下载 。设置为 `True` 后启用。 |

更多说明：

- [use_custom_jsc 详情](plugin/yt_dlp_plugins/docs/custom_ejs.md)

## 自定义功能

自定义功能位于 `yt_dlp_plugins/custom_youtube_dl.py`，通过覆盖或重写 `YoutubeDL` 的方法与属性，实现业务定制能力。


### 使用示例

```python
from yt_dlp_plugins.custom_youtube_dl import CustomYoutubeDL


ydl_opts = {
    "outtmpl": "downloads/%(id)s.%(format_id)s.%(ext)s",
    "format": "bestvideo*+bestaudio/best",
    "subtitleslangs": ["en", "en-orig", "zh-Hans", "zh-Hant"],
    "writeautomaticsub": True,
    "subtitlesformat": "json3",

    # 开启自定义功能，并启用多字幕下载增强逻辑
    "use_custom_youtube_dl": True,
    "use_custom_writeautomaticsub": True,
}

VIDEO_URL = "https://www.youtube.com/watch?v=wSSmNUl9Snw"

with CustomYoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(VIDEO_URL, download=True)
```

### 覆写方法一览

| 方法名 | 返回类型 | 说明 |
| :--- | :---: | :--- |
| `_write_subtitles` | `list` | 增强字幕写入鲁棒性。原生逻辑会因单条字幕失败抛异常并中断；覆写后改为告警并继续处理其它语言，避免整次下载被字幕写入失败阻断。 |
| `report_error` | 异常 | 自定义错误码处理，并向上抛出异常。 |
| `dl` | 下载器 | 下载器 Hook 点，可自定义下载器或覆写原生下载器逻辑。 |

更多说明：

- [错误码对照表详情](plugin/yt_dlp_plugins/docs/error_code.md)


## 自定义提取器


自定义提取器位于 `yt_dlp_plugins/extractor/custom_youtube.py`，通过覆盖或重写 `YoutubeIE` 的方法与属性，实现业务定制能力。


### 使用示例

```python
from yt_dlp_plugins.custom_youtube_dl import CustomYoutubeDL


ydl_opts = {
    "outtmpl": "downloads/%(id)s.%(format_id)s.%(ext)s",
    "format": "bestvideo*+bestaudio/best",
    "subtitleslangs": ["en", "en-orig", "zh-Hans", "zh-Hant"],
    "writeautomaticsub": True,
    "subtitlesformat": "json3",

    # 开启自定义插件功能，并指定播放器客户端
    "use_custom_youtube_dl": True,
    "use_custom_plugins": True,
    "use_custom_player_client": [
        "tv",
        "web_safari",
        "android_sdkless",
    ],
}

VIDEO_URL = "https://www.youtube.com/watch?v=wSSmNUl9Snw"

with CustomYoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(VIDEO_URL, download=True)
```

### 覆写方法一览

| 方法名 | 返回类型 | 说明 |
| :--- | :---: | :--- |
| `_get_requested_clients` | `list` | 自定义下载客户端列表。 |
| `_search_regex` | `str` | 从 HTML 或 API 返回的响应中通过正则提取数据。 |
| `_download_webpage_handle` | `tuple` | 自定义配置信息下载器。 |
| `_request_webpage` | `response` | 更细粒度的 `urllib` 请求发送器，可在提交请求时修改请求信息，例如 `m3u8` 清单下载。 |

更多说明：

- [Hook 点对照表详情](plugin/yt_dlp_plugins/docs/hook_detail.md)
