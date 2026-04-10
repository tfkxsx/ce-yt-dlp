## 项目简介

`ce-yt-dlp` 全称 `Custom Extension yt-dlp`, 是对 `yt-dlp` 的外部插件集合，提供可按需开启的定制能力，在不改动核心库的前提下切换到增强版 `YoutubeDL` 及自定义 `YouTube` 提取器。


## 部署流程

> 项目依赖 reqeusts 第三方请求库及可运行的yt-dlp环境， 在安装yt-dlp 环境时，
> 可通过：pip install -e '.[default]' or pip install '.[default]' 进行依赖安装。

1. 在`yt-dlp` 项目根目录下放置 `yt_dlp_plugins`项目。
2. 编写下载脚本时把原生的YoutubeDL 替换成插件中的 CustomYoutubeDL， yt-dlp 的使用方式不变。
3. 使用示例程序 `main.py`（见下方示例配置），即可触发插件逻辑:

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

使用自定义功能必须先开启 use_custom_youtube_dl 总开关

```bash

use_custom_youtube_dl                 自定义功能总开关, 默认关闭状态（False）使用原生 YoutubeDL功能，设置为True 开启自定义功能。
use_custom_writeautomaticsub          多字幕下载时写入字幕失败仅告警并继续下一条，默认关闭状态（False）， 设置为True 开启。
sabr_concurrency                      sabr 并发下载参数, 详细见 "SABR ARGUMENTS" 部分

```


### 自定义提取器选项

使用自定义提取器选项必须先开启 use_custom_plugins 总开关

```bash

use_custom_plugins                    自定义插件总开关, 默认关闭状态（False）使用原生 YoutubeIE功能，设置为True 开启自定义功能。
use_custom_player_client              自定义播放器客户端列表，例如：["tv", "web_safari", "android_sdkless"], 注意此功能优先级高于原生: extractor_args的设置
use_custom_download_webpage_handle    自定义web页面下载程序开关，在原生下载器出问题时可进行切换。默认关闭状态（False）， 设置为True 开启。
use_custom_sabr                       指定使用 player 生成 potoken的客户端，例如：["web_safari"]，多个客户端使用逗号（,）分割
youtube-ejs                           yt-dlp 开发者模式解决ejs 签名（ n/sig 重签名）, 详细见 "youtube-ejs" 部分

```
- [use_custom_jsc 详情](plugin/yt_dlp_plugins/docs/custom_ejs.md)



### SABR ARGUMENTS
sabr 并发下载参数， 接受一个 dict 对象如 `sabr_concurrency: {}`

| 配置项 | 作用 | 默认值 |
|---|---|---:|
| `enabled` | 是否启用并发 SABR 下载 | `True` |
| `thread_number` | 窗口并发线程数 | `4` |
| `warmup_segments` | 热身阶段样本 segment 数 | `4` |
| `window_start` | 用户指定的起始窗口 | `1` |
| `window_count` | 窗口数，`None` 表示自动计算 | `None` |
| `repair_rounds` | 修复轮次 | `2` |
| `window_size_verify_attempts` | stride 检测验证次数 | `1` |
| `dynamic_start_enabled` | 是否启用动态起始窗口 | `True` |
| `window_planning_mode` | 窗口规划模式 | `interleaved` |
| `interleaving_enabled` | 是否启用交错调度 | `True` |
| `window_overlap_segments` | 窗口重叠 segment 数 | `1` |
| `state_path` | 状态文件路径 | 自动生成 |
| `segment_dir` | 中间 part 目录 | 自动生成 |
| `state_save_interval_sec` | state 节流时间窗口 | `0.5` |
| `state_save_every_ops` | state 节流操作阈值 | `8` |
| `enable_concurrent_merge` | 是否允许并发合并 | `True` |
| `merge_concurrency` | 显式指定合并并发数 | `None` |
| `merge_copy_buffer_size` | 合并拷贝缓冲区大小 | `1 MiB` |

- [sabr 并发下载详细说明](plugin/yt_dlp_plugins/docs/sabr并发详解.md)


### youtube-ejs
youtube-ejs 是yt-dlp 原生自带功能， 这里利用其特性来加载自定义的ejs `yt.solver.core.min.js` 脚本， 接受一个 dict 对象如 `youtube-ejs: {}`

| 配置项 | 作用 | 默认值 |
|---|---|---:|
| `dev` | 是否启用并发 SABR 下载 | `["true"]` |
| `repo` | 是否启用并发 SABR 下载 | `["tfkxsx/tfkxsx-yt-dlp-ejs"]` |
| `script_version` | 是否启用并发 SABR 下载 | `["0.0.1"]` |

注意：由于`yt.solver.core.min.js` 脚本更新很快， 所有一般需要额外配置两个 `extractor_args` 参数：
```json
{
    "extractor_args": {
        "youtube": {
            "player_js_version": ['20521@18d29a11'],
            "player_js_variant": ["es6"],
        }
    }
}
```
- yt.solver.core.min.js 版本号：18d29a11； signatureTimestamp：20521
- 当前版本来源平台： es6


## 自定义功能

位于yt_dlp_plugins/custom_youtube_dl.py 中，为自定义功能实现，通过对 YoutubeDL 方法或属性的覆盖或重写，达到自定义的业务目标

### 使用示例

```python
from yt_dlp_plugins.custom_youtube_dl import CustomYoutubeDL


ydl_opts = {
    "outtmpl": "downloads/%(id)s.%(format_id)s.%(ext)s",
    "format": "bestvideo*+bestaudio/best",
    "subtitleslangs": ["en", "en-orig", "zh-Hans", "zh-Hant"],
    "writeautomaticsub": True,
    "subtitlesformat": "json3",

    # 开启开启自定义功能，并使用多字幕下载功能
    "use_custom_youtube_dl": True,
    "use_custom_writeautomaticsub": True
}
VIDEO_URL = "https://www.youtube.com/watch?v=wSSmNUl9Snw"
with CustomYoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(VIDEO_URL, download=True)

```

### 底层覆写方法一览与设计目的

|    **方法名**     | **返回类型** | **说明**                                                                                                   |
| :---------------: | :----------: | :--------------------------------------------------------------------------------------------------------- |
| \_write_subtitles |     list     | 字幕写入更鲁棒。默认会因单条字幕失败而抛异常中断；覆写后改为 warn 并继续其它语言，避免整次下载被字幕拖垮。 |
|   report_error    | 向上抛出异常 | 自定义错误码处理                                                                                           |
|        dl         |    下载器    | 下载器 hook 点，可自定义下载器或覆写原生下载器逻辑                                                         |

- [错误码对照表详情](plugin/yt_dlp_plugins/docs/error_code.md)


## 自定义提取器

位于 yt_dlp_plugins/extractor/custom_youtube.py 中，为自定义提取器插件，通过对 YoutubeIE 方法或属性的覆盖或重写，达到自定义的业务目标

### 使用示例

```python
from yt_dlp_plugins.custom_youtube_dl import CustomYoutubeDL


ydl_opts = {
    "outtmpl": "downloads/%(id)s.%(format_id)s.%(ext)s",
    "format": "bestvideo*+bestaudio/best",
    "subtitleslangs": ["en", "en-orig", "zh-Hans", "zh-Hant"],
    "writeautomaticsub": True,
    "subtitlesformat": "json3",

    # 开启自定义插件功能， 并使用指定客户端功能
    "use_custom_plugins": True,
    "use_custom_player_client": [
        "tv",
        "web_safari",
        "android_sdkless",
    ]
}
VIDEO_URL = "https://www.youtube.com/watch?v=wSSmNUl9Snw"
with CustomYoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(VIDEO_URL, download=True)

```

### 底层覆写方法一览与设计目的

|        **方法名**         | **返回类型** | **说明**                                                                  |
| :-----------------------: | :----------: | :------------------------------------------------------------------------ |
|  \_get_requested_clients  |     list     | 下载客户端列表                                                            |
|      \_search_regex       |     str      | 正则匹配，从 HTML 或 API 返回的 response 中提取数据                       |
| \_download_webpage_handle |    Tuple     | 配置信息下载器                                                            |
|     \_request_webpage     |   response   | urllib 请求发送器，粒度更细，可在提交请求时更改请求信息，如 m3u8 清单下载 |

- [hook点对照表详情](plugin/yt_dlp_plugins/docs/hook_detail.md)
