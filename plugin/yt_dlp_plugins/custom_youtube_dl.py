from yt_dlp import YoutubeDL as _YoutubeDL
from yt_dlp.utils import subtitles_filename
from yt_dlp.downloader import get_suitable_downloader
from yt_dlp_plugins.downloader.http import HttpFD
from yt_dlp_plugins.downloader.sabr import SabrFD
from yt_dlp_plugins.error import error_codes


class CustomYoutubeDL(_YoutubeDL):

    def __init__(self, params=None, *args, **kwargs):
        """
        参数开关：
        - params['use_custom_youtube_dl']: 为 True 时启用本类的自定义实现, 为 False/缺省时，
            除 __init__ 之外的覆写方法都会“失效”，转回父类逻辑。默认启用。
        """
        params = params or {}
        # 提前取出开关，避免父类 __init__ 处理未知参数
        self.enable_custom = params.pop("use_custom_youtube_dl", False)
        super().__init__(params, *args, **kwargs)

    def __getattribute__(self, name):
        """TODO
        一键启/停 - 自定义方法
        当 enable_custom 为 False 时，自动将本类覆写的非魔术属性/方法代理回
        _YoutubeDL 的同名实现（__init__ 本身和必要的内部属性除外）
        """
        # 这些属性必须走默认流程，避免递归或获取失败
        passthrough_names = {
            "enable_custom",
            "__class__",
            "__getattribute__",
            "__init__",
        }
        if name in passthrough_names:
            return object.__getattribute__(self, name)

        enable_custom = object.__getattribute__(self, "enable_custom")
        if not enable_custom:
            # 如果当前类覆写了该属性且父类存在同名属性，则直接使用父类实现
            cls_dict = type(self).__dict__
            if name in cls_dict and not name.startswith("__"):
                base_attr = getattr(_YoutubeDL, name, None)
                if base_attr is not None:
                    # 支持方法和普通属性
                    if callable(base_attr):
                        return base_attr.__get__(self, _YoutubeDL)
                    return base_attr

        return object.__getattribute__(self, name)

    def _write_subtitles(self, info_dict, filename):
        """
        重写字幕下载逻辑，如果下载失败不会抛异常， 而是继续下载下一个字幕
        """
        if not self.params.get("use_custom_writeautomaticsub", False):
            return super()._write_subtitles(info_dict, filename)

        self.to_screen(f"[info] CustomYoutubeDL handling --> _write_subtitles")
        ret = []
        subtitles = info_dict.get("requested_subtitles")
        if not (
            self.params.get("writesubtitles") or self.params.get("writeautomaticsub")
        ):
            # subtitles download errors are already managed as troubles in relevant IE
            # that way it will silently go on when used with unsupporting IE
            return ret
        elif not subtitles:
            self.to_screen("[info] There are no subtitles for the requested languages")
            return ret
        sub_filename_base = self.prepare_filename(info_dict, "subtitle")
        if not sub_filename_base:
            self.to_screen("[info] Skipping writing video subtitles")
            return ret

        for sub_lang, sub_info in subtitles.items():
            sub_format = sub_info["ext"]
            sub_filename = subtitles_filename(
                filename, sub_lang, sub_format, info_dict.get("ext")
            )
            sub_filename_final = subtitles_filename(
                sub_filename_base, sub_lang, sub_format, info_dict.get("ext")
            )
            existing_sub = self.existing_file((sub_filename_final, sub_filename))
            if existing_sub:
                self.to_screen(
                    f"[info] Video subtitle {sub_lang}.{sub_format} is already present"
                )
                sub_info["filepath"] = existing_sub
                ret.append((existing_sub, sub_filename_final))
                continue

            self.to_screen(f"[info] Writing video subtitles to: {sub_filename}")
            if sub_info.get("data") is not None:
                try:
                    # Use newline='' to prevent conversion of newline characters
                    # See https://github.com/ytdl-org/youtube-dl/issues/10268
                    with open(
                        sub_filename, "w", encoding="utf-8", newline=""
                    ) as subfile:
                        subfile.write(sub_info["data"])
                    sub_info["filepath"] = sub_filename
                    ret.append((sub_filename, sub_filename_final))
                    continue
                except OSError:
                    self.report_error(
                        f"Cannot write video subtitles file {sub_filename}"
                    )
                    return None

            try:
                sub_copy = sub_info.copy()
                sub_copy.setdefault("http_headers", info_dict.get("http_headers"))
                self.dl(sub_filename, sub_copy, subtitle=True)
                sub_info["filepath"] = sub_filename
                ret.append((sub_filename, sub_filename_final))
            except Exception as err:
                msg = f"Unable to download video subtitles for {sub_lang!r}: {err}"
                self.report_warning(msg)
        return ret

    def report_error(self, message, *args, **kwargs):
        """
        错误码处理
        """
        try:
            super().report_error(message, *args, **kwargs)
        except Exception as err:
            message = err.msg
            for k, v in error_codes.items():
                if v in message:
                    err.error_code = int(k)
                    break
            raise err

    def _calculate_total_filesize(self, info_dict):
        """
        计算总文件大小（合并音视频格式）
        返回：总大小（字节），0表示无法计算大小
        """
        total_bytes = 0
        
        # 检查是否有requested_formats（音视频分离）
        requested_formats = info_dict.get('requested_formats')
        if requested_formats:
            # 合并计算所有格式的大小
            for fmt in requested_formats:
                filesize = fmt.get('filesize') or fmt.get('filesize_approx')
                if filesize:
                    try:
                        total_bytes += int(filesize)
                    except (ValueError, TypeError):
                        pass
        else:
            # 单个格式
            filesize = info_dict.get('filesize') or info_dict.get('filesize_approx')
            if filesize:
                try:
                    total_bytes = int(filesize)
                except (ValueError, TypeError):
                    pass
        
        return total_bytes  # 返回0或正数

    def _adjust_sabr_concurrency_by_filesize(self, info_dict):
        """
        根据文件大小调整SABR并发配置
        逻辑：
        - 文件大小 < 10MB：禁用并发，使用原始SABR下载（无并发）
        - 文件大小为0：视为无法获取大小，保持现有配置
        - 10MB < 文件大小 <= 50MB：启用并发，使用用户指定的thread_number或默认2并发
        - 文件大小 > 50MB：启用并发，使用用户指定的thread_number或默认4并发
        """
        total_bytes = self._calculate_total_filesize(info_dict)
        
        MB = 1024 * 1024
        enable_threshold = 10 * MB
        high_threshold = 50 * MB
        
        # 获取或初始化sabr_concurrency配置
        sabr_config = self.params.get('sabr_concurrency', {})
        if sabr_config is True:
            sabr_config = {}
        
        # 如果文件大小为0，视为无法获取大小，保持现有配置
        if total_bytes == 0:
            self.to_screen('[sabr-auto] 无法获取文件大小，使用默认配置')
            return
        
        file_size_mb = total_bytes / MB
        
        if total_bytes >= enable_threshold:
            # >=10MB：启用并发
            default_thread_number = 4 if total_bytes > high_threshold else 2

            # 使用用户指定的thread_number或按分档设置默认值
            if 'thread_number' not in sabr_config:
                sabr_config['thread_number'] = default_thread_number

            if total_bytes > high_threshold:
                self.to_screen(
                    f'[sabr-auto] 文件大小 {file_size_mb:.1f}MB > 50MB，'
                    f'启用{sabr_config["thread_number"]}并发下载'
                )
            else:
                self.to_screen(
                    f'[sabr-auto] 文件大小 {file_size_mb:.1f}MB 在 10MB-50MB 区间，'
                    f'启用{sabr_config["thread_number"]}并发下载'
                )
        else:
            # <10MB：禁用并发
            sabr_config['enabled'] = False
            self.to_screen(f'[sabr-auto] 文件大小 {file_size_mb:.1f}MB < 10MB，小文件默认不开启并发下载')
        
        # 更新配置
        self.params['sabr_concurrency'] = sabr_config

    def dl(self, name, info, subtitle=False, test=False):
        """
        原生下载器 hook 点
        """
        if not info.get("url"):
            self.raise_no_formats(info, True)

        if test:
            verbose = self.params.get("verbose")
            quiet = self.params.get("quiet") or not verbose
            params = {
                "test": True,
                "quiet": quiet,
                "verbose": verbose,
                "noprogress": quiet,
                "nopart": True,
                "skip_unavailable_fragments": False,
                "keep_fragments": False,
                "overwrites": True,
                "_no_ytdl_file": True,
            }
        else:
            params = self.params

        fd_class = get_suitable_downloader(info, params, to_stdout=(name == "-"))
        
        # 根据文件大小调整SABR并发配置（仅非测试模式）
        if not test and fd_class.FD_NAME == "sabr":
            self._adjust_sabr_concurrency_by_filesize(info)
        
        # hook 内部下载器, 覆盖为自定义下载器
        if fd_class.FD_NAME == "http":
            self.write_debug(f"[CustomYoutubeDL] handling --> dl : http")
            fd_class = HttpFD
        elif fd_class.FD_NAME == "sabr":
            self.write_debug(f"[CustomYoutubeDL] handling --> dl : sabr")
            # 替换为自定义的 SABR 下载器（包含并发功能）
            fd_class = SabrFD
        fd = fd_class(self, params)
        if not test:
            for ph in self._progress_hooks:
                fd.add_progress_hook(ph)
            urls = '", "'.join(
                (
                    f["url"].split(",")[0] + ",<data>"
                    if f["url"].startswith("data:")
                    else f["url"]
                )
                for f in info.get("requested_formats", []) or [info]
            )
            self.write_debug(f'Invoking {fd.FD_NAME} downloader on "{urls}"')

        # Note: Ideally info should be a deep-copied so that hooks cannot modify it.
        # But it may contain objects that are not deep-copyable
        new_info = self._copy_infodict(info)
        if new_info.get("http_headers") is None:
            new_info["http_headers"] = self._calc_headers(new_info)
        return fd.download(name, new_info, subtitle)
