from yt_dlp import YoutubeDL as _YoutubeDL
from yt_dlp.utils import subtitles_filename
from yt_dlp.downloader import get_suitable_downloader
from yt_dlp_plugins.downloader.http import HttpFD
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
        # hook 内部下载器, 覆盖为自定义下载器
        if fd_class.FD_NAME == "http":
            self.write_debug(f"[CustomYoutubeDL] handling --> dl : http")
            fd_class = HttpFD
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
