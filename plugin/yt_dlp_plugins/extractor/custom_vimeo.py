# -*- coding: utf-8 -*-

import requests
from yt_dlp.extractor.vimeo import VimeoIE as _VimeoIE


class NO_DEFAULT:
    pass


class VimeoIE(_VimeoIE):
    _PLUGIN_PRIORITY = 100  # 覆盖内置
    _ENABLED = True

    @property
    def enable_custom(self):
        return self.get_param("use_custom_plugins", False)

    @property
    def _is_logged_in(self):
        return True

    def _download_webpage_handle(
        self,
        url_or_request,
        video_id,
        note=None,
        errnote=None,
        fatal=True,
        encoding=None,
        data=None,
        headers={},
        query={},
        expected_status=None,
        impersonate=None,
        require_impersonation=False,
    ):
        # 这里返回一个元组或list， 其中content 是请求的结果 string类型， 第二个返回值没有实际使用， 仅仅占位。
        # 我们可以在这里使用reqeusts 库来覆盖原生方法，
        # 如果这是更改参数，那么建议在 self._request_webpage 中修改参数更合适
        if self.enable_custom and self.get_param("use_custom_download_webpage_handle"):
            self.write_debug(
                f"[CustomYoutubeIE] handling --> _download_webpage_handle : {url_or_request} - {video_id}"
            )
            proxy = self.get_param("proxy")
            proxies = {"http": proxy, "https": proxy}
            if (
                "https://vimeo.com/" in url_or_request
                or "player.vimeo.com/video/" in url_or_request
            ):
                # webpage： 获取网页内容
                self.write_debug(
                    f"[CustomYoutubeIE] handling --> _download_webpage_handle : WEB PAGE"
                )
                response = requests.get(
                    url_or_request,
                    params=query,
                    headers=headers,
                    proxies=proxies,
                    timeout=20,
                )
                response.encoding = response.apparent_encoding
                return (response.text, response)
        return super()._download_webpage_handle(
            url_or_request,
            video_id,
            note,
            errnote,
            fatal,
            encoding,
            data,
            headers,
            query,
            expected_status,
            impersonate,
            require_impersonation,
        )
