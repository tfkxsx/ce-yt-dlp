# -*- coding: utf-8 -*-

import urllib
import requests
from yt_dlp.extractor.youtube import YoutubeIE as _YoutubeIE
from yt_dlp.utils import (
    NO_DEFAULT,
    traverse_obj,
)
from yt_dlp.extractor.youtube.pot.provider import PoTokenContext, PoTokenRequest
from yt_dlp.utils.networking import clean_headers, clean_proxies, select_proxy


class NO_DEFAULT:
    pass


class YoutubeIE(_YoutubeIE):
    _PLUGIN_PRIORITY = 100  # 覆盖内置
    _ENABLED = True

    @property
    def enable_custom(self):
        return self.get_param("use_custom_plugins", False)

    def _get_requested_clients(self, url, smuggled_data, is_premium_subscriber):
        use_custom_player_client = self.get_param("use_custom_player_client")
        if self.enable_custom and use_custom_player_client:
            self.write_debug(f"[CustomYoutubeIE] handling --> _get_requested_clients")
            if isinstance(use_custom_player_client, str):
                use_custom_player_client = list(use_custom_player_client)
            elif isinstance(use_custom_player_client, list):
                pass
            else:
                raise ValueError(
                    f"use_custom_player_client must be a string or list, got {type(use_custom_player_client)}"
                )
            return use_custom_player_client

        return super()._get_requested_clients(url, smuggled_data, is_premium_subscriber)

    def _search_regex(
        self, pattern, string, name, default=NO_DEFAULT, fatal=True, flags=0, group=None
    ):
        if self.enable_custom:
            if r"ytcfg\.set\s*\(\s*({.+?})\s*\)\s*;" == pattern:
                self.write_debug(
                    f"[CustomYoutubeIE] handling --> _search_json : webpage_ytcfg"
                )
            elif (
                r'(?:(?:window\s*\[\s*["\']ytInitialData["\']\s*\]|ytInitialData)\s*=)\s*(?P<json>{(?s:.+)})\s*(?:)'
                == pattern
            ):
                self.write_debug(
                    f"[CustomYoutubeIE] handling --> _search_json : initial_data"
                )
            elif (
                r"(?:ytInitialPlayerResponse\s*=)\s*(?P<json>{(?s:.+)})\s*(?:)"
                == pattern
            ):
                self.write_debug(
                    f"[CustomYoutubeIE] handling --> _search_json : initial_pr"
                )
            elif r"(?:signatureTimestamp|sts)\s*:\s*(?P<sts>[0-9]{5})" == pattern:
                self.write_debug(f"[CustomYoutubeIE] handling --> _search_json : sts")
        return super()._search_regex(
            pattern, string, name, default, fatal, flags, group
        )

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
            if "https://www.youtube.com/watch?v=" in url_or_request:
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
                return (response.text, None)
            elif "https://www.youtube.com/tv" == url_or_request:
                self.write_debug(
                    f"[CustomYoutubeIE] handling --> _download_webpage_handle : TV PAGE"
                )
                response = requests.get(
                    url_or_request,
                    params=query,
                    data=data,
                    headers=headers,
                    proxies=proxies,
                    timeout=20,
                )
                response.encoding = response.apparent_encoding
                return (response.text, None)
            elif ".js" in url_or_request:
                # 获取 JavaScript runtime 的 js code
                self.write_debug(
                    f"[CustomYoutubeIE] handling --> _download_webpage_handle : JS CODE"
                )
                response = requests.get(
                    url_or_request,
                    params=query,
                    data=data,
                    headers=headers,
                    proxies=proxies,
                    timeout=20,
                )
                response.encoding = response.apparent_encoding
                return (response.text, None)
            elif "youtubei/v1/player" in url_or_request:
                # 各客户端获取player response
                # Strip hashes from the URL (#1038)
                if isinstance(url_or_request, str):
                    url_or_request = url_or_request.partition("#")[0]
                if b"ANDROID" in data:
                    self.write_debug(
                        f"[CustomYoutubeIE] handling --> _download_webpage_handle : ANDROID SDKLESS"
                    )
                elif b"WEB" in data and b"Mac OS X 10_15_7" in data:
                    self.write_debug(
                        f"[CustomYoutubeIE] handling --> _download_webpage_handle : WEB_SAFARI"
                    )
                elif b"WEB" in data:
                    self.write_debug(
                        f"[CustomYoutubeIE] handling --> _download_webpage_handle : WEB_CHROME"
                    )
                elif b"TVHTML5" in data and b"7.20260114.12.00" in data:
                    self.write_debug(
                        f"[CustomYoutubeIE] handling --> _download_webpage_handle : TV"
                    )
                elif b"TVHTML5" in data and b"5.20260114" in data:
                    self.write_debug(
                        f"[CustomYoutubeIE] handling --> _download_webpage_handle : TV_DOWNGRADED"
                    )
                else:
                    self.write_debug(
                        f"[CustomYoutubeIE] handling --> _download_webpage_handle : UNKNOWN"
                    )
                response = requests.post(
                    url_or_request,
                    params=query,
                    data=data,
                    headers=headers,
                    proxies=proxies,
                    timeout=20,
                )
                response.encoding = response.apparent_encoding
                content = response.text
                return (content, None)
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

    def _request_webpage(
        self,
        url_or_request,
        video_id,
        note=None,
        errnote=None,
        fatal=True,
        data=None,
        headers=None,
        query=None,
        expected_status=None,
        impersonate=None,
        require_impersonation=False,
    ):
        """TODO
        可以修改下载参数: url_or_request、data、headers、query， 达到hook 下载的目的
        这里是对self._download_webpage_handle 的补充， 可以在这里修改请求参数， 继续让程序使用默认的 urllib 做请求
        """
        if self.enable_custom and self.get_param("use_custom_download_webpage_handle"):
            self.write_debug(
                f"[CustomYoutubeIE] handling --> _request_webpage : video_id - {video_id}"
            )
            if "index.m3u8" in url_or_request and "hls_variant" in url_or_request:
                self.write_debug(
                    f"[CustomYoutubeIE] handling --> _request_webpage : M3U8 MANIFEST"
                )
                proxy = self.get_param("proxy")
                proxy_handler = urllib.request.ProxyHandler(
                    {"http": proxy, "https": proxy}
                )
                opener = urllib.request.build_opener(proxy_handler)
                urllib.request.install_opener(opener)

                # urllib.request.Request 不支持 query、expected_status 等参数，手动拼接查询参数
                url = url_or_request
                if query:
                    qs = urllib.parse.urlencode(query)
                    url = f"{url}{'&' if '?' in url else '?'}{qs}"
                request = urllib.request.Request(
                    url,
                    headers=headers or {},
                    data=data,
                )
                response = urllib.request.urlopen(request)
                return response

        return super()._request_webpage(
            url_or_request,
            video_id,
            note,
            errnote,
            fatal,
            data,
            headers,
            query,
            expected_status,
            impersonate,
            require_impersonation,
        )

    def _fetch_po_token(self, client, **kwargs):
        if not self.enable_custom or not self.get_param("use_custom_sabr"):
            return super()._fetch_po_token(client, **kwargs)

        context = kwargs.get('context')
        custom_sabr_client = [self.get_param("use_custom_sabr")] if isinstance(self.get_param("use_custom_sabr"), str) else self.get_param("use_custom_sabr")

        if client in custom_sabr_client and context != "player":
            context = "player"
            kwargs['required'] = True

        # Avoid fetching PO Tokens when not required
        fetch_pot_policy = self._configuration_arg('fetch_pot', [''], ie_key=YoutubeIE)[0]
        if fetch_pot_policy not in ('never', 'auto', 'always'):
            fetch_pot_policy = 'auto'
        if (
            fetch_pot_policy == 'never'
            or (
                fetch_pot_policy == 'auto'
                and not kwargs.get('required', False)
            )
        ):
            return None

        headers = self.get_param('http_headers').copy()
        proxies = self._downloader.proxies.copy()
        clean_headers(headers)
        clean_proxies(proxies, headers)

        innertube_host = self._select_api_hostname(None, default_client=client)

        pot_request = PoTokenRequest(
            context=PoTokenContext(context),
            innertube_context=traverse_obj(kwargs, ('ytcfg', 'INNERTUBE_CONTEXT')),
            innertube_host=innertube_host,
            internal_client_name=client,
            session_index=kwargs.get('session_index'),
            player_url=kwargs.get('player_url'),
            video_webpage=kwargs.get('video_webpage'),
            is_authenticated=self.is_authenticated,
            visitor_data=kwargs.get('visitor_data'),
            data_sync_id=kwargs.get('data_sync_id'),
            video_id=kwargs.get('video_id'),
            request_cookiejar=self._downloader.cookiejar,
            _gvs_bind_to_video_id=kwargs.get('_gvs_bind_to_video_id', False),

            # All requests that would need to be proxied should be in the
            # context of www.youtube.com or the innertube host
            request_proxy=(
                select_proxy('https://www.youtube.com', proxies)
                or select_proxy(f'https://{innertube_host}', proxies)
            ),
            request_headers=headers,
            request_timeout=self.get_param('socket_timeout'),
            request_verify_tls=not self.get_param('nocheckcertificate'),
            request_source_address=self.get_param('source_address'),

            bypass_cache=kwargs.get('bypass_cache', False),
        )

        return self._pot_director.get_po_token(pot_request)
