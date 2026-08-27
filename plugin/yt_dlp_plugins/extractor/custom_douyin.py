# -*- coding: utf-8 -*-

import re, urllib
from yt_dlp.utils import variadic,traverse_obj, int_or_none, url_or_none,str_or_none, format_field, truncate_string, ExtractorError
from yt_dlp.extractor.tiktok import DouyinIE as _DouyinIE


class DouyinIE(_DouyinIE):
    _VALID_URL = r'https?://(?:www\.)?douyin\.com/(?:video/(?P<id>[0-9]+)|[^?#]*\?(?:[^#]*&)?modal_id=(?P<modal_id>[0-9]+))'

    @property
    def enable_custom(self):
        return self.get_param("use_custom_plugins", True)

    def _extract_douyin_web_formats(self, video_info):
        """Extract formats from Douyin web page's videoDetail.video structure."""
        formats = []
        play_width = int_or_none(video_info.get('width'))
        play_height = int_or_none(video_info.get('height'))

        for bitrate_info in traverse_obj(video_info, ('bitRateList', lambda _, v: v.get('playAddr'))):
            is_h265 = bitrate_info.get('isH265')
            fmt = bitrate_info.get('format', 'mp4')
            # skip DASH segments, prefer progressive mp4
            if fmt == 'dash':
                continue
            for addr in traverse_obj(bitrate_info, ('playAddr', ..., 'src', {url_or_none})):
                formats.append({
                    'url': addr,
                    'ext': 'mp4',
                    'width': int_or_none(bitrate_info.get('width')),
                    'height': int_or_none(bitrate_info.get('height')),
                    'tbr': int_or_none(bitrate_info.get('bitRate'), scale=1000),
                    'filesize': int_or_none(bitrate_info.get('dataSize')),
                    'vcodec': 'h265' if is_h265 else 'h264',
                    'acodec': 'aac',
                    'format_note': bitrate_info.get('gearName'),
                    'fps': int_or_none(bitrate_info.get('fps')),
                })
                break  # only take the first mirror URL per bitrate

        # Fallback: use primary playAddr on video object
        if not formats:
            for play_url in traverse_obj(video_info, ('playAddr', ..., 'src', {url_or_none})):
                formats.append({
                    'url': play_url,
                    'ext': 'mp4',
                    'width': play_width,
                    'height': play_height,
                    'vcodec': 'h264',
                    'acodec': 'aac',
                    'format_id': 'play',
                })

        # Watermarked download URL
        for dl_url in traverse_obj(video_info, (('download', None), 'url', {url_or_none})):
            formats.append({
                'url': dl_url,
                'ext': 'mp4',
                'vcodec': 'h264',
                'acodec': 'aac',
                'format_id': 'download',
                'format_note': 'watermarked',
                'preference': -2,
            })
            break

        self._remove_duplicate_formats(formats)
        return formats

    def _parse_douyin_videodetail(self, video_detail, video_id, webpage_url):
        """Parse Douyin web page videoDetail structure into yt-dlp info dict."""
        author_info = traverse_obj(video_detail, (('authorInfo', 'author', None), {
            'channel': ('nickname', {str}),
            'channel_id': (('secUid', 'authorSecId'), {str}),
            'uploader': (('uniqueId', 'shortId', 'nickname'), {str}),
            'uploader_id': (('uid', 'authorUserId', 'id'), {str_or_none}),
        }), get_all=False) or {}

        video_info = traverse_obj(video_detail, ('video', {dict})) or {}
        thumbnails = [
            {'url': thumb_url}
            for thumb_url in traverse_obj(video_info, (
                ('coverUrlList', 'cover169UrlList'), ..., {url_or_none}))
        ]
        if not thumbnails:
            for cover_url in traverse_obj(video_info, (('cover', 'thumbnail'), {url_or_none})):
                thumbnails.append({'url': cover_url})

        return {
            'id': video_id,
            'formats': self._extract_douyin_web_formats(
                {**video_info, 'download': video_detail.get('download')}),
            'http_headers': {'Referer': webpage_url},
            **author_info,
            'channel_url': format_field(author_info, 'channel_id', self._UPLOADER_URL_FORMAT, default=None),
            'uploader_url': format_field(
                author_info, ['uploader', 'uploader_id'], self._UPLOADER_URL_FORMAT, default=None),
            **traverse_obj(video_detail, ('music', {
                'track': ('title', {str}),
                'artists': ('author', {str}, {lambda x: [x] if x else None}),
                'duration': ('duration', {int_or_none}),
            })),
            **traverse_obj(video_detail, {
                'title': (('itemTitle', 'desc'), {truncate_string(left=72)}, filter),
                'description': ('desc', {str}),
                'duration': ('video', 'duration', {lambda x: int_or_none(x, scale=1000)}, filter),
                'timestamp': ('createTime', {int_or_none}),
            }),
            **traverse_obj(video_detail, ('stats', {
                'view_count': 'playCount',
                'like_count': 'diggCount',
                'repost_count': 'shareCount',
                'comment_count': 'commentCount',
                'save_count': 'collectCount',
            }), expected_type=int_or_none),
            'thumbnails': thumbnails,
        }

    def _real_extract(self, url):
        if self.enable_custom and self.get_param("use_custom_douyin"):
            mobj = self._match_valid_url(url)
            video_id = mobj.group('id') or mobj.group('modal_id')

            # First try the web API (requires valid cookies + server-side signature)
            detail = traverse_obj(self._download_json(
                'https://www.douyin.com/aweme/v1/web/aweme/detail/', video_id,
                'Downloading web detail JSON', 'Failed to download web detail JSON',
                query={'aweme_id': video_id}, fatal=False), ('aweme_detail', {dict}))
            if detail:
                return self._parse_aweme_video_app(detail)

            # Fallback: parse RENDER_DATA embedded in the video page HTML
            # Use the original URL (e.g. /jingxuan?modal_id=...) which has videoDetail in RENDER_DATA;
            # the /video/<id> page may not include it when the SPA loads data dynamically.
            # Also try /video/<id> as a backup.
            self.report_warning(
                'Web API returned empty response; falling back to webpage extraction')

            # Manually build Cookie header from cookiejar to ensure all cookies are sent
            # (yt-dlp's urllib handler may not send all cookies due to domain/path matching quirks)
            douyin_cookies = self._get_cookies('https://www.douyin.com/')
            cookie_header = '; '.join(
                f'{k}={v.value}' for k, v in douyin_cookies.items() if v.value)
            request_headers = {
                'Referer': 'https://www.douyin.com/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                **({'Cookie': cookie_header} if cookie_header else {}),
            }
            video_url = f'https://www.douyin.com/video/{video_id}'
            # Try the original URL first (modal URLs like /jingxuan?modal_id= embed videoDetail)
            urls_to_try = [url] if url != video_url else []
            # urls_to_try.append(video_url)

            for try_url in urls_to_try:
                webpage = self._download_webpage(
                    try_url, video_id, f'Downloading video webpage ({try_url})',
                    headers=request_headers)
                with open('/Users/tf/Documents/worke/lingjiang/yt-dlp-dev/xxx.js', 'r', encoding='utf-8') as f:
                    webpage = f.read()

                render_data_str = self._search_regex(
                    r'<script[^>]+id="RENDER_DATA"[^>]*>([^<]+)</script>',
                    webpage, 'render data', default=None)
                self.write_debug(
                    f'Webpage length: {len(webpage)}, RENDER_DATA found: {render_data_str is not None}')
                if render_data_str:
                    try:
                        render_data = self._parse_json(
                            urllib.parse.unquote(render_data_str), video_id)
                        video_detail = traverse_obj(render_data, ('app', 'videoDetail', {dict}))
                        self.write_debug(
                            f'render_data top keys: {list(render_data.keys()) if render_data else None}, '
                            f'videoDetail: {video_detail is not None}')
                        if video_detail:
                            return self._parse_douyin_videodetail(video_detail, video_id, try_url)
                    except Exception as e:
                        self.report_warning(f'Failed to parse RENDER_DATA: {e}')

            raise ExtractorError(
                'Unable to extract video info. '
                'Try providing Douyin cookies with --cookies-from-browser or --cookies',
                expected=True)
        else:
            return super()._real_extract(url)


