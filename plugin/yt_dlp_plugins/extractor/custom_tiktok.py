# -*- coding: utf-8 -*-

from yt_dlp.extractor.tiktok import traverse_obj, int_or_none, try_call, url_or_none, parse_qs, mimetype2ext, urllib

from yt_dlp.extractor.tiktok import TikTokIE as _TikTokIE


class TikTokIE(_TikTokIE):

    @property
    def enable_custom(self):
        return self.get_param("use_custom_plugins", True)

    def _extract_web_formats(self, aweme_detail):
        if self.enable_custom and self.get_param("use_custom_tiktok"):
            self.write_debug(
                f"[CustomYoutubeIE] handling --> use_custom_tiktok"
            )
            COMMON_FORMAT_INFO = {
                'ext': 'mp4',
                'vcodec': 'h264',
                'acodec': 'aac',
            }
            video_info = traverse_obj(aweme_detail, ('video', {dict})) or {}
            play_width = int_or_none(video_info.get('width'))
            play_height = int_or_none(video_info.get('height'))
            ratio = try_call(lambda: play_width / play_height) or 0.5625
            formats = []

            for bitrate_info in traverse_obj(video_info, ('bitrateInfo', lambda _, v: v['PlayAddr']['UrlList'])):
                format_info, res = self._parse_url_key(
                    traverse_obj(bitrate_info, ('PlayAddr', 'UrlKey', {str})) or '')
                # bytevc2 is bytedance's own custom h266/vvc codec, as-of-yet unplayable
                is_bytevc2 = format_info.get('vcodec') == 'bytevc2'
                format_info.update({
                    'format_note': 'UNPLAYABLE' if is_bytevc2 else None,
                    'preference': -100 if is_bytevc2 else -1,
                    'filesize': traverse_obj(bitrate_info, ('PlayAddr', 'DataSize', {int_or_none})),
                })

                format_info.update({
                    'width': play_width,
                    'height': play_height,
                })

                for video_url in traverse_obj(bitrate_info, ('PlayAddr', 'UrlList', ..., {url_or_none})):
                    formats.append({
                        **COMMON_FORMAT_INFO,
                        **format_info,
                        'url': self._proto_relative_url(video_url),
                    })

            # We don't have res string for play formats, but need quality for sorting & de-duplication
            play_quality = traverse_obj(formats, (lambda _, v: v['width'] == play_width, 'quality', any))

            _format_info = {
                'tbr': int_or_none(video_info['bitrate'], scale=1000) or None,
                'filesize': int(video_info['size']),
                'preference': -1,
                'format_note': None
            }
            for play_url in traverse_obj(video_info, ('playAddr', ((..., 'src'), None), {url_or_none})):
                formats.append({
                    **COMMON_FORMAT_INFO,
                    **_format_info,
                    'format_id': 'play',
                    'url': self._proto_relative_url(play_url),
                    'width': play_width,
                    'height': play_height,
                    'quality': play_quality,
                })

            for download_url in traverse_obj(video_info, (('downloadAddr', ('download', 'url')), {url_or_none})):
                formats.append({
                    **COMMON_FORMAT_INFO,
                    **_format_info,
                    'format_id': 'download',
                    'url': self._proto_relative_url(download_url),
                    'width': play_width,
                    'height': play_height,
                    'format_note': 'watermarked',
                    'preference': -2,
                })

            self._remove_duplicate_formats(formats)

            # Is it a slideshow with only audio for download?
            if not formats and traverse_obj(aweme_detail, ('music', 'playUrl', {url_or_none})):
                audio_url = aweme_detail['music']['playUrl']
                ext = traverse_obj(parse_qs(audio_url), (
                    'mime_type', -1, {lambda x: x.replace('_', '/')}, {mimetype2ext})) or 'm4a'
                formats.append({
                    'format_id': 'audio',
                    'url': self._proto_relative_url(audio_url),
                    'ext': ext,
                    'acodec': 'aac' if ext == 'm4a' else ext,
                    'vcodec': 'none',
                })

            # Filter out broken formats, see https://github.com/yt-dlp/yt-dlp/issues/11034
            return [f for f in formats if urllib.parse.urlparse(f['url']).hostname != 'www.tiktok.com']
        else:
            return super()._extract_web_formats(aweme_detail)