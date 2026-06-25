# -*- coding: utf-8 -*-

import json
import requests
from lxml import etree
from yt_dlp.extractor.instagram import InstagramIE as _InstagramIE
from yt_dlp.utils import (
    ExtractorError,
    bug_reports_message,
    decode_base_n,
    float_or_none,
    format_field,
    int_or_none,
    join_nonempty,
    lowercase_escape,
    str_to_int,
    traverse_obj,
)

_ENCODING_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'

def _id_to_pk(shortcode):
    """Convert a shortcode to a numeric value"""
    if len(shortcode) > 28:
        shortcode = shortcode[:-28]
    return decode_base_n(shortcode, table=_ENCODING_CHARS)


class InstagramIE(_InstagramIE):
    _PLUGIN_PRIORITY = 100  # 覆盖内置
    _ENABLED = True

    @property
    def enable_custom(self):
        return self.get_param("use_custom_plugins", False)

    @property
    def _is_logged_in(self):
        return True

    def _real_extract(self, url):
        if not self.enable_custom or not self.get_param("use_custom_instagram"):
            return super()._real_extract(url)
        self.write_debug(
            f"[CustomYoutubeIE] handling --> Instagram"
        )
        proxy = self.get_param("proxy")
        proxies = {"http": proxy, "https": proxy}

        video_id, url = self._match_valid_url(url).group('id', 'url')
        media, webpage = {}, ''

        api_check = self._download_json(
            f'{self._API_BASE_URL}/web/get_ruling_for_content/?content_type=MEDIA&target_id={_id_to_pk(video_id)}',
            video_id, headers=self._api_headers, fatal=False, note='Setting up session', errnote=False) or {}
        csrf_token = self._get_cookies('https://www.instagram.com').get('csrftoken')

        if not csrf_token:
            self.report_warning('No csrf token set by Instagram API', video_id)
        else:
            csrf_token = csrf_token.value if api_check.get('status') == 'ok' else None
            if not csrf_token:
                self.report_warning('Instagram API is not granting access', video_id)

        formats = []
        url = f"https://www.instagram.com/p/{video_id}/"
        payload = {}
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'zh-CN,zh;q=0.9,zh-HK;q=0.8',
            'sec-fetch-site': 'none', # 必须的
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
            'Cookie': f'csrftoken={csrf_token};'
        }
        response = requests.request("GET", url, headers=headers, data=payload, proxies=proxies)
        if not response.text:
            raise ValueError(f'request instagram {url} fail, code: {response.status_code}, text: null')
        html = etree.HTML(response.text)
        scripts = html.xpath("//script")
        for script in scripts:
            if script.text and 'video_dash_manifest' in script.text:
                break
        content = json.loads(script.text)
        xig_polaris_media = traverse_obj(
            content, ('require', 0, 3, 0, '__bbox', 'require', 5, 3, 1, '__bbox', 'result', 'data', 'xig_polaris_media'), expected_type=dict
        )
        media.update(xig_polaris_media)

        if_not_gated_logged_out = xig_polaris_media.get('if_not_gated_logged_out')
        video_dash_manifest = traverse_obj(if_not_gated_logged_out, ('video_dash_manifest'), expected_type=str)
        if video_dash_manifest:
            formats.extend(self._parse_mpd_formats(self._parse_xml(video_dash_manifest, video_id), mpd_id='dash'))

        username = if_not_gated_logged_out.get('user', {}).get('username')
        description = if_not_gated_logged_out.get('caption', {}).get('text') if if_not_gated_logged_out.get('caption', {}) else None

        comment_data = traverse_obj(media, ('comments_connection', 'edges'))
        comments = [{
            'author': traverse_obj(comment_dict, ('node', 'owner', 'username')),
            'author_id': traverse_obj(comment_dict, ('node', 'owner', 'id')),
            'id': traverse_obj(comment_dict, ('node', 'id')),
            'text': traverse_obj(comment_dict, ('node', 'text')),
            'timestamp': traverse_obj(comment_dict, ('node', 'created_at'), expected_type=int_or_none),
        } for comment_dict in comment_data] if comment_data else None

        display_resources = (
            media.get('display_resources')
            or [{'src': media.get(key)} for key in ('display_src', 'display_url')]
            or [{'src': self._og_search_thumbnail(webpage)}])
        thumbnails = [{
            'url': thumbnail['src'],
            'width': thumbnail.get('config_width'),
            'height': thumbnail.get('config_height'),
        } for thumbnail in display_resources if thumbnail.get('src')]

        return {
            'id': video_id,
            'formats': formats,
            'title': media.get('title') or f'Video by {username}',
            'description': description,
            'duration': float_or_none(media.get('video_duration')),
            'timestamp': traverse_obj(media, 'taken_at_timestamp', 'date', expected_type=int_or_none),
            'uploader_id': traverse_obj(media, ('owner', 'id')),
            'uploader': traverse_obj(media, ('owner', 'full_name')),
            'channel': username,
            'like_count': self._get_count(media, 'likes', 'preview_like') or str_to_int(self._search_regex(
                r'data-log-event="likeCountClick"[^>]*>[^\d]*([\d,\.]+)', webpage, 'like count', fatal=False)),
            'comment_count': self._get_count(media, 'comments', 'preview_comment', 'to_comment', 'to_parent_comment'),
            'comments': comments,
            'thumbnails': thumbnails,
            'http_headers': {
                'Referer': 'https://www.instagram.com/',
            },
        }
