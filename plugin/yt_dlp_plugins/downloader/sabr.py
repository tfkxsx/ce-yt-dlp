from __future__ import annotations

import concurrent.futures
import copy
import dataclasses
import itertools
import json
import os
import statistics
import time
from typing import Any

from yt_dlp.dependencies import protobug
from yt_dlp.downloader import FileDownloader
from yt_dlp.utils import DownloadError, traverse_obj


if not protobug:
    class SabrFD(FileDownloader):

        @classmethod
        def can_download(cls, info_dict):
            is_sabr = (
                info_dict.get('requested_formats')
                and all(
                    format_info.get('protocol') == 'sabr'
                    for format_info in info_dict['requested_formats']))

            if is_sabr:
                raise DownloadError('SABRFD requires protobug to be installed')

            return is_sabr


else:
    from yt_dlp.downloader.sabr._fd import SabrFD as _SabrFD
    from yt_dlp.downloader.sabr._logger import create_sabrfd_logger
    from yt_dlp.extractor.youtube._proto.innertube import NextRequestPolicy
    from yt_dlp.extractor.youtube._proto.videostreaming import (
        BufferedRange,
        FormatInitializationMetadata,
        MediaHeader,
        ReloadPlayerResponse,
        SabrContextSendingPolicy,
        SabrContextUpdate,
        SabrError,
        SabrRedirect,
        StreamProtectionStatus,
        TimeRange,
        VideoPlaybackAbrRequest,
    )
    from yt_dlp.extractor.youtube._streaming.sabr.models import (
        AudioSelector,
        CaptionSelector,
        VideoSelector,
    )
    from yt_dlp.extractor.youtube._streaming.sabr.part import (
        FormatInitializedSabrPart,
        MediaSegmentDataSabrPart,
        MediaSegmentEndSabrPart,
        PoTokenStatusSabrPart,
        RefreshPlayerResponseSabrPart,
    )
    from yt_dlp.extractor.youtube._streaming.sabr.processor import build_vpabr_request
    from yt_dlp.extractor.youtube._streaming.sabr.stream import SabrStream
    from yt_dlp.extractor.youtube._streaming.ump import UMPDecoder, UMPPartId
    from yt_dlp.networking import Request
    from yt_dlp.networking.exceptions import HTTPError, TransportError
    import math

    @dataclasses.dataclass
    class SabrSegmentSample:
        sequence_number: int
        start_ms: int
        duration_ms: int
        bytes_received: int


    @dataclasses.dataclass
    class SabrWarmupState:
        url: str
        vpabr: VideoPlaybackAbrRequest
        format_id: Any
        format_name: str
        filename: str | None
        formats: dict[str, dict[str, Any]]
        total_segments: int
        average_duration_ms: int
        estimated_origin_ms: int
        samples: list[SabrSegmentSample]
        init_segments: dict[str, bytes] = dataclasses.field(default_factory=dict)
        segment_data: dict[str, dict[str, bytes]] = dataclasses.field(default_factory=dict)


    @dataclasses.dataclass
    class SabrTask:
        target_sequence: int
        rn: int
        payload: bytes
        request_summary: dict[str, Any]

    class _SabrConcurrencyMixin:
        def real_download(self, filename, info_dict):
            return super().real_download(filename, info_dict)

        def _download_sabr_stream(
            self,
            video_id: str,
            info_dict: dict,
            video_format: dict,
            audio_format: dict,
            caption_format: dict,
            resume: bool,
            is_test: bool,
            server_abr_streaming_url: str,
            video_playback_ustreamer_config: str,
            initial_po_token: str,
            fetch_po_token_fn: callable | None = None,
            reload_config_fn: callable | None = None,
            heartbeat_callback: callable | None = None,
            client_info=None,
            start_time_ms: int = 0,
            target_duration_sec: int | None = None,
            live_status: str | None = None,
        ):
            config = self._get_sabr_concurrency_config()
            if not config['enabled']:
                return super()._download_sabr_stream(
                    video_id=video_id,
                    info_dict=info_dict,
                    video_format=video_format,
                    audio_format=audio_format,
                    caption_format=caption_format,
                    resume=resume,
                    is_test=is_test,
                    server_abr_streaming_url=server_abr_streaming_url,
                    video_playback_ustreamer_config=video_playback_ustreamer_config,
                    initial_po_token=initial_po_token,
                    fetch_po_token_fn=fetch_po_token_fn,
                    reload_config_fn=reload_config_fn,
                    heartbeat_callback=heartbeat_callback,
                    client_info=client_info,
                    start_time_ms=start_time_ms,
                    target_duration_sec=target_duration_sec,
                    live_status=live_status,
                )

            if live_status in ('is_live', 'post_live') or info_dict.get('is_live'):
                raise DownloadError('Concurrent SABR download currently supports VOD only')

            selectors, active_formats, primary_format = self._build_track_selectors(
                video_format=video_format,
                audio_format=audio_format,
                caption_format=caption_format,
            )
            if not primary_format:
                raise DownloadError('No video/audio SABR format found for concurrent SABR download')

            self.to_screen(
                f'[sabr] Starting concurrent SABR download for {primary_format["display_name"]} '
                f'({primary_format["kind"]}, threads={config["thread_number"]})',
            )
            self._sabr_download_start_time = time.time()

            warmup = self._warmup_sabr_state(
                primary_format=primary_format,
                active_formats=active_formats,
                selectors=selectors,
                config=config,
                server_abr_streaming_url=server_abr_streaming_url,
                video_playback_ustreamer_config=video_playback_ustreamer_config,
                initial_po_token=initial_po_token,
                fetch_po_token_fn=fetch_po_token_fn,
                reload_config_fn=reload_config_fn,
                heartbeat_callback=heartbeat_callback,
                client_info=client_info,
                start_time_ms=start_time_ms,
                target_duration_sec=target_duration_sec,
                live_status=live_status,
                video_id=video_id,
            )

            window_report = self._run_window_download(
                config=config,
                warmup=warmup,
            )
            self._log_window_download(window_report)

            self.to_screen('[sabr] Concurrent window download finished')
            return

        def _get_sabr_concurrency_config(self) -> dict[str, Any]:
            raw = self.params.get('sabr_concurrency') or {}
            if raw is True:
                raw = {}
            if not isinstance(raw, dict):
                raise DownloadError('sabr_concurrency must be a dict or True')

            return {
                'enabled': raw.get('enabled', True),
                'warmup_segments': raw.get('warmup_segments', 4),
                # Concurrency and request stride are independent.
                # `thread_number` controls how many window requests run at once.
                'thread_number': raw.get('thread_number', 4),
                'window_start': raw.get('window_start', 1),
                'window_count': raw.get('window_count'),
                'repair_rounds': raw.get('repair_rounds', 2),
                'state_path': raw.get('state_path'),
                'segment_dir': raw.get('segment_dir'),
                'segment_dir_explicit': raw.get('segment_dir') is not None,
                'merge_output': raw.get('merge_output'),
                # 新增：窗口大小检测相关配置
                'window_size_verify_attempts': raw.get('window_size_verify_attempts', 1),
                'dynamic_start_enabled': raw.get('dynamic_start_enabled', True),
                # 新增：动态窗口规划器配置
                'window_planning_mode': raw.get('window_planning_mode', 'interleaved'),  # 'sequential', 'interleaved', 'adaptive'
                'interleaving_enabled': raw.get('interleaving_enabled', True),
                'adaptive_threshold_success': raw.get('adaptive_threshold_success', 0.8),  # 成功率阈值
                'adaptive_threshold_response_ms': raw.get('adaptive_threshold_response_ms', 5000),  # 响应时间阈值
                'priority_scheduling_enabled': raw.get('priority_scheduling_enabled', True),
                # 新增：并发合并配置
                'merge_concurrency': raw.get('merge_concurrency'),  # 合并并发数，None表示自动计算
                'enable_concurrent_merge': raw.get('enable_concurrent_merge', True),  # 是否启用并发合并
                'intermediate_file_prefix': raw.get('intermediate_file_prefix', '.sabr_merge_'),  # 中间文件前缀
            }

        def _build_track_selectors(self, video_format, audio_format, caption_format):
            primary_format = None
            audio_selector = None
            video_selector = None
            caption_selector = None
            active_formats = {}

            if audio_format:
                audio_selector = AudioSelector(
                    display_name=audio_format['display_name'],
                    format_ids=[audio_format['format_id']],
                )
                active_formats[self._format_id_key(audio_format['format_id'])] = {
                    'display_name': audio_format['display_name'],
                    'format_id': audio_format['format_id'],
                    'kind': 'audio',
                    'filename': audio_format.get('filename'),
                    'info_dict': audio_format.get('info_dict'),
                }
                primary_format = primary_format or active_formats[self._format_id_key(audio_format['format_id'])]

            if video_format:
                video_selector = VideoSelector(
                    display_name=video_format['display_name'],
                    format_ids=[video_format['format_id']],
                )
                active_formats[self._format_id_key(video_format['format_id'])] = {
                    'display_name': video_format['display_name'],
                    'format_id': video_format['format_id'],
                    'kind': 'video',
                    'filename': video_format.get('filename'),
                    'info_dict': video_format.get('info_dict'),
                }
                primary_format = active_formats[self._format_id_key(video_format['format_id'])]

            if caption_format:
                caption_selector = CaptionSelector(
                    display_name=caption_format['display_name'],
                    format_ids=[caption_format['format_id']],
                )

            return {
                'audio': audio_selector,
                'video': video_selector,
                'caption': caption_selector,
            }, active_formats, primary_format

        def _create_sabr_stream(
            self,
            selectors: dict[str, Any],
            server_abr_streaming_url: str,
            video_playback_ustreamer_config: str,
            initial_po_token: str,
            heartbeat_callback,
            client_info,
            start_time_ms: int,
            target_duration_sec: int | None,
            live_status: str | None,
            video_id: str,
        ) -> SabrStream:
            return SabrStream(
                urlopen=self.ydl.urlopen,
                logger=create_sabrfd_logger(self.ydl, prefix='sabr:concurrent'),
                server_abr_streaming_url=server_abr_streaming_url,
                video_playback_ustreamer_config=video_playback_ustreamer_config,
                po_token=initial_po_token,
                video_selection=selectors['video'],
                audio_selection=selectors['audio'],
                caption_selection=selectors['caption'],
                start_time_ms=start_time_ms,
                client_info=client_info,
                live_segment_target_duration_sec=target_duration_sec,
                post_live=live_status == 'post_live',
                video_id=video_id,
                retry_sleep_func=self.params.get('retry_sleep_functions', {}).get('http'),
                heartbeat_callback=heartbeat_callback,
            )

        def _handle_stream_control_part(self, stream, part, fetch_po_token_fn, reload_config_fn):
            if isinstance(part, PoTokenStatusSabrPart):
                if not fetch_po_token_fn:
                    return
                if part.status in (part.PoTokenStatus.INVALID, part.PoTokenStatus.PENDING):
                    po_token = fetch_po_token_fn(bypass_cache=True)
                elif part.status in (part.PoTokenStatus.MISSING, part.PoTokenStatus.PENDING_MISSING):
                    po_token = fetch_po_token_fn()
                else:
                    po_token = None
                if po_token:
                    stream.processor.po_token = po_token
                return

            if isinstance(part, RefreshPlayerResponseSabrPart) and reload_config_fn:
                stream.url, stream.processor.video_playback_ustreamer_config = reload_config_fn(part.reload_playback_token)

        def _warmup_sabr_state(
            self,
            primary_format: dict,
            active_formats: dict[str, dict[str, Any]],
            selectors: dict[str, Any],
            config: dict[str, Any],
            server_abr_streaming_url: str,
            video_playback_ustreamer_config: str,
            initial_po_token: str,
            fetch_po_token_fn,
            reload_config_fn,
            heartbeat_callback,
            client_info,
            start_time_ms: int,
            target_duration_sec: int | None,
            live_status: str | None,
            video_id: str,
        ) -> SabrWarmupState:
            stream = self._create_sabr_stream(
                selectors=selectors,
                server_abr_streaming_url=server_abr_streaming_url,
                video_playback_ustreamer_config=video_playback_ustreamer_config,
                initial_po_token=initial_po_token,
                heartbeat_callback=heartbeat_callback,
                client_info=client_info,
                start_time_ms=start_time_ms,
                target_duration_sec=target_duration_sec,
                live_status=live_status,
                video_id=video_id,
            )
            samples = []
            seq_bytes = {}
            total_segments = 0
            init_segments = {}
            segment_data = {}
            primary_format_key = self._format_id_key(primary_format['format_id'])

            try:
                for part in stream:
                    self._handle_stream_control_part(stream, part, fetch_po_token_fn, reload_config_fn)

                    if isinstance(part, FormatInitializedSabrPart):
                        if self._format_id_key(part.format_id) == primary_format_key:
                            total_segments = (
                                stream.processor.initialized_formats[str(part.format_id)].last_segment_number
                                or total_segments
                            )
                        continue

                    if isinstance(part, MediaSegmentDataSabrPart):
                        format_key = self._format_id_key(part.format_id)
                        if format_key not in active_formats:
                            continue
                        if part.is_init_segment:
                            init_segments[format_key] = init_segments.get(format_key, b'') + part.data
                        else:
                            sequence_key = str(part.sequence_number)
                            bucket = segment_data.setdefault(format_key, {})
                            bucket[sequence_key] = bucket.get(sequence_key, b'') + part.data
                            if format_key != primary_format_key:
                                continue
                            seq_bytes[part.sequence_number] = seq_bytes.get(part.sequence_number, 0) + len(part.data)
                        continue

                    if isinstance(part, MediaSegmentEndSabrPart):
                        if part.is_init_segment or self._format_id_key(part.format_id) != primary_format_key:
                            continue
                        initialized_format = stream.processor.initialized_formats[str(part.format_id)]
                        segment = initialized_format.previous_segment
                        samples.append(SabrSegmentSample(
                            sequence_number=part.sequence_number,
                            start_ms=segment.start_ms,
                            duration_ms=segment.duration_ms,
                            bytes_received=seq_bytes.get(part.sequence_number, 0),
                        ))
                        total_segments = total_segments or part.total_segments or initialized_format.last_segment_number or 0
                        if len(samples) >= config['warmup_segments']:
                            stream.close()
                            break
            finally:
                stream.close()

            if len(samples) < config['warmup_segments']:
                raise DownloadError(
                    f'SABR warmup collected {len(samples)} segments, expected '
                    f'{config["warmup_segments"]}')

            durations = [sample.duration_ms for sample in samples if sample.duration_ms]
            if not durations:
                raise DownloadError('SABR warmup could not estimate segment duration')

            average_duration_ms = int(round(statistics.mean(durations)))
            estimated_origin_ms = samples[0].start_ms - ((samples[0].sequence_number - 1) * average_duration_ms)

            return SabrWarmupState(
                url=stream.url,
                vpabr=build_vpabr_request(stream.processor),
                format_id=primary_format['format_id'],
                format_name=primary_format['display_name'],
                filename=primary_format.get('filename'),
                formats=active_formats,
                total_segments=total_segments,
                average_duration_ms=average_duration_ms,
                estimated_origin_ms=estimated_origin_ms,
                samples=samples,
                init_segments=init_segments,
                segment_data=segment_data,
            )

        def _estimate_player_time_ms(self, warmup, target_sequence):
            return warmup.estimated_origin_ms + max(0, target_sequence - 1) * warmup.average_duration_ms

        def _make_target_buffered_range(self, warmup, target_sequence):
            end_sequence = target_sequence - 1
            if end_sequence < 1:
                return None

            start_time_ms = warmup.estimated_origin_ms
            duration_ms = max(0, self._estimate_player_time_ms(warmup, target_sequence) - start_time_ms)
            return BufferedRange(
                format_id=warmup.format_id,
                start_segment_index=1,
                end_segment_index=end_sequence,
                start_time_ms=start_time_ms,
                duration_ms=duration_ms,
                time_range=TimeRange(
                    start_ticks=start_time_ms,
                    duration_ticks=duration_ms,
                    timescale=1000,
                ),
            )

        def _mutate_vpabr_for_profile(self, warmup, target_sequence, profile):
            vpabr = copy.deepcopy(warmup.vpabr)

            if profile in ('vary_player_time', 'vary_range_and_time'):
                vpabr.client_abr_state.player_time_ms = self._estimate_player_time_ms(warmup, target_sequence)

            if profile in ('vary_buffered_ranges', 'vary_range_and_time'):
                non_target_ranges = [
                    buffered_range
                    for buffered_range in vpabr.buffered_ranges
                    if self._format_id_key(buffered_range.format_id) != self._format_id_key(warmup.format_id)
                ]
                target_range = self._make_target_buffered_range(warmup, target_sequence)
                vpabr.buffered_ranges = non_target_ranges + ([target_range] if target_range else [])

            return vpabr

        def _build_window_request_summary(self, vpabr, warmup, target_sequence):
            streamer_context = getattr(vpabr, 'streamer_context', None)
            client_abr_state = getattr(vpabr, 'client_abr_state', None)
            return {
                'target_sequence': target_sequence,
                'player_time_ms': getattr(client_abr_state, 'player_time_ms', None),
                'playback_cookie_present': bool(getattr(streamer_context, 'playback_cookie', None)),
                'sabr_context_count': len(getattr(streamer_context, 'sabr_contexts', []) or []),
                'buffered_ranges': [
                    {
                        'start_segment_index': buffered_range.start_segment_index,
                        'end_segment_index': buffered_range.end_segment_index,
                        'start_time_ms': buffered_range.start_time_ms,
                        'duration_ms': buffered_range.duration_ms,
                        'target_format': self._format_id_key(buffered_range.format_id) == self._format_id_key(warmup.format_id),
                    }
                    for buffered_range in vpabr.buffered_ranges
                ],
            }

        def _build_window_task(self, warmup, target_sequence, rn):
            vpabr = self._mutate_vpabr_for_profile(warmup, target_sequence, 'vary_range_and_time')
            return SabrTask(
                target_sequence=target_sequence,
                rn=rn,
                payload=protobug.dumps(vpabr),
                request_summary=self._build_window_request_summary(vpabr, warmup, target_sequence),
            )

        def _build_window_tasks(self, warmup, targets, start_rn=1):
            tasks = []
            rn_counter = itertools.count(start_rn)
            for target_sequence in targets:
                tasks.append(self._build_window_task(warmup, target_sequence, rn=next(rn_counter)))
            return tasks

        def _decode_concurrency_response(self, response, warmup):
            summary = {
                'status': response.status,
                'segments': [],
                'format_initializations': [],
                'stream_protection_status': None,
                'sabr_errors': [],
                'redirect_urls': [],
                'reload_playback_token_present': False,
                'playback_cookie_present': False,
                'target_segment_bytes': {},
                'other_segment_bytes': {},
                'seek_events': 0,
            }

            decoder = UMPDecoder(response.fp)
            target_format_key = self._format_id_key(warmup.format_id)
            headers_by_id = {}

            for part in decoder.iter_parts():
                if part.part_id == UMPPartId.MEDIA_HEADER:
                    media_header = protobug.load(part.data, MediaHeader)
                    headers_by_id[media_header.header_id] = media_header
                    summary['segments'].append({
                        'sequence_number': media_header.sequence_number,
                        'is_init_segment': bool(media_header.is_init_segment),
                        'target_format': self._format_id_key(media_header.format_id) == target_format_key,
                    })
                elif part.part_id == UMPPartId.MEDIA:
                    header_id = self._read_varint(part.data)
                    media_header = headers_by_id.get(header_id)
                    if not media_header or media_header.is_init_segment or media_header.sequence_number is None:
                        continue
                    content_length = part.size - part.data.tell()
                    key = str(media_header.sequence_number)
                    target_bucket = (
                        summary['target_segment_bytes']
                        if self._format_id_key(media_header.format_id) == target_format_key
                        else summary['other_segment_bytes']
                    )
                    target_bucket[key] = target_bucket.get(key, 0) + content_length
                elif part.part_id == UMPPartId.FORMAT_INITIALIZATION_METADATA:
                    metadata = protobug.load(part.data, FormatInitializationMetadata)
                    summary['format_initializations'].append({
                        'total_segments': metadata.total_segments,
                        'mime_type': metadata.mime_type,
                        'target_format': self._format_id_key(metadata.format_id) == target_format_key,
                    })
                elif part.part_id == UMPPartId.STREAM_PROTECTION_STATUS:
                    status = protobug.load(part.data, StreamProtectionStatus)
                    summary['stream_protection_status'] = StreamProtectionStatus.Status(status.status).name
                elif part.part_id == UMPPartId.SABR_ERROR:
                    sabr_error = protobug.load(part.data, SabrError)
                    summary['sabr_errors'].append(sabr_error.type)
                elif part.part_id == UMPPartId.SABR_REDIRECT:
                    redirect = protobug.load(part.data, SabrRedirect)
                    summary['redirect_urls'].append(redirect.redirect_url)
                elif part.part_id == UMPPartId.RELOAD_PLAYER_RESPONSE:
                    reload_response = protobug.load(part.data, ReloadPlayerResponse)
                    summary['reload_playback_token_present'] = bool(
                        traverse_obj(reload_response, ('reload_playback_params', 'token')))
                elif part.part_id == UMPPartId.NEXT_REQUEST_POLICY:
                    policy = protobug.load(part.data, NextRequestPolicy)
                    summary['playback_cookie_present'] = bool(policy.playback_cookie)
                elif part.part_id == UMPPartId.SABR_CONTEXT_UPDATE:
                    protobug.load(part.data, SabrContextUpdate)
                elif part.part_id == UMPPartId.SABR_CONTEXT_SENDING_POLICY:
                    protobug.load(part.data, SabrContextSendingPolicy)
                elif part.part_id == UMPPartId.SABR_SEEK:
                    summary['seek_events'] += 1

            summary['target_sequences'] = [
                segment['sequence_number']
                for segment in summary['segments']
                if segment['target_format'] and not segment['is_init_segment'] and segment['sequence_number'] is not None
            ]
            summary['window_size'] = len(summary['target_sequences'])
            summary['target_total_bytes'] = sum(summary['target_segment_bytes'].values())
            summary['first_target_sequence'] = (
                summary['target_sequences'][0] if summary['target_sequences'] else None
            )
            return summary

        def _execute_concurrency_task(self, task, warmup):
            response = None
            try:
                response = self.ydl.urlopen(Request(
                    url=warmup.url,
                    method='POST',
                    data=task.payload,
                    query={'rn': task.rn},
                    headers={
                        'content-type': 'application/x-protobuf',
                        'accept-encoding': 'identity',
                        'accept': 'application/vnd.yt-ump',
                    },
                ))
                summary = self._decode_concurrency_response(response, warmup)
                summary.update({
                    'request_rn': task.rn,
                    'request': task.request_summary,
                    'matched_target': task.target_sequence in summary['target_sequences'],
                })
                return summary
            except (HTTPError, TransportError) as err:
                return {
                    'request_rn': task.rn,
                    'request': task.request_summary,
                    'error': f'{err.__class__.__name__}: {err}',
                    'matched_target': False,
                    'target_sequences': [],
                    'first_target_sequence': None,
                }
            finally:
                if response and not response.closed:
                    response.close()

        def _run_window_download(self, config, warmup):
            total_segments = warmup.total_segments or 0
            # Warmup already covered segments 1..N, so the first concurrent window must start at N+1.
            # Starting at the next stride boundary can skip uncovered segments (e.g. 5,6,7).
            safe_window_start = warmup.samples[-1].sequence_number + 1
            # 动态计算起始窗口
            if config.get('dynamic_start_enabled', True):
                sample_target = self._calculate_dynamic_start(
                    safe_window_start=safe_window_start,
                    window_start_config=config['window_start'],
                    window_size=0,  # 暂时未知，稍后检测
                    concurrency=config['thread_number'],
                    total_segments=total_segments
                )
            else:
                sample_target = max(config['window_start'], safe_window_start)
            
            if total_segments and sample_target > total_segments:
                raise DownloadError('window_start is beyond total_segments')

            base_filename = warmup.filename or warmup.format_name or 'sabr'
            segment_dir = config['segment_dir'] or os.path.dirname(base_filename) or '.'
            state_path = config['state_path'] or f'{base_filename}.sabr_state.json'
            if config['segment_dir_explicit']:
                os.makedirs(segment_dir, exist_ok=True)
            state_dir = os.path.dirname(state_path)
            if state_dir:
                os.makedirs(state_dir, exist_ok=True)

            state = self._load_window_state(
                state_path=state_path,
                segment_dir=segment_dir,
                warmup=warmup,
                stride=0,
                segment_dir_explicit=config['segment_dir_explicit'],
            )
            self._persist_init_segment(
                state=state,
                state_path=state_path,
                segment_dir=segment_dir,
                warmup=warmup,
            )
            self._persist_warmup_segments(
                state=state,
                state_path=state_path,
                segment_dir=segment_dir,
                warmup=warmup,
            )

            sample_task = self._build_window_task(warmup, sample_target, rn=1)
            sample_summary, sample_segment_data = self._execute_window_task(sample_task, warmup)
            stride = self._detect_window_stride(sample_summary, sample_target)
            if stride <= 0:
                raise DownloadError('Unable to detect SABR window stride from sample request')
            state['stride'] = stride
            self._save_window_state(state_path, state)

            sample_duplicates, sample_written = self._persist_window_result(
                state=state,
                state_path=state_path,
                segment_dir=segment_dir,
                task=sample_task,
                summary=sample_summary,
                segment_data=sample_segment_data,
                warmup=warmup,
            )

            # 动态计算窗口大小和起始位置
            window_size = stride  # 检测到的窗口大小（服务器返回的连续片段数）
            window_stride = 1  # 窗口之间的步长应该是1，确保完全覆盖
            
            # 计算需要多少个窗口来覆盖所有片段
            if config['window_count'] is not None:
                window_count = max(0, config['window_count'])
            elif total_segments:
                # 使用window_stride=1确保完全覆盖
                window_count = ((total_segments - sample_target) // window_stride) + 1
            else:
                raise DownloadError('window_count is required when total_segments is unavailable')

            # 使用动态窗口规划器生成窗口序列
            targets = self._generate_window_plan(
                config=config,
                sample_target=sample_target,
                window_count=window_count,
                window_stride=window_stride,
                concurrency=config['thread_number']
            )
            
            # 确保targets不超过总片段数
            if total_segments:
                targets = [t for t in targets if t <= total_segments]
            
            if not targets:
                raise DownloadError('window_download produced no target windows')

            pending_targets = [
                target for target in targets
                if state['windows'].get(str(target), {}).get('status') != 'completed'
            ]

            self.to_screen(
                f'[sabr-window] start={sample_target} stride={stride} '
                f'threads={config["thread_number"]} targets={len(targets)} pending={len(pending_targets)}',
            )

            results = [sample_summary]
            duplicates = sample_duplicates
            written_sequences = sample_written

            if pending_targets:
                run_results, run_duplicates, run_written = self._download_window_targets(
                    config=config,
                    warmup=warmup,
                    state=state,
                    state_path=state_path,
                    segment_dir=segment_dir,
                    targets=pending_targets,
                )
                duplicates += run_duplicates
                written_sequences += run_written
                results.extend(run_results)

            repaired_targets = []
            for repair_round in range(config['repair_rounds']):
                missing_by_track = self._state_missing_sequences(state)
                # 使用window_size（即stride）作为修复参数
                repair_targets = self._repair_targets_for_missing(state, missing_by_track, sample_target, stride)
                if not repair_targets:
                    break

                repaired_targets.extend(repair_targets)
                self.to_screen(
                    f'[sabr-window] repair round {repair_round + 1}: '
                    f'missing_windows={repair_targets}',
                )
                run_results, run_duplicates, run_written = self._download_window_targets(
                    config=config,
                    warmup=warmup,
                    state=state,
                    state_path=state_path,
                    segment_dir=segment_dir,
                    targets=repair_targets,
                )
                duplicates += run_duplicates
                written_sequences += run_written
                results.extend(run_results)

            merge_output = self._merge_window_track(
                config=config,
                state=state,
                segment_dir=segment_dir,
                warmup=warmup,
            )
            self._cleanup_concurrency_temp_files(
                state_path=state_path,
                segment_dir=segment_dir,
                merge_output=merge_output,
            )

            return {
                'segment_dir': segment_dir,
                'state_path': state_path,
                'stride': stride,
                'window_targets': targets,
                'pending_targets_before_run': pending_targets,
                'repair_targets': repaired_targets,
                'completed_windows': sorted(
                    int(target) for target, item in state['windows'].items()
                    if item.get('status') == 'completed'
                ),
                'written_sequences': written_sequences,
                'duplicate_sequences': duplicates,
                'merge_output': merge_output,
                'results': results,
            }

        def _calculate_dynamic_start(self, safe_window_start, window_start_config, window_size, concurrency, total_segments):
            """
            动态计算起始窗口
            考虑warmup结果、并发数、窗口对齐和性能优化
            """
            # 1. 基础起始位置：warmup后的下一个片段
            base_start = safe_window_start
            
            # 2. 如果配置了window_start，使用较大值
            config_start = window_start_config
            start_candidate = max(base_start, config_start)
            
            # 3. 如果已知窗口大小，对齐到窗口边界（提高效率）
            if window_size > 0:
                # 对齐到最近的窗口边界
                aligned_start = ((start_candidate - 1) // window_size) * window_size + 1
            else:
                aligned_start = start_candidate
            
            # 4. 考虑并发优化：如果并发数>1且窗口大小>1，可以交错起始位置
            if concurrency > 1 and window_size > 1:
                # 简单策略：主线程使用aligned_start，其他线程会有偏移
                # 这里返回主起始位置，实际交错在窗口规划中处理
                pass
            
            # 5. 确保不超过总片段数
            if total_segments and aligned_start > total_segments:
                # 如果超过，从末尾倒退一个窗口
                aligned_start = max(1, total_segments - window_size + 1) if window_size > 0 else max(1, total_segments)
            
            # 6. 记录决策信息（用于调试）
            self.to_screen(
                f'[sabr-dynamic] dynamic_start: safe={safe_window_start}, '
                f'config={window_start_config}, size={window_size}, '
                f'concurrency={concurrency}, final={aligned_start}'
            )
            
            return aligned_start

        def _detect_window_stride(self, sample_summary, target_sequence):
            """
            检测窗口大小（服务器返回的连续片段数）
            使用增强算法提高可靠性
            """
            sequences = sample_summary.get('target_sequences') or []
            if not sequences:
                return 0
            
            # 方法1：从target_sequence开始的连续序列长度（保持向后兼容）
            expected = target_sequence
            found_target = False
            count = 0
            for sequence in sequences:
                if not found_target:
                    if sequence != target_sequence:
                        continue
                    found_target = True
                if sequence != expected:
                    break
                count += 1
                expected += 1
            
            # 如果找到了有效的连续序列，返回它
            if count > 0:
                return count
            
            # 方法2：如果没有从target_sequence开始的连续序列，尝试找出最大连续段
            # 这可能是target_sequence不在返回序列中的情况
            return self._detect_max_continuous_segment(sequences)
        
        def _detect_max_continuous_segment(self, sequences):
            """
            检测序列中的最大连续段长度
            """
            if not sequences:
                return 0
            
            sorted_seq = sorted(sequences)
            max_length = 1
            current_length = 1
            
            for i in range(1, len(sorted_seq)):
                if sorted_seq[i] == sorted_seq[i-1] + 1:
                    current_length += 1
                    max_length = max(max_length, current_length)
                else:
                    current_length = 1
            
            return max_length
        
        def _detect_window_size_robust(self, warmup, config, sample_target):
            """
            增强的窗口大小检测，支持多次验证
            """
            # 首先使用单个样本检测
            sample_task = self._build_window_task(warmup, sample_target, rn=1)
            sample_summary, _ = self._execute_window_task(sample_task, warmup)
            initial_size = self._detect_window_stride(sample_summary, sample_target)
            
            if initial_size <= 0:
                return 0
            
            # 如果配置了验证次数，执行额外验证
            verify_attempts = config.get('window_size_verify_attempts', 1)
            if verify_attempts <= 1:
                return initial_size
            
            detected_sizes = [initial_size]
            
            # 执行额外验证请求
            for attempt in range(1, verify_attempts):
                # 选择不同的目标序列进行验证
                verify_target = sample_target + (attempt * initial_size)
                verify_task = self._build_window_task(warmup, verify_target, rn=1000 + attempt)
                verify_summary, _ = self._execute_window_task(verify_task, warmup)
                verify_size = self._detect_window_stride(verify_summary, verify_target)
                
                if verify_size > 0:
                    detected_sizes.append(verify_size)
                
                # 如果连续两次检测结果一致，提前返回
                if len(detected_sizes) >= 2 and len(set(detected_sizes[-2:])) == 1:
                    break
            
            # 返回最常见的大小
            from collections import Counter
            if detected_sizes:
                most_common = Counter(detected_sizes).most_common(1)[0][0]
                return most_common
            
            return initial_size
        
        def _generate_interleaved_targets(self, base_target, window_count, window_stride, concurrency):
            """
            生成交错窗口序列以提高并发效率
            
            参数:
                base_target: 基础起始序列号
                window_count: 总窗口数
                window_stride: 窗口步长（通常为1）
                concurrency: 并发数
            
            返回:
                交错后的窗口序列列表
            """
            if concurrency <= 1 or window_count <= 1:
                # 无并发或窗口数少，返回线性序列
                return [base_target + (i * window_stride) for i in range(window_count)]
            
            # 交错算法：将窗口分成concurrency组，每组交错排列
            # 例如：base_target=5, window_count=8, concurrency=4
            # 线性序列：[5, 6, 7, 8, 9, 10, 11, 12]
            # 交错序列：[5, 9, 6, 10, 7, 11, 8, 12]
            
            targets = []
            for group in range(concurrency):
                for i in range(group, window_count, concurrency):
                    target = base_target + (i * window_stride)
                    targets.append(target)
            
            return targets
        
        def _generate_window_plan(self, config, sample_target, window_count, window_stride, concurrency):
            """
            根据配置生成窗口规划
            
            参数:
                config: 配置字典
                sample_target: 样本起始序列号
                window_count: 总窗口数
                window_stride: 窗口步长
                concurrency: 并发数
            
            返回:
                规划后的窗口序列列表
            """
            planning_mode = config.get('window_planning_mode', 'interleaved')
            interleaving_enabled = config.get('interleaving_enabled', True)
            
            if planning_mode == 'sequential' or not interleaving_enabled:
                # 线性序列模式
                targets = [sample_target + (i * window_stride) for i in range(window_count)]
            elif planning_mode == 'interleaved':
                # 交错模式
                targets = self._generate_interleaved_targets(
                    sample_target, window_count, window_stride, concurrency
                )
            elif planning_mode == 'adaptive':
                # 自适应模式（暂时使用交错模式）
                # TODO: 实现自适应逻辑
                targets = self._generate_interleaved_targets(
                    sample_target, window_count, window_stride, concurrency
                )
            else:
                # 默认使用线性序列
                targets = [sample_target + (i * window_stride) for i in range(window_count)]
            
            # 记录规划信息（用于调试）
            self.to_screen(
                f'[sabr-planning] mode={planning_mode}, '
                f'interleaving={interleaving_enabled}, '
                f'concurrency={concurrency}, '
                f'targets={len(targets)}'
            )
            
            return targets

        def _download_window_targets(self, config, warmup, state, state_path, segment_dir, targets):
            tasks = self._build_window_tasks(warmup, targets, start_rn=2)
            task_map = {task.target_sequence: task for task in tasks}

            results = []
            duplicates = 0
            written_sequences = 0

            if targets:
                with concurrent.futures.ThreadPoolExecutor(max_workers=config['thread_number']) as pool:
                    future_map = {
                        pool.submit(self._execute_window_task, task_map[target], warmup): task_map[target]
                        for target in targets
                    }
                    for future in concurrent.futures.as_completed(future_map):
                        task = future_map[future]
                        summary, segment_data = future.result()
                        duplicate_count, written_count = self._persist_window_result(
                            state=state,
                            state_path=state_path,
                            segment_dir=segment_dir,
                            task=task,
                            summary=summary,
                            segment_data=segment_data,
                            warmup=warmup,
                        )
                        duplicates += duplicate_count
                        written_sequences += written_count
                        results.append(summary)

            return results, duplicates, written_sequences

        def _state_missing_sequences(self, state):
            missing = {}
            for label, track_state in (state.get('tracks') or {}).items():
                sequence_numbers = sorted(
                    int(sequence_key)
                    for sequence_key in track_state
                    if sequence_key != 'init'
                )
                track_missing = self._missing_sequences(sequence_numbers)
                if track_missing:
                    missing[label] = track_missing
            return missing

        def _repair_targets_for_missing(self, state, missing_by_track, window_start, window_size):
            if not missing_by_track:
                return []

            # 收集所有缺失序列
            all_missing = set()
            for missing_sequences in missing_by_track.values():
                all_missing.update(missing_sequences)
            
            if not all_missing:
                return []
            
            # 方法1：基于窗口大小的修复（原始方法）
            targets_by_window = set()
            for sequence in all_missing:
                if sequence < window_start:
                    targets_by_window.add(sequence)
                else:
                    offset = sequence - window_start
                    targets_by_window.add(window_start + ((offset // window_size) * window_size))
            
            # 方法2：直接修复缺失序列的前后窗口
            # 对于每个缺失序列，也尝试它的前一个窗口（如果可能）
            direct_targets = set()
            for sequence in all_missing:
                # 尝试前一个窗口（如果>0）
                if sequence > window_start:
                    prev_target = window_start + (((sequence - window_start - 1) // window_size) * window_size)
                    direct_targets.add(prev_target)
                # 尝试当前窗口
                if sequence >= window_start:
                    curr_target = window_start + (((sequence - window_start) // window_size) * window_size)
                    direct_targets.add(curr_target)
                # 尝试后一个窗口
                if sequence >= window_start:
                    next_target = window_start + (((sequence - window_start + 1) // window_size) * window_size)
                    direct_targets.add(next_target)
            
            # 合并两种方法的修复目标
            all_targets = sorted(targets_by_window.union(direct_targets))
            
            # 记录修复决策信息
            self.to_screen(
                f'[sabr-repair] missing_sequences={sorted(all_missing)[:10]}{"..." if len(all_missing) > 10 else ""}, '
                f'window_size={window_size}, repair_targets={all_targets[:10]}{"..." if len(all_targets) > 10 else ""}'
            )
            
            return all_targets

        def _load_window_state(self, state_path, segment_dir, warmup, stride, segment_dir_explicit):
            if os.path.isfile(state_path):
                with open(state_path, 'r', encoding='utf-8') as f:
                    state = json.load(f)
            else:
                state = {
                    'version': 1,
                    'selected_format': warmup.format_name,
                    'format_key': self._format_id_key(warmup.format_id),
                    'formats': {
                        format_key: {
                            'display_name': fmt.get('display_name'),
                            'kind': fmt.get('kind'),
                            'filename': fmt.get('filename'),
                        }
                        for format_key, fmt in warmup.formats.items()
                    },
                    'stride': stride,
                    'segment_dir': segment_dir,
                    'segment_dir_explicit': segment_dir_explicit,
                    'windows': {},
                    'tracks': {},
                }

            state.setdefault('windows', {})
            state.setdefault('tracks', {})
            state.setdefault('formats', {
                format_key: {
                    'display_name': fmt.get('display_name'),
                    'kind': fmt.get('kind'),
                    'filename': fmt.get('filename'),
                }
                for format_key, fmt in warmup.formats.items()
            })
            state['segment_dir'] = segment_dir
            state['segment_dir_explicit'] = segment_dir_explicit
            state['stride'] = stride
            self._save_window_state(state_path, state)
            return state

        def _save_window_state(self, state_path, state):
            temp_path = f'{state_path}.tmp'
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
            os.replace(temp_path, state_path)

        def _persist_init_segment(self, state, state_path, segment_dir, warmup):
            changed = False
            for format_key, init_data in warmup.init_segments.items():
                if not init_data:
                    continue
                label = self._format_label(format_key, state)
                track_state = state['tracks'].setdefault(label, {})
                existing = track_state.get('init')
                filename = self._segment_part_path(format_key, state, 'init')
                size = len(init_data)
                if existing and existing.get('size') == size and os.path.isfile(existing.get('path', filename)):
                    continue

                with open(filename, 'wb') as f:
                    f.write(init_data)

                track_state['init'] = {
                    'path': filename,
                    'size': size,
                }
                changed = True

            if changed:
                self._save_window_state(state_path, state)

        def _persist_warmup_segments(self, state, state_path, segment_dir, warmup):
            changed = False
            for format_key, sequences in warmup.segment_data.items():
                label = self._format_label(format_key, state)
                track_state = state['tracks'].setdefault(label, {})
                for sequence_key, data in sequences.items():
                    filename = self._segment_part_path(format_key, state, sequence_key)
                    size = len(data)
                    existing = track_state.get(sequence_key)
                    if existing and existing.get('size') == size and os.path.isfile(existing.get('path', filename)):
                        continue
                    with open(filename, 'wb') as f:
                        f.write(data)
                    track_state[sequence_key] = {'path': filename, 'size': size}
                    changed = True
                    self._report_track_progress(format_key, state, warmup)
            if changed:
                self._save_window_state(state_path, state)

        def _execute_window_task(self, task, warmup):
            response = None
            try:
                response = self.ydl.urlopen(Request(
                    url=warmup.url,
                    method='POST',
                    data=task.payload,
                    query={'rn': task.rn},
                    headers={
                        'content-type': 'application/x-protobuf',
                        'accept-encoding': 'identity',
                        'accept': 'application/vnd.yt-ump',
                    },
                ))
                summary, segment_data = self._decode_window_response(response, warmup)
                summary.update({
                    'request_rn': task.rn,
                    'request': task.request_summary,
                    'matched_target': task.target_sequence in summary['target_sequences'],
                })
                return summary, segment_data
            except (HTTPError, TransportError) as err:
                return {
                    'request_rn': task.rn,
                    'request': task.request_summary,
                    'error': f'{err.__class__.__name__}: {err}',
                    'matched_target': False,
                    'target_sequences': [],
                    'first_target_sequence': None,
                }, {}
            finally:
                if response and not response.closed:
                    response.close()

        def _decode_window_response(self, response, warmup):
            summary = {
                'status': response.status,
                'segments': [],
                'format_initializations': [],
                'stream_protection_status': None,
                'sabr_errors': [],
                'redirect_urls': [],
                'reload_playback_token_present': False,
                'playback_cookie_present': False,
                'target_segment_bytes': {},
                'other_segment_bytes': {},
                'seek_events': 0,
            }

            decoder = UMPDecoder(response.fp)
            target_format_key = self._format_id_key(warmup.format_id)
            headers_by_id = {}
            segment_data = {}

            for part in decoder.iter_parts():
                if part.part_id == UMPPartId.MEDIA_HEADER:
                    media_header = protobug.load(part.data, MediaHeader)
                    headers_by_id[media_header.header_id] = media_header
                    summary['segments'].append({
                        'sequence_number': media_header.sequence_number,
                        'is_init_segment': bool(media_header.is_init_segment),
                        'target_format': self._format_id_key(media_header.format_id) == target_format_key,
                    })
                elif part.part_id == UMPPartId.MEDIA:
                    header_id = self._read_varint(part.data)
                    media_header = headers_by_id.get(header_id)
                    if not media_header or media_header.is_init_segment or media_header.sequence_number is None:
                        continue
                    format_key = self._format_id_key(media_header.format_id)
                    content_length = part.size - part.data.tell()
                    key = str(media_header.sequence_number)
                    target_bucket = (
                        summary['target_segment_bytes']
                        if format_key == target_format_key
                        else summary['other_segment_bytes']
                    )
                    target_bucket[key] = target_bucket.get(key, 0) + content_length
                    bucket = segment_data.setdefault(format_key, {})
                    bucket[key] = bucket.get(key, b'') + part.data.read()
                elif part.part_id == UMPPartId.FORMAT_INITIALIZATION_METADATA:
                    metadata = protobug.load(part.data, FormatInitializationMetadata)
                    summary['format_initializations'].append({
                        'total_segments': metadata.total_segments,
                        'mime_type': metadata.mime_type,
                        'target_format': self._format_id_key(metadata.format_id) == target_format_key,
                    })
                elif part.part_id == UMPPartId.STREAM_PROTECTION_STATUS:
                    status = protobug.load(part.data, StreamProtectionStatus)
                    summary['stream_protection_status'] = StreamProtectionStatus.Status(status.status).name
                elif part.part_id == UMPPartId.SABR_ERROR:
                    sabr_error = protobug.load(part.data, SabrError)
                    summary['sabr_errors'].append(sabr_error.type)
                elif part.part_id == UMPPartId.SABR_REDIRECT:
                    redirect = protobug.load(part.data, SabrRedirect)
                    summary['redirect_urls'].append(redirect.redirect_url)
                elif part.part_id == UMPPartId.RELOAD_PLAYER_RESPONSE:
                    reload_response = protobug.load(part.data, ReloadPlayerResponse)
                    summary['reload_playback_token_present'] = bool(
                        traverse_obj(reload_response, ('reload_playback_params', 'token')))
                elif part.part_id == UMPPartId.NEXT_REQUEST_POLICY:
                    policy = protobug.load(part.data, NextRequestPolicy)
                    summary['playback_cookie_present'] = bool(policy.playback_cookie)
                elif part.part_id == UMPPartId.SABR_CONTEXT_UPDATE:
                    protobug.load(part.data, SabrContextUpdate)
                elif part.part_id == UMPPartId.SABR_CONTEXT_SENDING_POLICY:
                    protobug.load(part.data, SabrContextSendingPolicy)
                elif part.part_id == UMPPartId.SABR_SEEK:
                    summary['seek_events'] += 1

            summary['target_sequences'] = [
                segment['sequence_number']
                for segment in summary['segments']
                if segment['target_format'] and not segment['is_init_segment'] and segment['sequence_number'] is not None
            ]
            summary['window_size'] = len(summary['target_sequences'])
            summary['target_total_bytes'] = sum(summary['target_segment_bytes'].values())
            summary['first_target_sequence'] = (
                summary['target_sequences'][0] if summary['target_sequences'] else None
            )
            return summary, segment_data

        def _persist_window_result(self, state, state_path, segment_dir, task, summary, segment_data, warmup):
            window_key = str(task.target_sequence)
            state['windows'].setdefault(window_key, {})

            if summary.get('error'):
                state['windows'][window_key] = {
                    'status': 'error',
                    'error': summary['error'],
                    'request': summary.get('request'),
                }
                self._save_window_state(state_path, state)
                return 0, 0

            duplicate_count = 0
            written_count = 0
            written_tracks = {}

            for format_key, sequences in segment_data.items():
                label = self._format_label(format_key, state)
                track_state = state['tracks'].setdefault(label, {})
                written_tracks[label] = []

                for sequence_key, data in sequences.items():
                    filename = self._segment_part_path(format_key, state, sequence_key)
                    size = len(data)
                    existing = track_state.get(sequence_key)
                    if existing and existing.get('size') == size and os.path.isfile(existing.get('path', filename)):
                        duplicate_count += 1
                        written_tracks[label].append(existing)
                        continue

                    with open(filename, 'wb') as f:
                        f.write(data)

                    record = {'path': filename, 'size': size}
                    track_state[sequence_key] = record
                    written_tracks[label].append(record)
                    written_count += 1
                    self._report_track_progress(format_key, state, warmup)

            state['windows'][window_key] = {
                'status': 'completed',
                'request': summary.get('request'),
                'target_sequences': summary.get('target_sequences'),
                'target_total_bytes': summary.get('target_total_bytes'),
                'written_tracks': written_tracks,
            }
            self._save_window_state(state_path, state)
            return duplicate_count, written_count

        def _format_label(self, format_key, state):
            fmt = (state.get('formats') or {}).get(format_key) or {}
            if fmt.get('display_name'):
                return str(fmt['display_name'])
            try:
                payload = json.loads(format_key)
            except Exception:
                payload = {}
            itag = payload.get('itag')
            return f'itag{itag}' if itag is not None else f'fmt_{abs(hash(format_key))}'

        def _segment_part_path(self, format_key, state, part_id):
            fmt = (state.get('formats') or {}).get(format_key) or {}
            filename = fmt.get('filename')
            label = self._format_label(format_key, state)

            if state.get('segment_dir_explicit'):
                basename = os.path.basename(filename) if filename else label
                base = os.path.join(state.get('segment_dir') or '.', basename)
            else:
                base = filename or os.path.join(state.get('segment_dir') or '.', label)

            if part_id == 'init':
                return f'{base}.init.part'
            return f'{base}.seq{part_id}.part'

        def _track_downloaded_bytes(self, label, state):
            track_state = state.get('tracks', {}).get(label) or {}
            return sum(item.get('size', 0) for item in track_state.values())

        def _report_track_progress(self, format_key, state, warmup):
            fmt = (warmup.formats or {}).get(format_key) or {}
            info_dict = fmt.get('info_dict')
            if not info_dict:
                return
            if not info_dict.get('_filename') and fmt.get('filename'):
                info_dict['_filename'] = fmt.get('filename')
            label = self._format_label(format_key, state)
            downloaded_bytes = self._track_downloaded_bytes(label, state)
            total_bytes = info_dict.get('filesize') or info_dict.get('filesize_approx')
            elapsed = max(0.001, time.time() - getattr(self, '_sabr_download_start_time', time.time()))
            speed = downloaded_bytes / elapsed
            eta = None
            if total_bytes and speed > 0 and total_bytes >= downloaded_bytes:
                eta = (total_bytes - downloaded_bytes) / speed
            self._hook_progress({
                'status': 'downloading',
                'downloaded_bytes': downloaded_bytes,
                'total_bytes': total_bytes,
                'filename': fmt.get('filename'),
                'eta': eta,
                'speed': speed,
                'elapsed': elapsed,
            }, info_dict)

        def _report_track_finished(self, format_key, output_path, state, warmup):
            fmt = (warmup.formats or {}).get(format_key) or {}
            info_dict = fmt.get('info_dict')
            if not info_dict or not output_path or not os.path.isfile(output_path):
                return
            if not info_dict.get('_filename'):
                info_dict['_filename'] = output_path
            elapsed = max(0.001, time.time() - getattr(self, '_sabr_download_start_time', time.time()))
            total_bytes = os.path.getsize(output_path)
            self._hook_progress({
                'status': 'finished',
                'filename': output_path,
                'total_bytes': total_bytes,
                'elapsed': elapsed,
            }, info_dict)

        def _merge_window_track(self, config, state, segment_dir, warmup):
            merged_outputs = {}
            all_tracks_complete = True

            for format_key, fmt in (state.get('formats') or {}).items():
                label = self._format_label(format_key, state)
                track_state = state.get('tracks', {}).get(label) or {}
                sequence_numbers = sorted(
                    int(sequence_key)
                    for sequence_key in track_state
                    if sequence_key != 'init'
                )
                if not sequence_numbers:
                    # 没有数据片段，标记为不完整
                    all_tracks_complete = False
                    continue

                # 检查完整性
                is_complete, missing_sequences = self._check_track_completeness(
                    track_state, warmup, format_key
                )
                
                if not is_complete:
                    all_tracks_complete = False
                    if missing_sequences:
                        self.report_warning(
                            f'SABR concurrent download is missing {len(missing_sequences)} sequence(s) '
                            f'for format {label}: {self._format_missing_sequences(missing_sequences)}',
                        )
                    # 根据需求：part 不完整就不合并
                    continue

                filename = fmt.get('filename')
                if not filename:
                    all_tracks_complete = False
                    continue

                # 使用并发合并功能
                final_output = self._concurrent_merge_track(
                    config=config,
                    track_state=track_state,
                    filename=filename,
                    label=label,
                    sequence_numbers=sequence_numbers
                )
                
                merged_outputs[label] = final_output
                self._report_track_finished(format_key, final_output, state, warmup)

            # 返回合并结果和完整性状态
            state['all_tracks_complete'] = all_tracks_complete
            return merged_outputs
        
        def _check_track_completeness(self, track_state, warmup, format_key):
            """
            检查轨道完整性
            返回：(是否完整, 缺失序列列表)
            """
            # 获取所有序列号
            sequence_numbers = sorted(
                int(sequence_key)
                for sequence_key in track_state
                if sequence_key != 'init'
            )
            
            if not sequence_numbers:
                return False, []
            
            # 检查是否有缺失序列
            missing_sequences = self._missing_sequences(sequence_numbers)
            
            # 检查是否覆盖了所有需要的片段
            fmt = (warmup.formats or {}).get(format_key) or {}
            info_dict = fmt.get('info_dict')
            
            if info_dict:
                total_segments = info_dict.get('total_segments')
                if total_segments and isinstance(total_segments, int):
                    # 检查是否从1开始到total_segments都有覆盖
                    expected_sequences = set(range(1, total_segments + 1))
                    actual_sequences = set(sequence_numbers)
                    
                    # 找出所有缺失（包括开头和结尾的缺失）
                    all_missing = sorted(expected_sequences - actual_sequences)
                    if all_missing:
                        missing_sequences = list(set(missing_sequences + all_missing))
            
            # 检查文件大小是否匹配预期（如果有预期大小）
            downloaded_bytes = sum(item.get('size', 0) for item in track_state.values())
            if info_dict:
                expected_bytes = info_dict.get('filesize') or info_dict.get('filesize_approx')
                if expected_bytes and downloaded_bytes > 0:
                    # 允许10%的误差，因为压缩等原因
                    tolerance = 0.1
                    min_expected = expected_bytes * (1 - tolerance)
                    max_expected = expected_bytes * (1 + tolerance)
                    
                    if not (min_expected <= downloaded_bytes <= max_expected):
                        self.to_screen(
                            f'[sabr-completeness] {format_key}: '
                            f'downloaded={downloaded_bytes}, expected={expected_bytes}, '
                            f'ratio={downloaded_bytes/expected_bytes:.2%}'
                        )
                        # 文件大小不匹配，但可能仍然可以继续
                        # 不标记为不完整，只记录日志
            
            is_complete = len(missing_sequences) == 0
            
            return is_complete, missing_sequences

        def _missing_sequences(self, sequence_numbers):
            if not sequence_numbers:
                return []
            missing = []
            previous = sequence_numbers[0]
            for current in sequence_numbers[1:]:
                if current > previous + 1:
                    missing.extend(range(previous + 1, current))
                previous = current
            return missing

        def _format_missing_sequences(self, missing_sequences, max_display=20):
            if len(missing_sequences) <= max_display:
                return ', '.join(map(str, missing_sequences))
            head = ', '.join(map(str, missing_sequences[:max_display]))
            return f'{head}, ...'

        def _cleanup_concurrency_temp_files(self, state_path, segment_dir, merge_output):
            merged_ok = bool(merge_output) and all(
                os.path.isfile(path)
                for path in (merge_output.values() if isinstance(merge_output, dict) else [merge_output])
            )
            if not merged_ok:
                return

            if state_path and os.path.isfile(state_path):
                state = {}
                try:
                    with open(state_path, 'r', encoding='utf-8') as f:
                        state = json.load(f)
                except Exception:
                    state = {}

                for track in (state.get('tracks') or {}).values():
                    for item in track.values():
                        path = item.get('path')
                        if path and path.endswith('.part') and os.path.isfile(path):
                            self.try_remove(path)

                if state.get('segment_dir_explicit') and os.path.isdir(segment_dir):
                    try:
                        if not os.listdir(segment_dir):
                            os.rmdir(segment_dir)
                    except OSError:
                        pass

                self.try_remove(state_path)

        def _read_varint(self, fp):
            byte = fp.read(1)
            if not byte:
                return -1

            prefix = byte[0]
            size = (
                1 if prefix < 128 else
                2 if prefix < 192 else
                3 if prefix < 224 else
                4 if prefix < 240 else
                5
            )
            result = 0
            shift = 0

            if size != 5:
                shift = 8 - size
                mask = (1 << shift) - 1
                result |= prefix & mask

            for _ in range(1, size):
                next_byte = fp.read(1)
                if not next_byte:
                    return -1
                value = next_byte[0]
                result |= value << shift
                shift += 8

            return result

        def _calculate_optimal_merge_concurrency(self, total_sequences, total_size_bytes=None, label=""):
            """
            计算最优合并并发数（带优雅降级）
            """
            try:
                import psutil
                # 使用完整算法（如果有psutil）
                return self._calculate_concurrency_with_psutil(total_sequences, total_size_bytes, label)
            except ImportError:
                # 使用简化算法（无psutil）
                self.to_screen(f'[sabr-merge] psutil not available, using simplified algorithm for {label}')
                return self._calculate_simple_concurrency(total_sequences, total_size_bytes, label)

        def _calculate_concurrency_with_psutil(self, total_sequences, total_size_bytes=None, label=""):
            """
            使用psutil的完整算法
            """
            import os
            import sys
            
            # 1. 基础CPU核心数
            cpu_cores = os.cpu_count() or 1
            
            # 2. 可用内存考虑（避免内存溢出）
            memory_info = psutil.virtual_memory()
            memory_gb = memory_info.available / (1024**3)
            
            # 3. 磁盘类型推断（简化版本）
            # 检查/tmp目录或工作目录所在文件系统
            disk_type = 'hdd'  # 默认保守假设为机械硬盘
            try:
                import psutil
                disk_partitions = psutil.disk_partitions()
                for partition in disk_partitions:
                    if partition.mountpoint in ['/', '/tmp', os.path.expanduser('~')]:
                        # 检查是否为SSD（简化检查）
                        # 实际上需要更复杂的检测，这里使用保守假设
                        if 'ssd' in partition.opts.lower() or 'flash' in partition.opts.lower():
                            disk_type = 'ssd'
                        break
            except:
                pass
            
            # 4. 工作负载因子
            workload_factor = 1.0
            # 因子1：文件数量
            sequence_factor = min(total_sequences / 100, 3.0)  # 文件越多，因子越高
            
            # 因子2：平均文件大小
            avg_size_factor = 1.0
            if total_size_bytes and total_sequences > 0:
                avg_size_mb = total_size_bytes / (1024**2) / total_sequences
                if avg_size_mb < 1:  # 小文件，适合高并发
                    avg_size_factor = 1.5
                elif avg_size_mb > 10:  # 大文件，限制并发
                    avg_size_factor = 0.7
            
            workload_factor = sequence_factor * avg_size_factor
            
            # 5. 加权并发计算
            # 基础并发：CPU核心数
            base_concurrency = cpu_cores
            
            # 内存调整：每GB内存支持1个并发（保守估计）
            memory_adjusted = min(base_concurrency, max(1, int(memory_gb)))
            
            # 磁盘类型调整
            if disk_type == 'ssd':
                disk_multiplier = 1.5  # SSD支持更高并发
            elif disk_type == 'nvme':
                disk_multiplier = 2.0  # NVMe支持更高并发
            else:  # 机械硬盘
                disk_multiplier = 0.7  # 机械硬盘降低并发
            
            # 工作负载调整
            workload_adjusted = memory_adjusted * disk_multiplier * workload_factor
            
            concurrency = max(1, int(workload_adjusted))
            
            # 6. 边界调整
            concurrency = self._adjust_concurrency_bounds(concurrency, total_sequences, label)
            
            # 记录决策信息
            self.to_screen(
                f'[sabr-merge] {label}: cpu_cores={cpu_cores}, memory_gb={memory_gb:.1f}, '
                f'disk_type={disk_type}, workload_factor={workload_factor:.2f}, '
                f'suggested_concurrency={concurrency}'
            )
            
            return concurrency

        def _calculate_simple_concurrency(self, total_sequences, total_size_bytes=None, label=""):
            """
            简化版并发计算（无psutil依赖）
            """
            import os
            import sys
            
            # 基础：CPU核心数
            cpu_cores = os.cpu_count() or 1
            
            # 根据平台调整
            if sys.platform == 'darwin' or sys.platform.startswith('linux'):
                # macOS/Linux：通常性能较好
                base = min(cpu_cores, 8)
            else:
                # 其他平台保守一些
                base = min(cpu_cores, 4)
            
            # 根据文件数量调整
            if total_sequences < 30:
                return 1  # 文件太少，无需并发
            elif total_sequences < 100:
                return min(base, 2)
            elif total_sequences < 300:
                return min(base, 4)
            elif total_sequences < 500:
                return min(base, 6)
            else:
                return min(base, 8)

        def _adjust_concurrency_bounds(self, concurrency, total_sequences, label):
            """
            边界检查和调整
            """
            # 下限：至少1个并发
            concurrency = max(1, concurrency)
            
            # 上限1：不超过总片段数的1/4（避免太多小分组）
            if total_sequences > 0:
                concurrency = min(concurrency, max(1, total_sequences // 4))
            
            # 上限2：绝对上限（避免过度并发）
            absolute_max = 16  # 即使是强大服务器也不超过16并发
            concurrency = min(concurrency, absolute_max)
            
            return concurrency

        def _concurrent_merge_track(self, config, track_state, filename, label, sequence_numbers):
            """
            并发合并单个轨道
            返回：最终输出文件路径
            """
            if not config.get('enable_concurrent_merge', True):
                # 禁用并发合并，使用原始顺序合并
                return self._sequential_merge_track(track_state, filename, sequence_numbers)
            
            # 计算最优并发数
            total_sequences = len(sequence_numbers)
            
            # 估算总大小
            total_size_bytes = sum(
                track_state.get(str(seq), {}).get('size', 0)
                for seq in sequence_numbers
            )
            
            # 获取配置的并发数，如果没有则自动计算
            merge_concurrency = config.get('merge_concurrency')
            if merge_concurrency is None:
                merge_concurrency = self._calculate_optimal_merge_concurrency(
                    total_sequences, total_size_bytes, label
                )
            
            # 边界检查
            merge_concurrency = max(1, min(merge_concurrency, total_sequences))
            
            # 如果并发数为1或文件太少，使用顺序合并
            if merge_concurrency <= 1 or total_sequences < 10:
                self.to_screen(f'[sabr-merge] {label}: sequential merge (files={total_sequences})')
                return self._sequential_merge_track(track_state, filename, sequence_numbers)
            
            # 记录并发合并信息
            self.to_screen(
                f'[sabr-merge] {label}: starting concurrent merge with {merge_concurrency} threads '
                f'(files={total_sequences})'
            )
            
            # 创建临时中间文件
            intermediate_files = self._create_intermediate_files(
                config, filename, merge_concurrency, label
            )
            
            # 分组序列号
            groups = self._group_sequences_for_concurrent_merge(
                sequence_numbers, merge_concurrency
            )
            
            # 并发合并到中间文件
            self._merge_to_intermediate_files_concurrently(
                track_state, groups, intermediate_files, label
            )
            
            # 合并中间文件到最终文件
            final_output = self._merge_intermediate_files_to_final(
                intermediate_files, filename, label, track_state
            )
            
            # 清理中间文件
            self._cleanup_intermediate_files(intermediate_files)
            
            return final_output

        def _sequential_merge_track(self, track_state, filename, sequence_numbers):
            """
            顺序合并轨道（原始实现）
            """
            temp_output = self.temp_name(filename)
            output_dir = os.path.dirname(temp_output)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            with open(temp_output, 'wb') as out_fp:
                init_info = track_state.get('init')
                if init_info and os.path.isfile(init_info['path']):
                    with open(init_info['path'], 'rb') as in_fp:
                        out_fp.write(in_fp.read())

                for sequence_number in sequence_numbers:
                    record = track_state.get(str(sequence_number))
                    if not record:
                        continue
                    path = record.get('path')
                    if not path or not os.path.isfile(path):
                        continue
                    with open(path, 'rb') as in_fp:
                        out_fp.write(in_fp.read())

            self.try_rename(temp_output, self.undo_temp_name(temp_output))
            return self.undo_temp_name(temp_output)

        def _create_intermediate_files(self, config, filename, concurrency, label):
            """
            创建中间文件
            """
            intermediate_files = []
            intermediate_prefix = config.get('intermediate_file_prefix', '.sabr_merge_')
            
            for i in range(concurrency):
                intermediate_name = f'{intermediate_prefix}{label}_{i}.tmp'
                intermediate_path = os.path.join(os.path.dirname(filename), intermediate_name)
                intermediate_files.append(intermediate_path)
            
            return intermediate_files

        def _group_sequences_for_concurrent_merge(self, sequence_numbers, concurrency):
            """
            为并发合并分组序列号
            """
            total_sequences = len(sequence_numbers)
            if total_sequences <= 0:
                return []

            concurrency = max(1, min(concurrency, total_sequences))
            base_group_size, remainder = divmod(total_sequences, concurrency)
            groups = []
            start = 0

            # 连续区间分组，确保每个中间文件内部和最终拼接顺序都保持原始时序。
            for group_index in range(concurrency):
                extra = 1 if group_index < remainder else 0
                end = start + base_group_size + extra
                if start >= total_sequences:
                    break
                groups.append(sequence_numbers[start:end])
                start = end

            if not groups:
                return []

            group_sizes = [len(group) for group in groups]
            group_ranges = [f'{group[0]}-{group[-1]}' for group in groups if group]
            self.to_screen(
                f'[sabr-merge] contiguous groups: sizes={group_sizes}, ranges={group_ranges} '
                f'(min={min(group_sizes)}, max={max(group_sizes)})'
            )

            return groups

        def _merge_to_intermediate_files_concurrently(self, track_state, groups, intermediate_files, label):
            """
            并发合并到中间文件
            """
            import concurrent.futures
            
            def merge_group(group, intermediate_file):
                """合并单个分组到中间文件"""
                with open(intermediate_file, 'wb') as out_fp:
                    for sequence_number in group:
                        record = track_state.get(str(sequence_number))
                        if not record:
                            continue
                        path = record.get('path')
                        if not path or not os.path.isfile(path):
                            continue
                        with open(path, 'rb') as in_fp:
                            out_fp.write(in_fp.read())
                return len(group)
            
            # 使用线程池并发合并
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(groups)) as executor:
                # 提交所有任务
                future_to_group = {}
                for i, (group, intermediate_file) in enumerate(zip(groups, intermediate_files)):
                    if group:  # 只合并非空分组
                        future = executor.submit(merge_group, group, intermediate_file)
                        future_to_group[future] = (i, len(group))
                
                # 等待完成并报告进度
                completed = 0
                total = sum(len(g) for g in groups)
                
                for future in concurrent.futures.as_completed(future_to_group):
                    i, group_size = future_to_group[future]
                    try:
                        merged_count = future.result()
                        completed += merged_count
                        self.to_screen(
                            f'[sabr-merge] group {i}: merged {merged_count}/{group_size} files '
                            f'({completed}/{total} total, {completed/total*100:.1f}%)'
                        )
                    except Exception as e:
                        self.to_screen(f'[sabr-merge] error in group {i}: {e}')

        def _merge_intermediate_files_to_final(self, intermediate_files, filename, label, track_state=None):
            """
            合并中间文件到最终文件
            参数:
                intermediate_files: 中间文件列表
                filename: 最终输出文件名
                label: 轨道标签（用于日志）
                track_state: 轨道状态字典（可选，用于获取init段）
            """
            temp_output = self.temp_name(filename)
            output_dir = os.path.dirname(temp_output)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            with open(temp_output, 'wb') as out_fp:
                # 首先写入init段（如果有）
                if track_state:
                    init_info = track_state.get('init')
                    if init_info and os.path.isfile(init_info.get('path', '')):
                        try:
                            with open(init_info['path'], 'rb') as in_fp:
                                out_fp.write(in_fp.read())
                            self.to_screen(f'[sabr-merge] {label}: wrote init segment')
                        except Exception as e:
                            self.to_screen(f'[sabr-merge] {label}: error writing init segment: {e}')

                # 合并所有中间文件
                for intermediate_file in intermediate_files:
                    if os.path.exists(intermediate_file):
                        try:
                            with open(intermediate_file, 'rb') as in_fp:
                                out_fp.write(in_fp.read())
                        except Exception as e:
                            self.to_screen(f'[sabr-merge] {label}: error merging {intermediate_file}: {e}')

            self.try_rename(temp_output, self.undo_temp_name(temp_output))
            final_output = self.undo_temp_name(temp_output)

            self.to_screen(f'[sabr-merge] {label}: merged {len(intermediate_files)} intermediate files to final output')

            return final_output

        def _cleanup_intermediate_files(self, intermediate_files):
            """
            清理中间文件
            """
            for intermediate_file in intermediate_files:
                if os.path.exists(intermediate_file):
                    try:
                        os.remove(intermediate_file)
                    except OSError:
                        pass  # 忽略清理错误

        def _log_window_download(self, window_report):
            self.to_screen(
                f'[sabr] stride={window_report["stride"]} '
                f'completed_windows={len(window_report["completed_windows"])} '
                f'written_sequences={window_report["written_sequences"]} '
                f'duplicate_sequences={window_report["duplicate_sequences"]}',
            )
            if window_report.get('merge_output'):
                self.to_screen(f'[sabr] merged tracks -> {window_report["merge_output"]}')

        def _format_id_key(self, format_id):
            if format_id is None:
                return None
            return json.dumps({
                'itag': getattr(format_id, 'itag', None),
                'lmt': getattr(format_id, 'lmt', None),
                'xtags': getattr(format_id, 'xtags', None),
            }, sort_keys=True, ensure_ascii=True)


    class SabrFD(_SabrConcurrencyMixin, _SabrFD):
        pass
