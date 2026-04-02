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

            targets = [
                sample_target + (index * window_stride)
                for index in range(window_count)
                if not total_segments or sample_target + (index * window_stride) <= total_segments
            ]
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

        def _detect_window_stride(self, sample_summary, target_sequence):
            sequences = sample_summary.get('target_sequences') or []
            if not sequences:
                return 0
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
            return count

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

            targets = set()
            for missing_sequences in missing_by_track.values():
                for sequence in missing_sequences:
                    if sequence < window_start:
                        targets.add(sequence)
                    else:
                        offset = sequence - window_start
                        targets.add(window_start + ((offset // window_size) * window_size))
            return sorted(targets)

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

            for format_key, fmt in (state.get('formats') or {}).items():
                label = self._format_label(format_key, state)
                track_state = state.get('tracks', {}).get(label) or {}
                sequence_numbers = sorted(
                    int(sequence_key)
                    for sequence_key in track_state
                    if sequence_key != 'init'
                )
                if not sequence_numbers:
                    continue

                missing_sequences = self._missing_sequences(sequence_numbers)
                if missing_sequences:
                    self.report_warning(
                        f'SABR concurrent download is missing {len(missing_sequences)} sequence(s) '
                        f'for format {label}: {self._format_missing_sequences(missing_sequences)}',
                    )

                filename = fmt.get('filename')
                if not filename:
                    continue

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
                final_output = self.undo_temp_name(temp_output)
                merged_outputs[label] = final_output
                self._report_track_finished(format_key, final_output, state, warmup)

            return merged_outputs

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
