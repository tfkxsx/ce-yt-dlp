"""Fetch a Vimeo player page through a resident Newman service or subprocess."""

"""
    命令版

"""

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import urlparse


BASE_DIR = Path(__file__).resolve().parent
COLLECTION_PATH = BASE_DIR / "collection.json"
DEFAULT_URL = "https://player.vimeo.com/video/"


def _request_variables(video_url: str) -> tuple[str, str]:
    """Extract the collection variables from a Vimeo player URL.
    
    Supports both formats:
    - https://player.vimeo.com/video/<video_id>
    - https://vimeo.com/<video_id>
    """
    parsed = urlparse(video_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Invalid player URL: {video_url!r}")
    
    netloc = parsed.netloc
    path_parts = [part for part in parsed.path.split("/") if part]
    
    if not path_parts:
        raise ValueError(f"Invalid player URL: {video_url!r}")
    
    video_id = None
    domain = netloc.lower()
    
    if "player.vimeo.com" in domain and len(path_parts) >= 2 and path_parts[0] == "video":
        # Format: https://player.vimeo.com/video/<video_id>
        video_id = path_parts[1]
    elif "vimeo.com" in domain and not domain.startswith("player."):
        # Format: https://vimeo.com/<video_id>
        video_id = path_parts[0]
    else:
        raise ValueError(
            f"Unsupported Vimeo URL format: {video_url!r}. "
            "Expected 'https://player.vimeo.com/video/<video_id>' "
            "or 'https://vimeo.com/<video_id>'"
        )
    
    if not video_id:
        raise ValueError(f"Could not extract video ID from: {video_url!r}")
    
    return video_id, netloc


def _body_from_response(response: dict) -> str:
    """Decode the response stream emitted by Newman's JSON reporter."""
    stream = response.get("stream")

    # Newman serializes Node.js Buffers as {"type": "Buffer", "data": [...]}
    if isinstance(stream, dict) and stream.get("type") == "Buffer":
        stream = stream.get("data", [])
    if isinstance(stream, list):
        try:
            return bytes(stream).decode("utf-8", errors="replace")
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Newman returned an invalid response buffer") from exc
    if isinstance(stream, str):
        return stream

    # Keep this fallback for Newman/reporters that expose the body directly.
    body = response.get("body")
    if isinstance(body, str):
        return body
    if body is not None:
        return str(body)
    raise RuntimeError("Newman returned a response without a body")


def fetch_vimeo_html(
    video_url: str,
    *,
    timeout: float = 60,
    proxy: str,
) -> str:
    """Return the player's HTML response.

    The resident Newman service is tried first. If its TCP endpoint is not
    running, the function falls back to launching Newman as a subprocess.

    A non-zero Newman exit code caused only by a collection test assertion does
    not discard a valid HTTP response.  A request error or missing response
    still raises ``RuntimeError``.
    """
    # video_url = f'{DEFAULT_URL}{video_id}'
    video_id, player_host = _request_variables(video_url)

    collection_path = Path(COLLECTION_PATH)
    newman = os.environ.get("NEWMAN_BIN") or shutil.which("newman")
    if not newman:
        raise FileNotFoundError(
            "Newman was not found. Install it with `npm install -g newman` "
            "or set NEWMAN_BIN to its executable."
        )
    if not collection_path.is_file():
        raise FileNotFoundError(f"Collection file not found: {collection_path}")

    command = [
        newman,
        "run",
        str(collection_path),
        "--env-var",
        f"video_id={video_id}",
        "--env-var",
        f"player_host={player_host}",
        "--reporters",
        "json",
    ]
    process_env = os.environ.copy()
    if proxy:
        # Newman reads proxy configuration from these environment variables.
        for variable in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            process_env[variable] = proxy
    else:
        for variable in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            process_env.pop(variable, None)

    with tempfile.TemporaryDirectory(prefix="newman-vimeo-") as temp_dir:
        report_path = Path(temp_dir) / "report.json"
        command.extend(["--reporter-json-export", str(report_path)])
        try:
            completed = subprocess.run(
                command,
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
                check=False,
                env=process_env,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"Newman timed out after {timeout:g} seconds") from exc

        if not report_path.is_file():
            details = (completed.stderr or completed.stdout).strip()
            raise RuntimeError(
                f"Newman did not produce a JSON report (exit code {completed.returncode})."
                + (f"\n{details}" if details else "")
            )

        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError("Newman produced invalid JSON output") from exc

    executions = report.get("run", {}).get("executions", [])
    response = next(
        (
            execution.get("response")
            for execution in executions
            if execution.get("response")
        ),
        None,
    )
    if response is None:
        request_error = next(
            (
                execution.get("requestError")
                for execution in executions
                if execution.get("requestError")
            ),
            None,
        )
        message = "Newman did not receive an HTTP response"
        if request_error:
            message += f": {request_error.get('code') or request_error.get('message') or request_error}"
        raise RuntimeError(message)

    return _body_from_response(response)


if __name__ == "__main__":
    import time
    st = int(time.time() * 1000)
    # print(fetch_vimeo_html("https://player.vimeo.com/video/1055303619", proxy='http://127.0.0.1:20171'))
    # print(fetch_vimeo_html("https://player.vimeo.com/video/1000000014", proxy='http://127.0.0.1:20171'))
    print(fetch_vimeo_html("https://vimeo.com/1087715186", proxy='http://127.0.0.1:20171'))
    et = int(time.time() * 1000)
    print(f"耗时： {et - st} 毫秒")
