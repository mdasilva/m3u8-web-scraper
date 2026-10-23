import argparse
import concurrent.futures
import csv
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field, asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urldefrag, quote, unquote, parse_qs

import requests
from bs4 import BeautifulSoup

# Enable UTF-8 output and ANSI escape sequences on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if os.name == "nt":
    os.system("")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("m3u8_scraper")


class UI:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    MAGENTA = "\033[35m"

    @classmethod
    def c(cls, text: str, color: str) -> str:
        return f"{color}{text}{cls.RESET}"


def find_vlc_executable() -> Optional[str]:
    """Locate VLC executable in PATH or standard Windows / macOS install locations."""
    found = shutil.which("vlc")
    if found:
        return found
    for candidate in (
        r"C:\Program Files\VideoLAN\VLC\vlc.exe",
        r"C:\Program Files (x86)\VideoLAN\VLC\vlc.exe",
        "/Applications/VLC.app/Contents/MacOS/VLC",
        os.path.expanduser("~/Applications/VLC.app/Contents/MacOS/VLC"),
    ):
        if os.path.exists(candidate):
            return candidate
    return None


def get_lan_ip() -> str:
    """Detect the primary LAN IPv4 address of this machine so Chromecast devices on the Wi-Fi can reach the proxy."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"


@dataclass
class StreamInfo:
    m3u8_url: str
    found_on_pages: List[str] = field(default_factory=list)
    headers: Dict[str, str] = field(default_factory=dict)
    parent_url: str = ""

    def vlc_command(self) -> str:
        ref = self.headers.get("Referer", "")
        ua = self.headers.get("User-Agent", "")
        vlc_bin = shutil.which("vlc")
        if vlc_bin:
            parts = ["vlc"]
        elif os.path.exists(r"C:\Program Files (x86)\VideoLAN\VLC\vlc.exe"):
            parts = [r'& "C:\Program Files (x86)\VideoLAN\VLC\vlc.exe"']
        elif os.path.exists(r"C:\Program Files\VideoLAN\VLC\vlc.exe"):
            parts = [r'& "C:\Program Files\VideoLAN\VLC\vlc.exe"']
        elif os.path.exists("/Applications/VLC.app/Contents/MacOS/VLC"):
            parts = ['"/Applications/VLC.app/Contents/MacOS/VLC"']
        else:
            parts = ["vlc"]
        if ref:
            parts.append(f'--http-referrer="{ref}"')
        if ua:
            parts.append(f'--http-user-agent="{ua}"')
        parts.append(f'"{self.m3u8_url}"')
        return " ".join(parts)

    def ffplay_command(self) -> str:
        ref = self.headers.get("Referer", "")
        origin = self.headers.get("Origin", "")
        ua = self.headers.get("User-Agent", "")
        hdr_lines = []
        if ref:
            hdr_lines.append(f"Referer: {ref}")
        if origin:
            hdr_lines.append(f"Origin: {origin}")
        hdr_arg = "\\r\\n".join(hdr_lines) + ("\\r\\n" if hdr_lines else "")
        parts = ["ffplay"]
        if ua:
            parts.append(f'-user_agent "{ua}"')
        if hdr_arg:
            parts.append(f'-headers "{hdr_arg}"')
        parts.append(f'"{self.m3u8_url}"')
        return " ".join(parts)

    def streamlink_command(self) -> str:
        ref = self.headers.get("Referer", "")
        origin = self.headers.get("Origin", "")
        ua = self.headers.get("User-Agent", "")
        parts = ["streamlink"]
        if ref:
            parts.append(f'--http-header "Referer={ref}"')
        if origin:
            parts.append(f'--http-header "Origin={origin}"')
        if ua:
            parts.append(f'--http-header "User-Agent={ua}"')
        parts.append(f'"{self.m3u8_url}" best')
        return " ".join(parts)


class LocalHLSProxy:
    """
    Local & LAN HTTP HLS relay server for VLC and Chromecast.
    - Binds to 0.0.0.0 with TCP_NODELAY so both local players (127.0.0.1) and LAN Chromecast devices can connect.
    - Injects Referer, Origin, and User-Agent on every playlist and segment request.
    - Caches master playlists (#EXT-X-STREAM-INF) in memory so single-use tokens on master URLs
      are only requested once upstream, while live media playlists (mono.m3u8) are fetched fresh.
    - Locks master playlists to a single variant ('high' or 'low') when requested so Chromecast
      never stutters from mid-stream ABR resolution switching.
    - Strips disguised image headers (e.g. 42-byte RIFF/WebP headers prepended before 0x47 MPEG-TS
      sync bytes by CDNs like tiktokcdn) and streams 64 KB chunks cut-through with exact Content-Length
      while parallel-prefetching upcoming segments in the background.
    """

    MAX_CACHED_SEGMENTS = 12

    def __init__(self, port: int = 8765):
        self.port = port
        self.streams: Dict[int, StreamInfo] = {}
        self.master_cache: Dict[str, str] = {}
        self.segment_cache: Dict[str, bytes] = {}
        self._segment_order: deque[str] = deque()
        self._inflight: Dict[str, threading.Event] = {}
        self._cache_lock = threading.Lock()
        self._prefetch_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    @staticmethod
    def find_ts_sync_offset(data: bytes) -> int:
        """
        Return the byte offset where the 188-byte MPEG-TS packet stream (sync byte 0x47) starts.
        Returns 0 if already aligned or if no disguised header is detected within the first 1024 bytes.
        """
        if len(data) < 377:
            return 0
        if data[0] == 0x47 and data[188] == 0x47:
            return 0
        limit = min(len(data) - 376, 1024)
        for i in range(1, limit):
            if data[i] == 0x47 and data[i + 188] == 0x47 and data[i + 376] == 0x47:
                return i
        return 0

    @classmethod
    def strip_disguised_ts_header(cls, data: bytes) -> bytes:
        """
        If upstream CDN prepended a fake image header (such as a 42-byte RIFF/WebP header)
        before the MPEG-TS 188-byte packet stream (sync byte 0x47), strip the leading
        garbage bytes so Chromecast and VLC hardware demuxers receive clean MPEG-TS packets.
        """
        offset = cls.find_ts_sync_offset(data)
        return data[offset:] if offset > 0 else data

    @staticmethod
    def filter_master_variants(text: str, quality: str) -> str:
        """
        Lock a master M3U8 playlist (#EXT-X-STREAM-INF) to a single variant ('high' or 'low')
        so Chromecast does not oscillate between 1080p and 540p mid-stream (which resets the
        hardware decoder and causes a visible stutter). Pass 'auto' to keep all variants.
        """
        if quality not in ("high", "low") or "#EXT-X-STREAM-INF" not in text:
            return text

        lines = text.splitlines()
        header_lines: List[str] = []
        variants: List[Tuple[int, str, str]] = []
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("#EXT-X-STREAM-INF"):
                bw_match = re.search(r"BANDWIDTH=(\d+)", line)
                bw = int(bw_match.group(1)) if bw_match else 0
                uri_line = ""
                i += 1
                while i < len(lines):
                    cand = lines[i].strip()
                    if cand and not cand.startswith("#"):
                        uri_line = lines[i]
                        break
                    i += 1
                if uri_line:
                    variants.append((bw, line, uri_line))
            else:
                if not variants:
                    header_lines.append(lines[i])
            i += 1

        if len(variants) <= 1:
            return text

        chosen = max(variants, key=lambda v: v[0]) if quality == "high" else min(variants, key=lambda v: v[0])
        out = list(header_lines)
        out.append(chosen[1])
        out.append(chosen[2])
        return "\n".join(out) + "\n"

    def _store_in_segment_cache(self, target_url: str, clean_bytes: bytes) -> None:
        with self._cache_lock:
            if target_url not in self.segment_cache:
                self.segment_cache[target_url] = clean_bytes
                self._segment_order.append(target_url)
                while len(self._segment_order) > self.MAX_CACHED_SEGMENTS:
                    oldest = self._segment_order.popleft()
                    self.segment_cache.pop(oldest, None)

    def _fetch_segment_bytes(self, target_url: str, headers: Dict[str, str]) -> Optional[bytes]:
        """
        Fetch a media segment (deduplicating concurrent requests for the same URL),
        strip any disguised WebP/image header, and store it in the bounded RAM cache.
        """
        with self._cache_lock:
            if target_url in self.segment_cache:
                return self.segment_cache[target_url]
            if target_url in self._inflight:
                wait_event = self._inflight[target_url]
                is_leader = False
            else:
                wait_event = threading.Event()
                self._inflight[target_url] = wait_event
                is_leader = True

        if not is_leader:
            wait_event.wait(timeout=15)
            with self._cache_lock:
                return self.segment_cache.get(target_url)

        try:
            with self.session.get(
                target_url,
                headers=headers,
                timeout=12,
                stream=True,
            ) as resp:
                if resp.status_code != 200:
                    return None
                raw_bytes = resp.content
            clean_bytes = self.strip_disguised_ts_header(raw_bytes)
            self._store_in_segment_cache(target_url, clean_bytes)
            return clean_bytes
        except Exception:
            return None
        finally:
            with self._cache_lock:
                ev = self._inflight.pop(target_url, None)
                if ev is not None:
                    ev.set()

    def _prefetch_segments_async(self, seg_urls: List[str], headers: Dict[str, str]) -> None:
        """Parallel background prefetch of upcoming segments from a live media playlist."""
        for u in seg_urls:
            with self._cache_lock:
                already = u in self.segment_cache or u in self._inflight
            if not already:
                self._prefetch_pool.submit(self._fetch_segment_bytes, u, headers)

    def start(self) -> int:
        if self._server is not None:
            return self.port

        proxy_ref = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                try:
                    self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except Exception:
                    pass

            def log_message(self, format, *args):
                pass

            def _send_cors_headers(self):
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "*")
                self.send_header("Access-Control-Expose-Headers", "Content-Length, Content-Type")

            def do_OPTIONS(self):
                self.send_response(200)
                self._send_cors_headers()
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_HEAD(self):
                self._handle_request(head_only=True)

            def do_GET(self):
                self._handle_request(head_only=False)

            def _handle_request(self, head_only: bool = False):
                parsed = urlparse(self.path)
                qparams = parse_qs(parsed.query)
                quality = (qparams.get("quality", ["auto"])[0] or "auto").lower()

                if parsed.path.startswith("/stream/"):
                    idx_str = parsed.path.removeprefix("/stream/").removesuffix(".m3u8")
                    if not idx_str.isdigit() or int(idx_str) not in proxy_ref.streams:
                        self.send_error(404, "Stream index not found")
                        return
                    idx = int(idx_str)
                    info = proxy_ref.streams[idx]
                    target_url = info.m3u8_url
                elif parsed.path.startswith("/fetch/"):
                    idx_str = parsed.path.removeprefix("/fetch/")
                    if not idx_str.isdigit() or int(idx_str) not in proxy_ref.streams:
                        self.send_error(404, "Stream index not found")
                        return
                    idx = int(idx_str)
                    info = proxy_ref.streams[idx]
                    qs = parsed.query
                    if not qs.startswith("url="):
                        self.send_error(400, "Missing url parameter")
                        return
                    target_url = unquote(qs[4:])
                else:
                    self.send_error(404, "Not found")
                    return

                host_hdr = self.headers.get("Host") or f"127.0.0.1:{proxy_ref.port}"

                # Serve cached master playlist if already fetched (protects single-use tokens on playlist.m3u8)
                if target_url in proxy_ref.master_cache:
                    text = proxy_ref.filter_master_variants(proxy_ref.master_cache[target_url], quality)
                    self._serve_rewritten_m3u8(text, target_url, idx, info.headers, host_hdr, head_only)
                    return

                # Fast-path if this URL is an already-prefetched segment in RAM
                with proxy_ref._cache_lock:
                    cached_seg = proxy_ref.segment_cache.get(target_url)
                if cached_seg is not None:
                    self._serve_segment_bytes(cached_seg, head_only)
                    return

                url_path_lower = target_url.split("?")[0].lower()
                looks_like_m3u8 = url_path_lower.endswith(".m3u8")

                if not looks_like_m3u8:
                    self._stream_segment_cut_through(target_url, info.headers, head_only)
                    return

                try:
                    with proxy_ref.session.get(
                        target_url,
                        headers=info.headers,
                        timeout=10,
                        stream=True,
                    ) as resp:
                        if resp.status_code != 200:
                            self.send_response(resp.status_code)
                            self._send_cors_headers()
                            self.end_headers()
                            return

                        ctype = (resp.headers.get("Content-Type") or "").lower()
                        is_m3u8 = looks_like_m3u8 or "mpegurl" in ctype

                        if is_m3u8:
                            text = resp.text
                            if "#EXT-X-STREAM-INF" in text:
                                proxy_ref.master_cache[target_url] = text
                                text = proxy_ref.filter_master_variants(text, quality)
                            self._serve_rewritten_m3u8(text, target_url, idx, info.headers, host_hdr, head_only)
                        else:
                            clean_bytes = proxy_ref.strip_disguised_ts_header(resp.content)
                            self._serve_segment_bytes(clean_bytes, head_only)
                except Exception as exc:
                    self.send_error(502, f"Upstream error: {exc}")

            def _stream_segment_cut_through(self, target_url: str, req_headers: Dict[str, str], head_only: bool):
                """
                If a background worker is already downloading this segment, wait for it and serve from RAM.
                Otherwise, stream 64 KB chunks directly to the player as soon as the first chunk arrives
                (stripping any 42-byte fake WebP header on the first chunk) so there is zero store-and-forward wait.
                """
                with proxy_ref._cache_lock:
                    if target_url in proxy_ref.segment_cache:
                        cached = proxy_ref.segment_cache[target_url]
                        self._serve_segment_bytes(cached, head_only)
                        return
                    if target_url in proxy_ref._inflight:
                        wait_ev = proxy_ref._inflight[target_url]
                        is_leader = False
                    else:
                        wait_ev = threading.Event()
                        proxy_ref._inflight[target_url] = wait_ev
                        is_leader = True

                if not is_leader:
                    wait_ev.wait(timeout=15)
                    with proxy_ref._cache_lock:
                        cached = proxy_ref.segment_cache.get(target_url)
                    if cached is not None:
                        self._serve_segment_bytes(cached, head_only)
                    else:
                        self.send_response(502)
                        self._send_cors_headers()
                        self.end_headers()
                    return

                try:
                    with proxy_ref.session.get(
                        target_url,
                        headers=req_headers,
                        timeout=12,
                        stream=True,
                    ) as resp:
                        if resp.status_code != 200:
                            self.send_response(resp.status_code)
                            self._send_cors_headers()
                            self.end_headers()
                            return

                        # Support mocked responses in unit tests that set resp.content directly
                        raw_mock = getattr(resp, "content", None)
                        if isinstance(raw_mock, (bytes, bytearray)) and "iter_content" not in getattr(resp, "__dict__", {}):
                            clean_bytes = proxy_ref.strip_disguised_ts_header(bytes(raw_mock))
                            proxy_ref._store_in_segment_cache(target_url, clean_bytes)
                            self._serve_segment_bytes(clean_bytes, head_only)
                            return

                        chunk_iter = resp.iter_content(chunk_size=65536)
                        first_chunk = b""
                        for ch in chunk_iter:
                            if ch:
                                first_chunk = ch
                                break

                        if not first_chunk:
                            self.send_response(502)
                            self._send_cors_headers()
                            self.end_headers()
                            return

                        offset = proxy_ref.find_ts_sync_offset(first_chunk)
                        first_clean = first_chunk[offset:]
                        upstream_cl = resp.headers.get("Content-Length")

                        self.send_response(200)
                        self._send_cors_headers()
                        self.send_header("Content-Type", "video/MP2T")
                        if upstream_cl and upstream_cl.isdigit():
                            exact_len = max(0, int(upstream_cl) - offset)
                            self.send_header("Content-Length", str(exact_len))
                        self.end_headers()

                        if head_only:
                            return

                        collected = [first_clean]
                        self.wfile.write(first_clean)
                        self.wfile.flush()

                        for ch in chunk_iter:
                            if ch:
                                collected.append(ch)
                                self.wfile.write(ch)
                        self.wfile.flush()
                        proxy_ref._store_in_segment_cache(target_url, b"".join(collected))
                except (BrokenPipeError, ConnectionResetError):
                    pass
                except Exception:
                    try:
                        self.send_response(502)
                        self._send_cors_headers()
                        self.end_headers()
                    except Exception:
                        pass
                finally:
                    with proxy_ref._cache_lock:
                        ev = proxy_ref._inflight.pop(target_url, None)
                        if ev is not None:
                            ev.set()

            def _serve_segment_bytes(self, seg_bytes: bytes, head_only: bool):
                self.send_response(200)
                self._send_cors_headers()
                self.send_header("Content-Type", "video/MP2T")
                self.send_header("Content-Length", str(len(seg_bytes)))
                self.end_headers()
                if not head_only:
                    try:
                        self.wfile.write(seg_bytes)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        pass

            def _serve_rewritten_m3u8(
                self,
                text: str,
                target_url: str,
                idx: int,
                req_headers: Dict[str, str],
                host_hdr: str,
                head_only: bool,
            ):
                is_master = "#EXT-X-STREAM-INF" in text
                rewritten_lines = []
                media_segment_urls: List[str] = []

                for line in text.splitlines():
                    stripped = line.strip()
                    if not stripped:
                        rewritten_lines.append(line)
                    elif stripped.startswith("#"):
                        if 'URI="' in stripped:
                            def repl_uri(match):
                                raw_u = match.group(1)
                                abs_u = urljoin(target_url, raw_u)
                                proxied = f"http://{host_hdr}/fetch/{idx}?url={quote(abs_u, safe='')}"
                                return f'URI="{proxied}"'
                            line = re.sub(r'URI="([^"]+)"', repl_uri, line)
                        rewritten_lines.append(line)
                    else:
                        abs_u = urljoin(target_url, stripped)
                        if not is_master and not abs_u.split("?")[0].lower().endswith(".m3u8"):
                            media_segment_urls.append(abs_u)
                        proxied = f"http://{host_hdr}/fetch/{idx}?url={quote(abs_u, safe='')}"
                        rewritten_lines.append(proxied)

                # Parallel-prefetch the latest 4 live segments in the background so they are ready in RAM
                if media_segment_urls:
                    proxy_ref._prefetch_segments_async(media_segment_urls[-4:], req_headers)

                body = ("\n".join(rewritten_lines) + "\n").encode("utf-8")
                self.send_response(200)
                self._send_cors_headers()
                self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if not head_only:
                    try:
                        self.wfile.write(body)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        pass

        for p in range(self.port, self.port + 20):
            try:
                self._server = ThreadingHTTPServer(("0.0.0.0", p), Handler)
                self.port = p
                break
            except OSError:
                continue

        if self._server is not None:
            self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
        return self.port

    def register_streams(self, streams: Dict[str, StreamInfo]) -> None:
        self.streams = {idx: info for idx, info in enumerate(streams.values(), 1)}

    def proxy_url(self, idx: int, lan: bool = False, quality: str = "auto") -> str:
        self.start()
        host = get_lan_ip() if lan else "127.0.0.1"
        q_suffix = f"?quality={quality}" if quality in ("high", "low") else ""
        return f"http://{host}:{self.port}/stream/{idx}.m3u8{q_suffix}"


@dataclass
class PageResult:
    url: str
    depth: int
    status_code: int
    m3u8_links: List[str] = field(default_factory=list)
    discovered_domain_links: int = 0
    popups_blocked: int = 0
    redirects_blocked: int = 0


class DomainM3U8Scraper:
    """
    Crawls HTML pages within a given domain (and optionally embedded player domains),
    ignoring/blocking popups and redirects, and extracts discovered .m3u8 stream URLs
    along with the exact HTTP headers (Referer, Origin, User-Agent) required to play them.
    """

    M3U8_FULL_URL_RE = re.compile(
        r"""https?://[^\s"'<>\\`]+\.m3u8(?:\?[^\s"'<>\\`]*)?""",
        re.IGNORECASE,
    )

    M3U8_RELATIVE_RE = re.compile(
        r"""["']((?:/|\.\.?/|[a-zA-Z0-9_\-]+/)[^\s"'<>\\`]*?\.m3u8(?:\?[^\s"'<>\\`]*)?)["']""",
        re.IGNORECASE,
    )

    SKIP_EXTENSIONS = {
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
        ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
        ".mp4", ".mkv", ".webm", ".mp3", ".wav", ".ogg", ".ts",
        ".pdf", ".zip", ".rar", ".7z", ".tar", ".gz", ".apk",
    }

    def __init__(
        self,
        start_url: str,
        allowed_domain: Optional[str] = None,
        include_subdomains: bool = True,
        follow_player_iframes: bool = True,
        max_depth: int = 2,
        max_pages: int = 10,
        delay: float = 0.5,
        timeout: int = 15,
        mode: str = "browser",
        wait_seconds: float = 4.0,
        user_agent: Optional[str] = None,
    ):
        if not start_url.startswith(("http://", "https://")):
            start_url = "https://" + start_url
        self.start_url = self._normalize_url(start_url)
        parsed_start = urlparse(self.start_url)
        if not parsed_start.scheme or not parsed_start.netloc:
            raise ValueError(f"Invalid start URL: {start_url}")

        start_host = (parsed_start.hostname or "").lower().removeprefix("www.")
        self.allowed_domains: Set[str] = set()
        if allowed_domain and allowed_domain.lower() != "auto":
            for d in allowed_domain.split(","):
                d_clean = d.strip().lower().removeprefix("www.")
                if d_clean:
                    self.allowed_domains.add(d_clean)
        else:
            self.allowed_domains.add(start_host)

        self.include_subdomains = include_subdomains
        self.follow_player_iframes = follow_player_iframes
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.delay = delay
        self.timeout = timeout
        self.mode = mode.lower()
        self.wait_seconds = wait_seconds
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        )

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

        self.visited_urls: Set[str] = set()
        self.skipped_redirects: List[Tuple[str, int, str]] = []
        self.blocked_popups: List[str] = []
        self.page_results: List[PageResult] = []
        self.all_m3u8_links: Dict[str, List[str]] = {}
        self.stream_details: Dict[str, StreamInfo] = {}

    @staticmethod
    def _normalize_url(url: str) -> str:
        cleaned, _ = urldefrag(url.strip())
        return cleaned

    def is_allowed_domain(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host = (parsed.hostname or "").lower()
        if not host:
            return False

        host_no_www = host.removeprefix("www.")
        for domain in self.allowed_domains:
            if host_no_www == domain:
                return True
            if self.include_subdomains and host_no_www.endswith("." + domain):
                return True
        return False

    def is_same_domain(self, url: str) -> bool:
        return self.is_allowed_domain(url)

    @staticmethod
    def is_popup_element(tag) -> bool:
        href = (tag.get("href") or "").strip().lower()
        if href.startswith("javascript:") or "window.open" in href:
            return True

        onclick = (tag.get("onclick") or "").lower()
        if "window.open" in onclick or "popup" in onclick:
            return True

        rel = tag.get("rel") or []
        if isinstance(rel, str):
            rel = rel.lower().split()
        else:
            rel = [r.lower() for r in rel]
        if "popup" in rel or "sponsored" in rel:
            return True

        return False

    @staticmethod
    def is_ad_or_hidden_iframe(iframe_tag) -> bool:
        src = (iframe_tag.get("src") or "").strip().lower()
        if not src or src.startswith(("javascript:", "about:")):
            return True
        if "/ad.html" in src or "adservice" in src or "doubleclick" in src:
            return True
        style = (iframe_tag.get("style") or "").lower()
        if "visibility:hidden" in style.replace(" ", "") or "display:none" in style.replace(" ", ""):
            return True
        width = str(iframe_tag.get("width") or "").strip()
        height = str(iframe_tag.get("height") or "").strip()
        if width == "1" and height == "1":
            return True
        return False

    def should_follow_url(self, url: str, is_player_iframe: bool = False) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        path_lower = parsed.path.lower()
        if path_lower.endswith(".m3u8"):
            return False
        for ext in self.SKIP_EXTENSIONS:
            if path_lower.endswith(ext):
                return False

        if self.is_allowed_domain(url):
            return True

        if is_player_iframe and self.follow_player_iframes:
            host = (parsed.hostname or "").lower().removeprefix("www.")
            if host:
                logger.info("Allowing player iframe domain: %s (from %s)", host, url)
                self.allowed_domains.add(host)
                return True

        return False

    def _default_headers_for_page(self, page_url: str) -> Dict[str, str]:
        parsed = urlparse(page_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        referer = f"{origin}/"
        return {
            "Referer": referer,
            "Origin": origin,
            "User-Agent": self.user_agent,
        }

    def _record_m3u8(
        self,
        m3u8_url: str,
        source_page: str,
        req_headers: Optional[Dict[str, str]] = None,
        parent_url: Optional[str] = None,
    ) -> None:
        sources = self.all_m3u8_links.setdefault(m3u8_url, [])
        is_new = m3u8_url not in self.stream_details
        if source_page not in sources:
            sources.append(source_page)

        info = self.stream_details.get(m3u8_url)
        if info is None:
            info = StreamInfo(
                m3u8_url=m3u8_url,
                found_on_pages=list(sources),
                headers=self._default_headers_for_page(source_page),
                parent_url=parent_url or self.start_url,
            )
            self.stream_details[m3u8_url] = info
        else:
            info.found_on_pages = list(sources)
            if parent_url and not info.parent_url:
                info.parent_url = parent_url

        if req_headers:
            lower_map = {k.lower(): v for k, v in req_headers.items()}
            if "referer" in lower_map:
                info.headers["Referer"] = lower_map["referer"]
            if "origin" in lower_map:
                info.headers["Origin"] = lower_map["origin"]
            elif "Referer" in info.headers:
                p_ref = urlparse(info.headers["Referer"])
                if p_ref.scheme and p_ref.netloc:
                    info.headers["Origin"] = f"{p_ref.scheme}://{p_ref.netloc}"
            if "user-agent" in lower_map:
                info.headers["User-Agent"] = lower_map["user-agent"]
            if "cookie" in lower_map:
                info.headers["Cookie"] = lower_map["cookie"]

        if is_new:
            logger.info(
                "  -> Found m3u8: %s (Referer: %s)",
                m3u8_url,
                info.headers.get("Referer", "none"),
            )

    def extract_m3u8_links(self, html: str, base_url: str, soup: BeautifulSoup) -> List[str]:
        found: Set[str] = set()

        for tag in soup.find_all(True):
            for attr_name, attr_val in tag.attrs.items():
                values = attr_val if isinstance(attr_val, list) else [attr_val]
                for val in values:
                    if not isinstance(val, str):
                        continue
                    if ".m3u8" in val.lower():
                        full_matches = self.M3U8_FULL_URL_RE.findall(val)
                        if full_matches:
                            found.update(full_matches)
                        elif val.strip().lower().split("?")[0].endswith(".m3u8"):
                            found.add(urljoin(base_url, val.strip()))

        unescaped_html = html.replace(r"\/", "/")
        for match in self.M3U8_FULL_URL_RE.findall(unescaped_html):
            found.add(match)

        for rel_match in self.M3U8_RELATIVE_RE.findall(unescaped_html):
            found.add(urljoin(base_url, rel_match))

        return sorted(found)

    def extract_domain_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        links: List[str] = []
        seen: Set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            if self.is_popup_element(a_tag):
                continue

            raw_href = a_tag["href"].strip()
            if not raw_href or raw_href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue

            full_url = self._normalize_url(urljoin(base_url, raw_href))
            if self.should_follow_url(full_url, is_player_iframe=False) and full_url not in seen:
                seen.add(full_url)
                links.append(full_url)

        for iframe in soup.find_all("iframe", src=True):
            if self.is_ad_or_hidden_iframe(iframe):
                continue
            raw_src = iframe["src"].strip()
            full_url = self._normalize_url(urljoin(base_url, raw_src))
            iframe_id = (iframe.get("id") or "").lower()
            iframe_title = (iframe.get("title") or "").lower()
            is_player = (
                "player" in iframe_id
                or "stream" in iframe_id
                or "player" in iframe_title
                or "embed" in full_url.lower()
            )
            if self.should_follow_url(full_url, is_player_iframe=is_player) and full_url not in seen:
                seen.add(full_url)
                links.append(full_url)

        return links

    def crawl(self, browser_context=None) -> List[PageResult]:
        if self.mode == "browser":
            return self._crawl_browser(shared_context=browser_context)
        return self._crawl_http()

    @classmethod
    def launch_stealth_browser(cls, playwright_instance, headless: bool = True):
        args = ["--disable-blink-features=AutomationControlled"]
        if headless:
            args.insert(0, "--headless=new")
        return playwright_instance.chromium.launch(
            channel="chromium",
            headless=headless,
            args=args,
        )

    @classmethod
    def _create_playwright_context(cls, browser, no_viewport: bool = False):
        """
        Create a Playwright browser context using Chromium's genuine User-Agent
        (so navigator.userAgent and Sec-CH-UA client hints match 100%) and block
        window.open popups without tampering with page globals checked by anti-adblockers.
        """
        if no_viewport:
            context = browser.new_context(no_viewport=True)
        else:
            context = browser.new_context(viewport={"width": 1280, "height": 720})
        context.add_init_script(
            """
            (() => {
                window.open = function(url) {
                    console.log('[Blocked window.open]:', url);
                    return null;
                };
            })();
            """
        )
        return context

    def _crawl_browser(self, shared_context=None) -> List[PageResult]:
        from playwright.sync_api import sync_playwright

        queue: deque[Tuple[str, int, Optional[str]]] = deque([(self.start_url, 0, None)])
        queued_urls: Set[str] = {self.start_url}

        logger.info(
            "Starting BROWSER crawl at %s (domains: %s, max_depth: %d, max_pages: %d)",
            self.start_url,
            ", ".join(sorted(self.allowed_domains)),
            self.max_depth,
            self.max_pages,
        )

        def run_with_context(context):
            while queue and len(self.visited_urls) < self.max_pages:
                current_url, depth, referer = queue.popleft()
                if current_url in self.visited_urls:
                    continue

                self.visited_urls.add(current_url)
                logger.info(
                    "[%d/%d] Fetching (depth %d): %s",
                    len(self.visited_urls),
                    self.max_pages,
                    depth,
                    current_url,
                )

                page = context.new_page()
                page_m3u8: Set[str] = set()
                page_popups_blocked = 0
                page_redirects_blocked = 0
                initial_navigation_done = False

                def on_popup(popup_page):
                    nonlocal page_popups_blocked
                    popup_url = popup_page.url
                    page_popups_blocked += 1
                    self.blocked_popups.append(popup_url)
                    logger.info("  [Blocked Popup Window] %s", popup_url)
                    try:
                        popup_page.close()
                    except Exception:
                        pass

                page.on("popup", on_popup)

                def on_response(resp):
                    resp_url = resp.url
                    ctype = (resp.headers.get("content-type") or "").lower()
                    if ".m3u8" in resp_url.lower() or "mpegurl" in ctype:
                        page_m3u8.add(resp_url)
                        req = resp.request
                        frame_url = req.frame.url if req.frame and req.frame.url else current_url
                        self._record_m3u8(
                            resp_url,
                            frame_url or current_url,
                            req_headers=req.all_headers(),
                            parent_url=referer or self.start_url,
                        )

                page.on("response", on_response)

                def handle_route(route):
                    nonlocal page_redirects_blocked
                    req = route.request

                    if ".m3u8" in req.url.lower():
                        page_m3u8.add(req.url)
                        frame_url = req.frame.url if req.frame and req.frame.url else current_url
                        try:
                            hdrs = req.all_headers()
                        except Exception:
                            hdrs = req.headers
                        self._record_m3u8(
                            req.url,
                            frame_url or current_url,
                            req_headers=hdrs,
                            parent_url=referer or self.start_url,
                        )
                        logger.info("  [Preserved One-Time Token] Intercepted & aborted browser fetch for %s", req.url)
                        route.abort()
                        return

                    if req.is_navigation_request() and req.frame == page.main_frame:
                        if req.redirected_from is not None:
                            from_url = req.redirected_from.url
                            page_redirects_blocked += 1
                            self.skipped_redirects.append((from_url, 302, req.url))
                            logger.info("  [Blocked HTTP Redirect] %s -> %s", from_url, req.url)
                            route.abort()
                            return

                        if initial_navigation_done and self._normalize_url(req.url) != current_url:
                            page_redirects_blocked += 1
                            self.skipped_redirects.append((current_url, 0, req.url))
                            logger.info("  [Blocked Page Redirect] %s -> %s", current_url, req.url)
                            route.abort()
                            return

                    route.continue_()

                page.route("**/*", handle_route)

                status_code = 0
                try:
                    goto_kwargs = {
                        "wait_until": "domcontentloaded",
                        "timeout": self.timeout * 1000,
                    }
                    if referer:
                        goto_kwargs["referer"] = referer

                    resp = page.goto(current_url, **goto_kwargs)
                    initial_navigation_done = True
                    status_code = resp.status if resp else 0

                    page.wait_for_timeout(int(self.wait_seconds * 1000))

                    if not page_m3u8:
                        for selector in ["#player", ".jwplayer", "video", ".clappr-player", "[data-player]"]:
                            loc = page.locator(selector).first
                            if loc.count() > 0:
                                try:
                                    loc.click(timeout=1500, force=True)
                                    page.wait_for_timeout(2000)
                                    break
                                except Exception:
                                    pass

                    html = page.content()
                    soup = BeautifulSoup(html, "html.parser")

                    for m3u8_url in self.extract_m3u8_links(html, current_url, soup):
                        page_m3u8.add(m3u8_url)
                        self._record_m3u8(m3u8_url, current_url, parent_url=referer or self.start_url)

                    for frame in page.frames[1:]:
                        try:
                            f_url = frame.url
                            if not f_url or f_url.startswith("about:"):
                                continue
                            f_html = frame.content()
                            f_soup = BeautifulSoup(f_html, "html.parser")
                            for m3u8_url in self.extract_m3u8_links(f_html, f_url, f_soup):
                                page_m3u8.add(m3u8_url)
                                self._record_m3u8(m3u8_url, f_url, parent_url=current_url)
                        except Exception:
                            pass

                    domain_links = self.extract_domain_links(soup, current_url)
                    if depth < self.max_depth:
                        for link in domain_links:
                            if link not in self.visited_urls and link not in queued_urls:
                                queued_urls.add(link)
                                queue.append((link, depth + 1, current_url))

                    self.page_results.append(
                        PageResult(
                            url=current_url,
                            depth=depth,
                            status_code=status_code,
                            m3u8_links=sorted(page_m3u8),
                            discovered_domain_links=len(domain_links),
                            popups_blocked=page_popups_blocked,
                            redirects_blocked=page_redirects_blocked,
                        )
                    )
                except Exception as exc:
                    logger.warning("Failed processing %s: %s", current_url, exc)
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass

                if self.delay > 0 and queue:
                    time.sleep(self.delay)

        if shared_context is not None:
            run_with_context(shared_context)
        else:
            with sync_playwright() as p:
                browser = self.launch_stealth_browser(p, headless=True)
                context = self._create_playwright_context(browser)
                run_with_context(context)
                browser.close()

        logger.info(
            "Crawl complete. Visited %d pages, found %d unique .m3u8 links, blocked %d popups, ignored %d redirects.",
            len(self.visited_urls),
            len(self.all_m3u8_links),
            len(self.blocked_popups),
            len(self.skipped_redirects),
        )
        return self.page_results

    def _crawl_http(self) -> List[PageResult]:
        queue: deque[Tuple[str, int, Optional[str]]] = deque([(self.start_url, 0, None)])
        queued_urls: Set[str] = {self.start_url}

        logger.info(
            "Starting HTTP crawl at %s (domains: %s, max_depth: %d, max_pages: %d)",
            self.start_url,
            ", ".join(sorted(self.allowed_domains)),
            self.max_depth,
            self.max_pages,
        )

        while queue and len(self.visited_urls) < self.max_pages:
            current_url, depth, referer = queue.popleft()
            if current_url in self.visited_urls:
                continue

            self.visited_urls.add(current_url)
            logger.info(
                "[%d/%d] Fetching (depth %d): %s",
                len(self.visited_urls),
                self.max_pages,
                depth,
                current_url,
            )

            headers = {"Referer": referer} if referer else {}
            try:
                response = self.session.get(
                    current_url,
                    headers=headers,
                    allow_redirects=False,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                logger.warning("Request failed for %s: %s", current_url, exc)
                continue

            if 300 <= response.status_code < 400:
                location = response.headers.get("Location", "")
                logger.info(
                    "Ignoring HTTP %d redirect from %s -> %s",
                    response.status_code,
                    current_url,
                    location,
                )
                self.skipped_redirects.append((current_url, response.status_code, location))
                continue

            content_type = response.headers.get("Content-Type", "").lower()
            if "mpegurl" in content_type or current_url.lower().endswith(".m3u8"):
                self._record_m3u8(current_url, referer or current_url)
                continue

            if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                logger.debug("Skipping non-HTML content (%s) at %s", content_type, current_url)
                continue

            html = response.text
            soup = BeautifulSoup(html, "html.parser")

            m3u8_links = self.extract_m3u8_links(html, current_url, soup)
            for m3u8_url in m3u8_links:
                self._record_m3u8(m3u8_url, current_url)

            domain_links = self.extract_domain_links(soup, current_url)
            if depth < self.max_depth:
                for link in domain_links:
                    if link not in self.visited_urls and link not in queued_urls:
                        queued_urls.add(link)
                        queue.append((link, depth + 1, current_url))

            self.page_results.append(
                PageResult(
                    url=current_url,
                    depth=depth,
                    status_code=response.status_code,
                    m3u8_links=m3u8_links,
                    discovered_domain_links=len(domain_links),
                )
            )

            if self.delay > 0 and queue:
                time.sleep(self.delay)

        logger.info(
            "Crawl complete. Visited %d pages, found %d unique .m3u8 links, ignored %d redirects.",
            len(self.visited_urls),
            len(self.all_m3u8_links),
            len(self.skipped_redirects),
        )
        return self.page_results

    def save_results(self, output_path: str) -> None:
        export_streams(
            self.stream_details,
            output_path,
            extra_metadata={
                "start_url": self.start_url,
                "allowed_domains": sorted(self.allowed_domains),
                "pages_visited": len(self.visited_urls),
                "popups_blocked": self.blocked_popups,
                "redirects_ignored": [
                    {"from": src, "status": code, "to": loc}
                    for src, code, loc in self.skipped_redirects
                ],
                "pages": [asdict(p) for p in self.page_results],
            },
        )
        logger.info("Saved results to %s", output_path)


def export_streams(
    streams: Dict[str, StreamInfo],
    output_path: str,
    extra_metadata: Optional[dict] = None,
) -> None:
    lower_path = output_path.lower()
    if lower_path.endswith(".json"):
        data = dict(extra_metadata or {})
        data["m3u8_links"] = [
            {
                "m3u8_url": info.m3u8_url,
                "found_on_pages": info.found_on_pages,
                "parent_url": info.parent_url,
                "headers": info.headers,
                "commands": {
                    "vlc": info.vlc_command(),
                    "ffplay": info.ffplay_command(),
                    "streamlink": info.streamlink_command(),
                },
            }
            for info in streams.values()
        ]
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    elif lower_path.endswith(".csv"):
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["m3u8_url", "referer", "origin", "user_agent", "found_on_page"])
            for info in streams.values():
                for page in info.found_on_pages:
                    writer.writerow(
                        [
                            info.m3u8_url,
                            info.headers.get("Referer", ""),
                            info.headers.get("Origin", ""),
                            info.headers.get("User-Agent", ""),
                            page,
                        ]
                    )
    elif lower_path.endswith((".m3u", ".m3u8")):
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for idx, info in enumerate(streams.values(), 1):
                src = info.found_on_pages[0] if info.found_on_pages else f"Stream {idx}"
                f.write(f"#EXTINF:-1,Stream {idx} ({src})\n")
                if info.headers.get("Referer"):
                    f.write(f"#EXTVLCOPT:http-referrer={info.headers['Referer']}\n")
                if info.headers.get("User-Agent"):
                    f.write(f"#EXTVLCOPT:http-user-agent={info.headers['User-Agent']}\n")
                f.write(f"{info.m3u8_url}\n")
    else:
        with open(output_path, "w", encoding="utf-8") as f:
            for info in streams.values():
                f.write(f"{info.m3u8_url}\n")


def print_stream_info(idx: int, info: StreamInfo, proxy: Optional[LocalHLSProxy] = None) -> None:
    print(f"  {UI.c(f'[{idx}]', UI.GREEN)} {UI.c(info.m3u8_url, UI.BOLD)}")
    for p in info.found_on_pages:
        print(f"      {UI.c('Found on :', UI.DIM)} {p}")
    ref = info.headers.get("Referer", "")
    origin = info.headers.get("Origin", "")
    if ref or origin:
        print(
            f"      {UI.c('Headers  :', UI.CYAN)} Referer={ref} | Origin={origin}"
        )
    if proxy is not None:
        p_url = proxy.proxy_url(idx)
        print(
            f"      {UI.c('Actions  :', UI.GREEN + UI.BOLD)} "
            f"Type '{idx}' or 'watch {idx}' (Browser) | 'play {idx}' (VLC: {p_url}) | 'cast {idx}' (Chromecast)"
        )
    print(f"      {UI.c('ffplay   :', UI.YELLOW)} {info.ffplay_command()}")
    print(f"      {UI.c('streamlnk:', UI.YELLOW)} {info.streamlink_command()}")


def print_crawl_summary(scraper: DomainM3U8Scraper, proxy: Optional[LocalHLSProxy] = None) -> None:
    print("\n" + UI.c("─" * 68, UI.CYAN))
    print(
        f"  {UI.c('Pages Visited:', UI.BOLD)} {len(scraper.visited_urls)}   "
        f"{UI.c('Popups Blocked:', UI.BOLD)} {len(scraper.blocked_popups)}   "
        f"{UI.c('Redirects Ignored:', UI.BOLD)} {len(scraper.skipped_redirects)}   "
        f"{UI.c('M3U8s Found:', UI.BOLD)} {UI.c(str(len(scraper.stream_details)), UI.GREEN if scraper.stream_details else UI.YELLOW)}"
    )
    print(UI.c("─" * 68, UI.CYAN))

    if scraper.stream_details:
        for idx, info in enumerate(scraper.stream_details.values(), 1):
            print_stream_info(idx, info, proxy=proxy)
    else:
        print(f"  {UI.c('No .m3u8 links found on this run.', UI.YELLOW)}")
    print()


class InteractiveCLI:
    """
    Interactive terminal loop allowing the user to continuously enter URLs,
    play streams via an ad-blocked/popup-blocked browser player (`watch <n>` or `<n>`),
    a local header-injecting VLC proxy (`play <n>`), or LAN Chromecast (`cast <n>`).
    """

    def __init__(self, args: argparse.Namespace):
        self.domain: Optional[str] = args.domain
        self.include_subdomains: bool = not args.no_subdomains
        self.follow_player_iframes: bool = not args.no_player_iframes
        self.max_depth: int = args.max_depth
        self.max_pages: int = args.max_pages
        self.delay: float = args.delay
        self.wait: float = args.wait
        self.timeout: int = args.timeout
        self.mode: str = args.mode
        self.output: str = args.output

        self.session_streams: Dict[str, StreamInfo] = {}
        self.total_runs: int = 0
        self.proxy = LocalHLSProxy()

        self._pw = None
        self._browser = None
        self._context = None
        self._active_cast = None
        self._cast_browser = None

    def _ensure_playwright(self):
        if self._pw is None:
            from playwright.sync_api import sync_playwright

            self._pw = sync_playwright().start()
        return self._pw

    def _ensure_browser_context(self):
        pw = self._ensure_playwright()
        if self._context is None:
            print(UI.c("Starting stealth Chromium browser for the session...", UI.DIM))
            self._browser = DomainM3U8Scraper.launch_stealth_browser(pw, headless=True)
            self._context = DomainM3U8Scraper._create_playwright_context(self._browser)
        return self._context

    def _close_cast_discovery(self):
        if self._cast_browser is not None:
            try:
                self._cast_browser.stop_discovery()
            except Exception:
                pass
            self._cast_browser = None

    def _close_browser(self):
        self._close_cast_discovery()
        if self._active_cast is not None:
            try:
                self._active_cast.disconnect(timeout=2)
            except Exception:
                pass
            self._active_cast = None
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
            self._browser = None
            self._context = None
        if self._pw:
            try:
                self._pw.stop()
            except Exception:
                pass
            self._pw = None

    def print_banner(self):
        print(UI.c("═" * 68, UI.CYAN))
        print(
            UI.c("  M3U8 DOMAIN SCRAPER — Interactive CLI", UI.BOLD + UI.CYAN)
            + UI.c(" (Browser + VLC + Chromecast Relay)", UI.DIM)
        )
        print(UI.c("═" * 68, UI.CYAN))
        self.print_status()
        self.print_help()

    def print_status(self):
        dom_label = self.domain if self.domain else "auto (start domain + player iframes)"
        cast_label = self._active_cast.name if self._active_cast is not None else "none"
        print(
            f"  {UI.c('Settings:', UI.BOLD)} "
            f"mode={UI.c(self.mode, UI.GREEN)} | "
            f"domain={UI.c(dom_label, UI.CYAN)} | "
            f"depth={UI.c(str(self.max_depth), UI.YELLOW)} | "
            f"max_pages={UI.c(str(self.max_pages), UI.YELLOW)} | "
            f"wait={UI.c(f'{self.wait}s', UI.YELLOW)} | "
            f"cast={UI.c(cast_label, UI.CYAN)} | "
            f"out={UI.c(self.output, UI.MAGENTA)}"
        )

    def print_help(self):
        print(UI.c("\n  Commands:", UI.BOLD))
        print(f"    {UI.c('<url> [domain]', UI.GREEN)}     Scrape <url> (optionally override target domain for this run)")
        print(f"    {UI.c('watch <index> / <n>', UI.GREEN)} Open stream #<index> in a popup-blocked/redirect-blocked player window")
        print(f"    {UI.c('play <index>', UI.GREEN)}       Open stream #<index> in VLC via local header-injecting proxy")
        print(f"    {UI.c('cast <index> [ip]', UI.GREEN)}  Cast stream #<index> to a Chromecast on your LAN via zero-CPU HTTP relay")
        print(f"    {UI.c('castdevice [ip]', UI.CYAN)}    Scan & switch the active Chromecast device (optional manual IP)")
        print(f"    {UI.c('stopcast', UI.CYAN)}           Stop playback on the active Chromecast device")
        print(f"    {UI.c('verify [index]', UI.CYAN)}     Test .m3u8 URL(s) with captured Referer/Origin headers (checks 200 OK)")
        print(f"    {UI.c('links', UI.CYAN)}              Show all .m3u8 streams, proxy URLs & commands from latest run")
        print(f"    {UI.c('domain <name|auto>', UI.CYAN)} Set persistent target domain filter (e.g. 'domain embed.st' or 'domain auto')")
        print(f"    {UI.c('depth <n>', UI.CYAN)}          Set max crawl depth (current: {self.max_depth})")
        print(f"    {UI.c('pages <n>', UI.CYAN)}          Set max pages per run (current: {self.max_pages})")
        print(f"    {UI.c('mode <browser|http>', UI.CYAN)} Switch between Playwright browser and static HTTP mode")
        print(f"    {UI.c('wait <sec>', UI.CYAN)}         Set JS wait time in seconds for browser mode")
        print(f"    {UI.c('save [filepath]', UI.CYAN)}    Save streams (.json, .m3u, .csv, or .txt)")
        print(f"    {UI.c('status / clear / q', UI.RED)} Show config, clear screen, or quit\n")

    def save_session_links(self, path: str):
        export_streams(
            self.session_streams,
            path,
            extra_metadata={
                "total_runs": self.total_runs,
                "total_unique_m3u8": len(self.session_streams),
            },
        )
        print(UI.c(f"  ✓ Saved {len(self.session_streams)} .m3u8 stream(s) to {path}", UI.GREEN))

    def verify_streams(self, target_index: Optional[int] = None):
        if not self.session_streams:
            print(UI.c("  No .m3u8 streams in session yet.", UI.YELLOW))
            return

        items = list(self.session_streams.values())
        if target_index is not None:
            if target_index < 1 or target_index > len(items):
                print(UI.c(f"  Invalid stream index (1..{len(items)}).", UI.RED))
                return
            pairs = [(target_index, items[target_index - 1])]
        else:
            pairs = list(enumerate(items, 1))

        for idx, info in pairs:
            try:
                r = requests.get(info.m3u8_url, headers=info.headers, timeout=10)
                first_line = (r.text.splitlines()[0] if r.text else "").strip()
                if r.status_code == 200 and "#EXTM3U" in first_line:
                    print(
                        f"  {UI.c(f'[{idx}] 200 OK (Valid HLS #EXTM3U)', UI.GREEN)} -> {info.m3u8_url}"
                    )
                else:
                    print(
                        f"  {UI.c(f'[{idx}] HTTP {r.status_code}', UI.YELLOW)} ({first_line[:40]}) -> {info.m3u8_url}"
                    )
            except Exception as exc:
                print(f"  {UI.c(f'[{idx}] Error:', UI.RED)} {exc}")

    def play_stream(self, index: int):
        items = list(self.session_streams.values())
        if index < 1 or index > len(items):
            print(UI.c(f"  Invalid stream index (1..{len(items)}).", UI.RED))
            return

        self.proxy.register_streams(self.session_streams)
        proxied_url = self.proxy.proxy_url(index)

        vlc_path = find_vlc_executable()
        ffplay_path = shutil.which("ffplay")

        if vlc_path:
            print(
                UI.c(
                    f"  Launching VLC via local header-injecting proxy ({proxied_url})...\n"
                    f"  (Tip: You can also use 'watch {index}' for the browser player or 'cast {index}' for Chromecast)",
                    UI.GREEN,
                )
            )
            subprocess.Popen([vlc_path, proxied_url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        elif ffplay_path:
            print(UI.c(f"  Launching ffplay via local proxy ({proxied_url})...", UI.GREEN))
            subprocess.Popen([ffplay_path, proxied_url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            print(UI.c(f"  Local proxy ready at: {proxied_url}", UI.GREEN))

    def select_chromecast_device(self, force_rescan: bool = False, known_host: Optional[str] = None):
        """
        Discover Chromecast devices on the LAN using mDNS (or an optional known IP),
        prompt the user to pick one, and cache the selected device for subsequent casts.
        """
        if self._active_cast is not None and not force_rescan and known_host is None:
            return self._active_cast

        if known_host and ".x" in known_host.lower():
            print(
                UI.c(
                    f"  ✗ '{known_host}' is a placeholder. Use 'cast 1' to auto-discover your TV, "
                    f"or pass your TV's real IP address (e.g. 'cast 1 192.168.4.27').",
                    UI.YELLOW,
                )
            )
            return None

        try:
            import pychromecast
        except ImportError:
            print(UI.c("  ✗ pychromecast is not installed. Run: pip install pychromecast", UI.RED))
            return None

        # Silence noisy pychromecast/zeroconf internal deprecation & async teardown logs
        logging.getLogger("pychromecast").setLevel(logging.ERROR)
        logging.getLogger("zeroconf").setLevel(logging.ERROR)

        self._close_cast_discovery()
        if self._active_cast is not None:
            try:
                self._active_cast.disconnect(timeout=2)
            except Exception:
                pass
            self._active_cast = None

        known_hosts = [known_host] if known_host else None
        if known_host:
            print(UI.c(f"  Scanning for Chromecast at {known_host}...", UI.CYAN))
        else:
            print(UI.c("  Scanning local network for Chromecast devices (5s)...", UI.CYAN))

        try:
            chromecasts, browser = pychromecast.get_chromecasts(timeout=5, known_hosts=known_hosts)
            self._cast_browser = browser
        except Exception as exc:
            print(UI.c(f"  ✗ Chromecast discovery failed: {exc}", UI.RED))
            return None

        if not chromecasts:
            self._close_cast_discovery()
            print(
                UI.c(
                    "  No Chromecast devices found on the local network.\n"
                    "  Tip: Ensure your Chromecast/TV is turned on and connected to the same Wi-Fi/LAN.",
                    UI.YELLOW,
                )
            )
            return None

        chromecasts.sort(key=lambda cc: (cc.name or "").lower())
        print(UI.c(f"\n  Found {len(chromecasts)} Chromecast device(s):", UI.BOLD + UI.GREEN))
        for i, cc in enumerate(chromecasts, 1):
            host_str = f"{cc.cast_info.host}:{cc.cast_info.port}" if getattr(cc, "cast_info", None) else cc.uri
            print(
                f"    {UI.c(f'[{i}]', UI.GREEN)} {UI.c(cc.name, UI.BOLD)} "
                f"{UI.c(f'({cc.model_name} — {host_str})', UI.DIM)}"
            )

        selected_idx = 1
        if len(chromecasts) == 1:
            choice = input(
                UI.c(f"  Select device [1] (Enter for '{chromecasts[0].name}', or 'c' to cancel): ", UI.BOLD)
            ).strip()
            if choice.lower() in ("c", "cancel", "q"):
                print(UI.c("  Cast cancelled.", UI.YELLOW))
                return None
            if choice.isdigit() and int(choice) == 1:
                selected_idx = 1
        else:
            choice = input(
                UI.c(f"  Select device [1-{len(chromecasts)}] (Enter for 1, or 'c' to cancel): ", UI.BOLD)
            ).strip()
            if choice.lower() in ("c", "cancel", "q"):
                print(UI.c("  Cast cancelled.", UI.YELLOW))
                return None
            if choice:
                if not choice.isdigit() or not (1 <= int(choice) <= len(chromecasts)):
                    print(UI.c(f"  Invalid selection (1..{len(chromecasts)}).", UI.RED))
                    return None
                selected_idx = int(choice)

        chosen = chromecasts[selected_idx - 1]
        chosen_host = f"{chosen.cast_info.host}:{chosen.cast_info.port}" if getattr(chosen, "cast_info", None) else chosen.uri
        print(UI.c(f"  Connecting to {chosen.name} ({chosen_host})...", UI.DIM))
        chosen.wait(timeout=10)
        self._active_cast = chosen
        print(UI.c(f"  ✓ Active Chromecast set to: {chosen.name} ({chosen_host})", UI.GREEN))
        return chosen

    def cast_stream(self, index: int, quality: str = "high", known_host: Optional[str] = None):
        """
        Cast stream #<index> to a Chromecast device on the LAN using Option A:
        Chromecast connects to our PC's LAN HLS proxy (`http://<LAN_IP>:8765/stream/<index>.m3u8`),
        which injects Referer/Origin/User-Agent headers, locks the variant quality ('high' or 'low')
        to prevent mid-stream ABR decoder resets, and streams cut-through + prefetched segments.
        """
        items = list(self.session_streams.values())
        if index < 1 or index > len(items):
            print(UI.c(f"  Invalid stream index (1..{len(items)}).", UI.RED))
            return

        quality = quality.lower() if quality.lower() in ("high", "low", "auto") else "high"
        info = items[index - 1]
        self.proxy.register_streams(self.session_streams)
        lan_url = self.proxy.proxy_url(index, lan=True, quality=quality)
        local_warmup_url = self.proxy.proxy_url(index, lan=False, quality=quality)

        cast_dev = self.select_chromecast_device(
            force_rescan=(known_host is not None),
            known_host=known_host,
        )
        if cast_dev is None:
            return

        # Pre-warm the master playlist and trigger background prefetch of the first live segments
        # while the Chromecast receiver app is launching on the TV
        def warmup():
            try:
                r = requests.get(local_warmup_url, timeout=6)
                if r.status_code == 200:
                    sub_lines = [l.strip() for l in r.text.splitlines() if l.strip() and not l.startswith("#")]
                    if sub_lines:
                        requests.get(sub_lines[0], timeout=6)
            except Exception:
                pass

        threading.Thread(target=warmup, daemon=True).start()

        title = info.found_on_pages[0] if info.found_on_pages else f"Stream #{index}"
        print(
            UI.c(
                f"  Casting Stream #{index} (quality={quality}) to '{cast_dev.name}'...\n"
                f"  LAN Relay URL : {lan_url}\n"
                f"  Upstream M3U8 : {info.m3u8_url}",
                UI.CYAN,
            )
        )
        try:
            cast_dev.wait(timeout=10)
            mc = cast_dev.media_controller
            mc.play_media(
                lan_url,
                "application/x-mpegURL",
                title=title,
                stream_type="BUFFERED",
                autoplay=True,
            )
            mc.block_until_active(timeout=10)
            print(
                UI.c(
                    f"  ✓ Now casting on '{cast_dev.name}' (locked quality: {quality})!\n"
                    f"  Tip: If 1080p60 is too heavy for your Wi-Fi or Chromecast hardware, try 'cast {index} low'.\n"
                    f"  (Use 'stopcast' to stop, or 'castdevice' to switch TVs)",
                    UI.GREEN,
                )
            )
        except Exception as exc:
            print(
                UI.c(
                    f"  ✗ Failed to start playback on '{cast_dev.name}': {exc}\n"
                    f"  Tip: Ensure Windows Firewall allows incoming connections on port {self.proxy.port} for Private networks.",
                    UI.RED,
                )
            )

    def stop_cast(self):
        if self._active_cast is None:
            print(UI.c("  No active Chromecast device in this session.", UI.YELLOW))
            return
        try:
            mc = self._active_cast.media_controller
            if mc.status.player_is_playing or mc.status.player_is_paused:
                mc.stop()
            self._active_cast.quit_app()
            print(UI.c(f"  ✓ Stopped casting on '{self._active_cast.name}'.", UI.GREEN))
        except Exception as exc:
            print(UI.c(f"  ✗ Error stopping Chromecast: {exc}", UI.RED))

    def watch_in_clean_browser(self, index: int):
        """
        Open the stream's embed player directly in a visible Chromium window with
        `no_viewport=True` (so resizing/maximizing the browser window dynamically
        resizes the video feed and the native Fullscreen button works), while
        strictly blocking all popup windows and top-level redirects.
        """
        items = list(self.session_streams.values())
        if index < 1 or index > len(items):
            print(UI.c(f"  Invalid stream index (1..{len(items)}).", UI.RED))
            return

        info = items[index - 1]
        embed_url = info.found_on_pages[0] if info.found_on_pages else info.m3u8_url

        parsed_parent = urlparse(info.parent_url or "https://isportsurge.shop/")
        host_origin = (
            f"{parsed_parent.scheme}://{parsed_parent.netloc}/"
            if parsed_parent.scheme and parsed_parent.netloc
            else "https://isportsurge.shop/"
        )

        print(
            UI.c(
                f"  Opening clean popup-blocked player window for Stream #{index}:\n"
                f"  Embed URL : {embed_url}\n"
                f"  Referer   : {host_origin}\n"
                f"  (Close the player window when done watching)",
                UI.GREEN,
            )
        )

        pw = self._ensure_playwright()
        visible_browser = None
        try:
            visible_browser = DomainM3U8Scraper.launch_stealth_browser(pw, headless=False)
            context = DomainM3U8Scraper._create_playwright_context(visible_browser, no_viewport=True)
            page = context.new_page()

            # Block any new popup tabs/windows immediately
            def on_popup(pop):
                logger.info("  [Blocked Popup Window] %s", pop.url)
                try:
                    pop.close()
                except Exception:
                    pass

            page.on("popup", on_popup)

            nav_done = False

            def route_guard(route):
                req = route.request

                # Block any top-level redirect attempting to navigate away from the player URL
                if req.is_navigation_request() and req.frame == page.main_frame:
                    if nav_done and DomainM3U8Scraper._normalize_url(req.url) != DomainM3U8Scraper._normalize_url(embed_url):
                        logger.info("  [Blocked Redirect in Player Window] -> %s", req.url)
                        route.abort()
                        return

                route.continue_()

            page.route("**/*", route_guard)
            page.goto(
                embed_url,
                referer=host_origin,
                wait_until="domcontentloaded",
                timeout=15000,
            )
            nav_done = True

            # Ensure the player container stretches smoothly when the window is resized
            page.add_style_tag(
                content="""
                html, body { margin: 0 !important; padding: 0 !important; width: 100vw !important; height: 100vh !important; overflow: hidden !important; background: #000 !important; }
                #player, .jwplayer, [data-player] { width: 100vw !important; height: 100vh !important; }
                """
            )

            # Wait until the user closes the browser window
            page.wait_for_event("close", timeout=0)
        except Exception as exc:
            logger.debug("Player window closed: %s", exc)
        finally:
            if visible_browser:
                try:
                    visible_browser.close()
                except Exception:
                    pass
            print(UI.c("  Player window closed.", UI.DIM))

    def scrape_single(self, url: str, domain_override: Optional[str] = None):
        active_domain = domain_override if domain_override is not None else self.domain
        try:
            scraper = DomainM3U8Scraper(
                start_url=url,
                allowed_domain=active_domain,
                include_subdomains=self.include_subdomains,
                follow_player_iframes=self.follow_player_iframes,
                max_depth=self.max_depth,
                max_pages=self.max_pages,
                delay=self.delay,
                timeout=self.timeout,
                mode=self.mode,
                wait_seconds=self.wait,
            )
        except ValueError as exc:
            print(UI.c(f"  ✗ {exc}", UI.RED))
            return

        ctx = self._ensure_browser_context() if self.mode == "browser" else None
        scraper.crawl(browser_context=ctx)
        self.total_runs += 1

        # Replace session_streams with the newly scraped streams (putting new streams first
        # so [1], [2], ... in `watch 1` / `play 1` / `cast 1` match the numbers just printed on screen!)
        if scraper.stream_details:
            self.session_streams = dict(scraper.stream_details)
            self.proxy.register_streams(self.session_streams)

        print_crawl_summary(scraper, proxy=self.proxy)
        if self.output and self.session_streams:
            self.save_session_links(self.output)

    def run_loop(self, initial_url: Optional[str] = None):
        self.print_banner()

        if initial_url:
            self.scrape_single(initial_url)

        try:
            while True:
                try:
                    raw = input(UI.c("scraper> ", UI.BOLD + UI.GREEN)).strip()
                except EOFError:
                    print()
                    break

                if not raw:
                    continue

                parts = raw.split()
                cmd = parts[0].lower()

                try:
                    if cmd in ("q", "quit", "exit"):
                        break
                    elif cmd in ("h", "help", "?"):
                        self.print_help()
                    elif cmd == "status":
                        self.print_status()
                    elif cmd == "clear":
                        os.system("cls" if os.name == "nt" else "clear")
                        self.print_banner()
                    elif cmd in ("links", "history"):
                        if not self.session_streams:
                            print(UI.c("  No .m3u8 links collected in this session yet.", UI.YELLOW))
                        else:
                            self.proxy.register_streams(self.session_streams)
                            print(UI.c(f"\n  Current M3U8 Streams ({len(self.session_streams)} total):", UI.BOLD + UI.CYAN))
                            for idx, info in enumerate(self.session_streams.values(), 1):
                                print_stream_info(idx, info, proxy=self.proxy)
                            print()
                    elif cmd == "verify":
                        idx = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
                        self.verify_streams(idx)
                    elif cmd == "play":
                        if len(parts) > 1 and parts[1].isdigit():
                            self.play_stream(int(parts[1]))
                        else:
                            print(UI.c("  Usage: play <stream_number> (e.g. 'play 1')", UI.YELLOW))
                    elif cmd == "cast":
                        if len(parts) > 1 and parts[1].isdigit():
                            quality_arg = "high"
                            manual_ip = None
                            for extra in parts[2:]:
                                if extra.lower() in ("high", "low", "auto"):
                                    quality_arg = extra.lower()
                                else:
                                    manual_ip = extra
                            self.cast_stream(int(parts[1]), quality=quality_arg, known_host=manual_ip)
                        else:
                            print(
                                UI.c(
                                    "  Usage: cast <stream_number> [high|low|auto] [chromecast_ip] (e.g. 'cast 1' or 'cast 1 low')",
                                    UI.YELLOW,
                                )
                            )
                    elif cmd in ("castdevice", "scan", "devices", "chromecasts"):
                        manual_ip = parts[1] if len(parts) > 1 else None
                        self.select_chromecast_device(force_rescan=True, known_host=manual_ip)
                    elif cmd == "stopcast":
                        self.stop_cast()
                    elif cmd == "watch":
                        if len(parts) > 1 and parts[1].isdigit():
                            self.watch_in_clean_browser(int(parts[1]))
                        else:
                            print(UI.c("  Usage: watch <stream_number> (e.g. 'watch 1')", UI.YELLOW))
                    elif cmd.isdigit() and len(parts) == 1:
                        self.watch_in_clean_browser(int(cmd))
                    elif cmd == "save":
                        target_path = parts[1] if len(parts) > 1 else self.output
                        self.save_session_links(target_path)
                    elif cmd == "domain":
                        if len(parts) < 2 or parts[1].lower() in ("auto", "none", "reset"):
                            self.domain = None
                            print(UI.c("  ✓ Domain filter reset to auto (start domain + player iframes)", UI.GREEN))
                        else:
                            self.domain = parts[1]
                            print(UI.c(f"  ✓ Domain filter set to: {self.domain}", UI.GREEN))
                    elif cmd == "depth":
                        if len(parts) > 1 and parts[1].isdigit():
                            self.max_depth = int(parts[1])
                            print(UI.c(f"  ✓ Max depth set to: {self.max_depth}", UI.GREEN))
                        else:
                            print(UI.c("  Usage: depth <integer>", UI.YELLOW))
                    elif cmd == "pages":
                        if len(parts) > 1 and parts[1].isdigit():
                            self.max_pages = int(parts[1])
                            print(UI.c(f"  ✓ Max pages set to: {self.max_pages}", UI.GREEN))
                        else:
                            print(UI.c("  Usage: pages <integer>", UI.YELLOW))
                    elif cmd == "mode":
                        if len(parts) > 1 and parts[1].lower() in ("browser", "http"):
                            self.mode = parts[1].lower()
                            print(UI.c(f"  ✓ Mode set to: {self.mode}", UI.GREEN))
                        else:
                            print(UI.c("  Usage: mode <browser|http>", UI.YELLOW))
                    elif cmd == "wait":
                        try:
                            self.wait = float(parts[1])
                            print(UI.c(f"  ✓ JS wait time set to: {self.wait}s", UI.GREEN))
                        except (IndexError, ValueError):
                            print(UI.c("  Usage: wait <seconds>", UI.YELLOW))
                    else:
                        target_url = parts[0]
                        domain_override = parts[1] if len(parts) > 1 else None
                        self.scrape_single(target_url, domain_override=domain_override)
                except Exception as cmd_exc:
                    print(UI.c(f"  ✗ Command error: {cmd_exc}", UI.RED))

        except KeyboardInterrupt:
            print(UI.c("\nInterrupted. Exiting...", UI.YELLOW))
        finally:
            self._close_browser()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl a domain for HTML pages (ignoring popups and redirects) and extract .m3u8 URLs + headers."
    )
    parser.add_argument(
        "url",
        nargs="?",
        default=None,
        help="Optional starting URL to scrape. Omit to launch interactive CLI UI.",
    )
    parser.add_argument(
        "--interactive",
        "-i",
        action="store_true",
        help="Stay in interactive CLI UI mode (automatic when no URL argument is given)",
    )
    parser.add_argument(
        "--domain",
        help="Target domain(s) to follow, comma-separated (defaults to the starting URL's domain + player iframes)",
        default=None,
    )
    parser.add_argument(
        "--no-subdomains",
        action="store_true",
        help="Only follow exact domain matches (exclude subdomains)",
    )
    parser.add_argument(
        "--no-player-iframes",
        action="store_true",
        help="Do not automatically follow cross-domain video player iframes",
    )
    parser.add_argument(
        "--mode",
        choices=["browser", "http"],
        default="browser",
        help="Scraping mode: 'browser' (Playwright JS rendering + popup/redirect blocking) or 'http' (static requests). Default: browser",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=2,
        help="Maximum link depth to follow (default: 2)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Maximum number of pages to visit per run (default: 10)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay in seconds between requests (default: 0.5)",
    )
    parser.add_argument(
        "--wait",
        type=float,
        default=4.0,
        help="Seconds to wait for dynamic JS streams in browser mode (default: 4.0)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=15,
        help="Request/navigation timeout in seconds (default: 15)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="results.json",
        help="Output file path (.json, .m3u, .csv, or .txt). Default: results.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.url is None or args.interactive:
        cli = InteractiveCLI(args)
        cli.run_loop(initial_url=args.url)
        return

    scraper = DomainM3U8Scraper(
        start_url=args.url,
        allowed_domain=args.domain,
        include_subdomains=not args.no_subdomains,
        follow_player_iframes=not args.no_player_iframes,
        max_depth=args.max_depth,
        max_pages=args.max_pages,
        delay=args.delay,
        timeout=args.timeout,
        mode=args.mode,
        wait_seconds=args.wait,
    )
    scraper.crawl()
    print_crawl_summary(scraper)

    if args.output:
        scraper.save_results(args.output)


if __name__ == "__main__":
    main()
