import unittest
from unittest.mock import MagicMock, patch
import requests
from bs4 import BeautifulSoup
from scraper import DomainM3U8Scraper, LocalHLSProxy, StreamInfo


class TestDomainM3U8Scraper(unittest.TestCase):
    def setUp(self):
        self.scraper = DomainM3U8Scraper(
            start_url="https://example.com/videos",
            max_depth=2,
            max_pages=10,
            delay=0,
            mode="http",
            follow_player_iframes=False,
        )

    def test_extract_domain_links_ignores_popups_and_external(self):
        html = """
        <html>
          <head><meta http-equiv="refresh" content="0; url=https://ads.other.com/redir"></head>
          <body>
            <a href="/videos/page2">Valid Page 2</a>
            <a href="https://sub.example.com/watch/1">Valid Subdomain</a>
            <a href="https://otherdomain.com/page">External Link</a>
            <a href="/popup-ad" onclick="window.open('/popup-ad')">Popup Link</a>
            <a href="javascript:window.open('https://example.com/pop')">JS Popup</a>
            <iframe src="/embed/player1"></iframe>
            <iframe src="https://ads.other.com/iframe"></iframe>
          </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        links = self.scraper.extract_domain_links(soup, "https://example.com/videos")
        self.assertEqual(
            links,
            [
                "https://example.com/videos/page2",
                "https://sub.example.com/watch/1",
                "https://example.com/embed/player1",
            ],
        )

    def test_extract_m3u8_links_from_tags_and_scripts(self):
        html = """
        <html>
          <body>
            <video src="/streams/live.m3u8?token=abc"></video>
            <script>
              var playerConfig = {
                file: "https:\\/\\/cdn.example.com\\/hls\\/master.m3u8?Expires=123",
                backup: "/hls/backup/index.m3u8"
              };
            </script>
          </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        m3u8s = self.scraper.extract_m3u8_links(html, "https://example.com/videos", soup)
        self.assertIn("https://example.com/streams/live.m3u8?token=abc", m3u8s)
        self.assertIn("https://cdn.example.com/hls/master.m3u8?Expires=123", m3u8s)
        self.assertIn("https://example.com/hls/backup/index.m3u8", m3u8s)

    def test_crawl_ignores_http_redirects(self):
        resp_start = MagicMock()
        resp_start.status_code = 200
        resp_start.headers = {"Content-Type": "text/html"}
        resp_start.text = '<a href="/redirect-page">Redir</a><a href="/video-page">Video</a>'

        resp_redir = MagicMock()
        resp_redir.status_code = 302
        resp_redir.headers = {"Location": "https://ads.example.org/promo"}

        resp_video = MagicMock()
        resp_video.status_code = 200
        resp_video.headers = {"Content-Type": "text/html"}
        resp_video.text = '<source src="https://cdn.example.com/stream.m3u8">'

        def side_effect(url, headers, allow_redirects, timeout):
            if url == "https://example.com/videos":
                return resp_start
            if url == "https://example.com/redirect-page":
                return resp_redir
            if url == "https://example.com/video-page":
                return resp_video
            raise AssertionError(f"Unexpected URL: {url}")

        with patch.object(self.scraper.session, "get", side_effect=side_effect):
            self.scraper.crawl()

        self.assertEqual(len(self.scraper.skipped_redirects), 1)
        self.assertIn("https://cdn.example.com/stream.m3u8", self.scraper.all_m3u8_links)


class TestLocalHLSProxy(unittest.TestCase):
    def test_master_cache_and_lan_host_rewriting_and_cors(self):
        proxy = LocalHLSProxy(port=18765)
        info = StreamInfo(
            m3u8_url="https://lb.strmd.st/secure/tok/playlist.m3u8",
            headers={"Referer": "https://embed.st/", "Origin": "https://embed.st"},
        )
        proxy.register_streams({info.m3u8_url: info})
        local_url = proxy.proxy_url(1, lan=False)

        upstream_calls = {"master": 0, "segment": 0}
        fake_webp_header = b"RIFFbe@\x00WEBPVP8L" + (b"\x00" * 26)
        clean_ts_packets = (b"\x47" + b"\x00" * 187) * 3

        def fake_get(url, headers, timeout, stream):
            resp = MagicMock()
            resp.__enter__.return_value = resp
            resp.__exit__.return_value = False
            if url.endswith("playlist.m3u8"):
                upstream_calls["master"] += 1
                if upstream_calls["master"] > 1:
                    # Simulate single-use token returning 403 on second upstream hit
                    resp.status_code = 403
                    return resp
                resp.status_code = 200
                resp.headers = {"Content-Type": "application/vnd.apple.mpegurl"}
                resp.text = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=4928000\nhigh/mono.m3u8\n"
                return resp
            else:
                upstream_calls["segment"] += 1
                resp.status_code = 200
                resp.headers = {"Content-Type": "image/webp"}
                resp.content = fake_webp_header + clean_ts_packets
                return resp

        with patch.object(proxy.session, "get", side_effect=fake_get):
            # 1st fetch from local VLC
            r1 = requests.get(local_url, timeout=5)
            self.assertEqual(r1.status_code, 200)
            self.assertEqual(r1.headers.get("Access-Control-Allow-Origin"), "*")
            self.assertIn(f"http://127.0.0.1:{proxy.port}/fetch/1?url=", r1.text)

            # 2nd fetch simulating Chromecast with a LAN Host header (must succeed from master_cache!)
            r2 = requests.get(local_url, headers={"Host": f"192.168.1.99:{proxy.port}"}, timeout=5)
            self.assertEqual(r2.status_code, 200)
            self.assertEqual(upstream_calls["master"], 1)
            self.assertIn(f"http://192.168.1.99:{proxy.port}/fetch/1?url=", r2.text)

            # Fetch a segment disguised with a 42-byte RIFF/WebP header -> proxy must strip header & set Content-Length
            seg_url = f"http://127.0.0.1:{proxy.port}/fetch/1?url=https%3A%2F%2Flb.strmd.st%2Fseg1.ts"
            r_seg = requests.get(seg_url, timeout=5)
            self.assertEqual(r_seg.status_code, 200)
            self.assertEqual(r_seg.headers.get("Content-Type"), "video/MP2T")
            self.assertEqual(r_seg.headers.get("Content-Length"), str(len(clean_ts_packets)))
            self.assertEqual(r_seg.content, clean_ts_packets)


if __name__ == "__main__":
    unittest.main()

