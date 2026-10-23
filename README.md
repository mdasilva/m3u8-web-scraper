# M3U8 Domain Scraper & Stream Player

An interactive Python HTML & JavaScript stream scraper that crawls a target domain (and embedded video player iframes), blocks popups and ad redirects, extracts `.m3u8` HLS stream URLs along with their required HTTP headers (`Referer`, `Origin`, `User-Agent`), and lets you watch streams directly in:

- **Clean Ad-Free Browser Player (`watch <n>` or `<n>`)** — Opens a dedicated Chromium player window with all popups and redirects blocked, full window resizing, Picture-in-Picture, and Fullscreen support.
- **VLC / ffplay via Local Header Proxy (`play <n>`)** — Streams through a local HTTP proxy (`LocalHLSProxy`) that automatically injects `Referer`, `Origin`, and `User-Agent` on every playlist and `.ts` segment request.
- **Chromecast via Zero-CPU LAN Relay (`cast <n>`)** — Discovers Chromecast devices on your Wi-Fi/LAN and streams directly to your TV with zero video re-encoding on your computer.

---

## 1. Installation

### Prerequisites
- **Python 3.10+**

---

### Windows (Windows 10 / 11 — PowerShell)

1. **Clone or open the project folder in PowerShell:**
   ```powershell
   cd path\to\SCRAPER
   ```

2. **Install Python dependencies:**
   ```powershell
   pip install -r requirements.txt
   ```

3. **Install the Playwright Chromium browser:**
   ```powershell
   playwright install chromium
   ```

4. **(Optional) Install VLC, FFmpeg (`ffplay`), or Streamlink via `winget`:**
   ```powershell
   winget install VideoLAN.VLC
   winget install Gyan.FFmpeg
   winget install Streamlink.Streamlink
   ```
   *(Restart PowerShell after installing with `winget` so your `PATH` updates.)*

---

### macOS (OS X — Terminal)

1. **Install Python 3 & Homebrew (if not already installed):**
   ```bash
   brew install python
   ```

2. **Create and activate a virtual environment (recommended on macOS):**
   ```bash
   cd /path/to/SCRAPER
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Install the Playwright Chromium browser:**
   ```bash
   playwright install chromium
   ```

5. **(Optional) Install VLC, FFmpeg (`ffplay`), or Streamlink via Homebrew:**
   ```bash
   brew install --cask vlc
   brew install ffmpeg streamlink
   ```

---

## 2. Quick Start (Interactive CLI)

Launch the interactive CLI prompt without arguments:

- **Windows:**
  ```powershell
  python scraper.py
  ```
- **macOS:**
  ```bash
  python3 scraper.py
  ```

### Example Workflow

```text
scraper> https://isportsurge.shop/
```

Once the scraper finishes and lists discovered streams (`[1]`, `[2]`, ...):

| Command | Description |
| :--- | :--- |
| `<url> [domain]` | Scrape `<url>` for `.m3u8` streams (optionally restrict to `[domain]`). |
| `1` or `watch 1` | Open stream `#1` in a clean, popup-blocked Chromium player window. |
| `play 1` | Launch stream `#1` in VLC (or `ffplay`) via the local header-injecting proxy. |
| `cast 1` | Scan your LAN for Chromecast devices, choose your TV, and cast stream `#1`. |
| `cast 1 192.168.1.50` | Cast stream `#1` directly to a specific Chromecast IP address. |
| `castdevice [ip]` | Re-scan your network and switch the active Chromecast device. |
| `stopcast` | Stop playback on the active Chromecast device. |
| `verify [index]` | Test `.m3u8` stream URLs with captured `Referer`/`Origin` headers (`200 OK`). |
| `links` | Display all `.m3u8` streams and playback commands from the current session. |
| `save [filepath]` | Export discovered streams to `.json`, `.m3u`, `.csv`, or `.txt`. |
| `domain <name\|auto>` | Set or reset the persistent target domain filter. |
| `depth <n>` | Set maximum link crawl depth (default: `2`). |
| `pages <n>` | Set maximum pages to crawl per run (default: `10`). |
| `mode <browser\|http>` | Switch between Playwright `browser` mode and static `http` mode. |
| `wait <sec>` | Set JavaScript wait time per page in seconds (default: `4.0`). |
| `status` / `clear` / `q` | View current settings, clear the terminal, or quit. |

---

## 3. Command-Line Options (One-Shot or Interactive)

You can also pass a starting URL and flags directly from the terminal:

```bash
# Scrape a URL and stay in the interactive prompt (-i)
python scraper.py https://example.com/live -i

# One-shot scrape with custom depth/page limits and export to playlist.m3u
python scraper.py https://example.com/live --max-depth 2 --max-pages 15 -o playlist.m3u
```

### All CLI Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| `url` | *(None)* | Optional starting URL. Omit to start directly in interactive mode. |
| `-i`, `--interactive` | `False` | Stay in interactive CLI mode after scraping the initial `url`. |
| `--domain` | `auto` | Comma-separated domain(s) to follow (defaults to start domain + player iframes). |
| `--no-subdomains` | `False` | Only follow exact domain matches (ignore subdomains). |
| `--no-player-iframes` | `False` | Do not follow cross-domain embedded video player `<iframe>` URLs. |
| `--mode` | `browser` | `browser` (Playwright JS rendering) or `http` (fast static HTML). |
| `--max-depth` | `2` | Maximum link depth to follow from the starting URL. |
| `--max-pages` | `10` | Maximum number of pages to visit per crawl run. |
| `--wait` | `4.0` | Seconds to wait on each page for dynamic JS `.m3u8` requests. |
| `--delay` | `0.5` | Delay in seconds between page requests. |
| `--timeout` | `15` | Page navigation / HTTP request timeout in seconds. |
| `-o`, `--output` | `results.json` | Output file path (`.json`, `.m3u`, `.csv`, or `.txt`). |

---

## 4. Chromecast & Network Tips

When you run `cast 1`, the scraper starts a lightweight HTTP relay on port `8765` (`http://<YOUR_LAN_IP>:8765/stream/1.m3u8`) so your Chromecast can fetch the HLS manifest and `.ts` video segments with the required `Referer` and `Origin` headers injected automatically.

- **Keep the CLI open while casting:** Because your computer acts as the header-injecting relay for the `.ts` segments, keep `scraper.py` running while watching on your TV.
- **Windows Firewall:** The first time you run `play` or `cast`, Windows Defender Firewall may prompt you to allow Python network access. Check **Private networks** and click **Allow access** so your Chromecast can reach port `8765`.
- **macOS Local Network Permission:** On macOS Sequoia / Sonoma, if prompted with *"Terminal/Python would like to find and connect to devices on your local network"*, click **Allow** so Chromecast mDNS discovery works.

---

## 5. Running Unit Tests

```bash
python -m unittest -v test_scraper.py
```
