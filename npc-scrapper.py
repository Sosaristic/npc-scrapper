"""
============================================================
  Nigeria Property Centre — Abuja Agent Contact Scraper
  Site: https://nigeriapropertycentre.com/abuja/agents
  Total Abuja agents: ~740
============================================================

SETUP (run once in your terminal):
    pip install crawl4ai
    crawl4ai-setup

RUN:
    python abuja_agents.py

RESUME (if interrupted, just run again — picks up where it left off):
    python abuja_agents.py

OUTPUT:
    abuja_agent_contacts.csv   — name, phone, whatsapp, address, website, profile_url
    abuja_progress.txt         — tracks completed agents (used for resuming)
============================================================
"""

import asyncio
import csv
import os
import re
import time

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode


# ─────────────────────────────────────────────
#  SETTINGS
# ─────────────────────────────────────────────
BASE_URL     = "https://nigeriapropertycentre.com/abuja/agents"
DELAY        = 2.0
OUTPUT_FILE  = "abuja_agent_contacts.csv"
PROGRESS_LOG = "abuja_progress.txt"
SAVE_EVERY   = 25
# ─────────────────────────────────────────────

CSV_FIELDS = ["name", "phone", "whatsapp", "address", "website", "profile_url"]

browser_cfg = BrowserConfig(
    headless=True,
    user_agent=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    java_script_enabled=True,
)

run_cfg = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,
    wait_until="networkidle",
    page_timeout=30000,
    simulate_user=True,
    magic=True,
    delay_before_return_html=2.0,
)


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def parse_listing_page(markdown: str) -> list:
    pattern = r'https://nigeriapropertycentre\.com/agents/[a-z0-9\-]+-\d+'
    links = re.findall(pattern, markdown)
    seen, unique = set(), []
    for link in links:
        if link not in seen:
            seen.add(link)
            unique.append(link)
    return unique


def parse_profile_page(markdown: str, url: str) -> dict:
    contact = {
        "name":        "",
        "phone":       "",
        "whatsapp":    "",
        "address":     "",
        "website":     "",
        "profile_url": url,
    }

    name = re.search(r'^# (.+)$', markdown, re.MULTILINE)
    if name:
        contact["name"] = name.group(1).strip()

    address = re.search(r'\*\*Address\*\*\s*\n+(.+?)(?=\n\n|\*\*)', markdown, re.DOTALL)
    if address:
        contact["address"] = address.group(1).strip().replace('\n', ', ')

    phone = re.search(r'\*\*Phone\*\*\s*\n+(.+?)(?=\n\n|\*\*)', markdown, re.DOTALL)
    if phone:
        contact["phone"] = phone.group(1).strip()

    whatsapp = re.search(r'\*\*Whatsapp\*\*\s*\n+(.+?)(?=\n\n|\*\*)', markdown, re.DOTALL)
    if whatsapp:
        contact["whatsapp"] = whatsapp.group(1).strip()

    website = re.search(r'\*\*Website\*\*\s*\n+<?(https?://[^\s>]+)>?', markdown)
    if website:
        contact["website"] = website.group(1).strip()

    return contact


def append_to_csv(rows: list):
    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def init_csv():
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()


def load_done() -> set:
    if os.path.exists(PROGRESS_LOG):
        with open(PROGRESS_LOG, "r") as f:
            return set(l.strip() for l in f if l.strip())
    return set()


def mark_done(url: str):
    with open(PROGRESS_LOG, "a") as f:
        f.write(url + "\n")


def eta_str(done: int, total: int, elapsed: float) -> str:
    if done == 0:
        return "--"
    rate = done / elapsed
    secs_left = (total - done) / rate
    h, r = divmod(int(secs_left), 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s"


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
async def main():
    print("\n" + "=" * 58)
    print("  Nigeria Property Centre — Abuja Agent Scraper")
    print("=" * 58)

    done_urls = load_done()
    is_resume = len(done_urls) > 0

    if not is_resume:
        init_csv()

    async with AsyncWebCrawler(config=browser_cfg) as crawler:

        # ── STEP 1: Collect all Abuja agent profile URLs ─────────────
        print("\n📋 STEP 1: Collecting all Abuja agent profile URLs...")
        if is_resume:
            print(f"   (Resuming — {len(done_urls):,} agents already done)\n")
        else:
            print(f"   (~740 agents across ~37 pages)\n")

        all_profile_urls = []
        page = 1

        while True:
            url = BASE_URL if page == 1 else f"{BASE_URL}?page={page}"
            print(f"  → Page {page} ...", end=" ", flush=True)

            result = await crawler.arun(url=url, config=run_cfg)

            if not result.success:
                print(f"FAILED ({result.error_message})")
                print("  Stopping — saving what we have.")
                break

            links = parse_listing_page(result.markdown.raw_markdown)

            if not links:
                print("end of listings.")
                break

            all_profile_urls.extend(links)
            print(f"✓ {len(links)} agents (total: {len(all_profile_urls):,})")

            page += 1
            await asyncio.sleep(DELAY)

        todo         = [u for u in all_profile_urls if u not in done_urls]
        total        = len(all_profile_urls)
        already_done = total - len(todo)

        print(f"\n  Total agents   : {total:,}")
        print(f"  Already done   : {already_done:,}")
        print(f"  Left to scrape : {len(todo):,}")
        print(f"  Est. time      : ~{len(todo) * DELAY / 60:.0f} mins at {DELAY}s/request")

        # ── STEP 2: Visit each profile and extract contacts ──────────
        print(f"\n📇 STEP 2: Scraping agent contact details...\n")

        batch      = []
        step_start = time.time()

        for i, url in enumerate(todo, 1):
            overall_done = already_done + i
            pct      = overall_done / total * 100
            bar_done = int(pct / 2)
            bar      = "█" * bar_done + "░" * (50 - bar_done)
            elapsed  = time.time() - step_start
            eta      = eta_str(i, len(todo), elapsed)
            print(f"\r  [{bar}] {pct:.1f}% | {overall_done:,}/{total:,} | ETA: {eta}   ",
                  end="", flush=True)

            try:
                result = await crawler.arun(url=url, config=run_cfg)
                if result.success:
                    contact = parse_profile_page(result.markdown.raw_markdown, url)
                    batch.append(contact)
                mark_done(url)
            except Exception:
                mark_done(url)

            if len(batch) >= SAVE_EVERY:
                append_to_csv(batch)
                batch = []

            await asyncio.sleep(DELAY)

        if batch:
            append_to_csv(batch)

        print()

    total_done = len(load_done())
    elapsed    = time.time() - step_start
    h, r       = divmod(int(elapsed), 3600)
    m, s       = divmod(r, 60)

    print(f"\n{'=' * 58}")
    print(f"  🎉 ALL DONE!")
    print(f"  Agents collected : {total_done:,}")
    print(f"  Output file      : {OUTPUT_FILE}")
    print(f"  Time taken       : {h}h {m}m {s}s")
    print(f"  Open '{OUTPUT_FILE}' in Excel or Google Sheets.")
    print(f"{'=' * 58}\n")


if __name__ == "__main__":
    asyncio.run(main())
