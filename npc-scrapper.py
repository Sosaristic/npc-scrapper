"""
============================================================
  Nigeria Property Centre — FULL SITE Agent Scraper
  Site: https://nigeriapropertycentre.com/agents
  Total agents: ~16,000+
============================================================

SETUP (run once in your terminal):
    pip install crawl4ai
    crawl4ai-setup

RUN:
    python npc_scraper_full.py

RESUME (if it stops mid-way, just run again — it picks up where it left off):
    python npc_scraper_full.py

OUTPUT:
    agents_contacts.csv     — your final data (opens in Excel / Google Sheets)
    scraped_urls.txt        — tracks which agents are already done (for resuming)
    agent_urls.txt          — all collected profile URLs (saved after Step 1)
============================================================
"""

import asyncio
import csv
import os
import re
import time

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode


# ─────────────────────────────────────────────────────────────
#  SETTINGS
# ─────────────────────────────────────────────────────────────
BASE_URL         = "https://nigeriapropertycentre.com/agents"
DELAY_SECONDS    = 2.0      # pause between requests (increase to 3-4 if you get blocked)
OUTPUT_FILE      = "agents_contacts.csv"
SCRAPED_LOG      = "scraped_urls.txt"       # tracks completed agents (for resume)
AGENT_URLS_FILE  = "agent_urls.txt"         # all profile URLs collected in Step 1
SAVE_EVERY       = 50                       # auto-save CSV every N agents
# ─────────────────────────────────────────────────────────────


# ── Browser: mimics a real Chrome user ───────────────────────
browser_cfg = BrowserConfig(
    headless=True,
    user_agent=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    java_script_enabled=True,
)

# ── Crawl: waits for full page load, bypasses basic anti-bot ─
run_cfg = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,
    wait_until="domcontentloaded",
    page_timeout=60000,
    simulate_user=True,
    magic=True,
    delay_before_return_html=2.0,
)

CSV_FIELDS = ["name", "phone", "whatsapp", "address", "website", "profile_url"]


# ─────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────

def get_agent_links(markdown: str) -> list:
    """Extract all agent profile URLs from a listing page."""
    pattern = r'https://nigeriapropertycentre\.com/agents/[a-z0-9\-]+-\d+'
    links = re.findall(pattern, markdown)
    seen, unique = set(), []
    for link in links:
        if link not in seen:
            seen.add(link)
            unique.append(link)
    return unique


def get_contact_details(markdown: str, url: str) -> dict:
    """Parse contact info from an individual agent profile page."""
    contact = {
        "name": "", "phone": "", "whatsapp": "",
        "address": "", "website": "", "profile_url": url,
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


def load_scraped_urls() -> set:
    """Load the list of already-scraped URLs (for resuming)."""
    if os.path.exists(SCRAPED_LOG):
        with open(SCRAPED_LOG, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def mark_scraped(url: str):
    """Append a URL to the scraped log so we don't redo it."""
    with open(SCRAPED_LOG, "a") as f:
        f.write(url + "\n")


def save_csv(contacts: list, mode: str = "a"):
    """Save (or append) contacts to the CSV file."""
    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if mode == "w" or not file_exists:
            writer.writeheader()
        writer.writerows(contacts)


def load_agent_urls() -> list:
    """Load previously collected agent profile URLs."""
    if os.path.exists(AGENT_URLS_FILE):
        with open(AGENT_URLS_FILE, "r") as f:
            return [line.strip() for line in f if line.strip()]
    return []


def save_agent_urls(urls: list):
    """Save collected agent URLs to file."""
    with open(AGENT_URLS_FILE, "w") as f:
        f.write("\n".join(urls))


def print_progress(current: int, total: int, start_time: float):
    """Print a progress bar with ETA."""
    pct = current / total * 100
    done = int(pct / 2)
    bar = "█" * done + "░" * (50 - done)
    elapsed = time.time() - start_time
    rate = current / elapsed if elapsed > 0 else 0
    remaining = (total - current) / rate if rate > 0 else 0
    eta_h, eta_m = divmod(int(remaining / 60), 60)
    print(f"\r  [{bar}] {pct:.1f}% | {current}/{total} | ETA: {eta_h}h {eta_m}m", end="", flush=True)


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
async def scrape():
    print("\n" + "=" * 60)
    print("  Nigeria Property Centre — FULL SITE Agent Scraper")
    print("=" * 60)

    async with AsyncWebCrawler(config=browser_cfg) as crawler:

        # ── STEP 1: Collect all agent profile URLs ────────────────────────────
        # If we already collected them in a previous run, skip this step
        agent_urls = load_agent_urls()

        if agent_urls:
            print(f"\n✅ Loaded {len(agent_urls)} agent URLs from previous run ({AGENT_URLS_FILE})")
            print("   Skipping Step 1 — going straight to scraping profiles.")
        else:
            print("\n📋 STEP 1: Collecting all agent profile URLs...")
            print("   (This will go through all listing pages — may take 10–20 mins)\n")

            page = 1
            while True:
                url = BASE_URL if page == 1 else f"{BASE_URL}?page={page}"
                print(f"  → Page {page}: {url}")

                result = await crawler.arun(url=url, config=run_cfg)

                if not result.success:
                    print(f"  ✗ Failed: {result.error_message}")
                    print("    Stopping collection. Will scrape what we have so far.")
                    break

                links = get_agent_links(result.markdown.raw_markdown)

                if not links:
                    print(f"  ✓ Reached end of listings at page {page}.")
                    break

                agent_urls.extend(links)
                print(f"    ✓ {len(links)} agents found | Total so far: {len(agent_urls)}")

                # Auto-save URLs in case we get interrupted
                save_agent_urls(agent_urls)

                page += 1
                await asyncio.sleep(DELAY_SECONDS)

            print(f"\n✅ Step 1 complete. {len(agent_urls)} total agent profiles found.")
            print(f"   Saved to {AGENT_URLS_FILE}\n")

        # ── STEP 2: Scrape each agent profile ────────────────────────────────
        already_done = load_scraped_urls()
        remaining    = [u for u in agent_urls if u not in already_done]
        total        = len(agent_urls)
        done_count   = len(already_done)

        print(f"\n📇 STEP 2: Scraping agent contact details...")
        print(f"   Total agents : {total:,}")
        print(f"   Already done : {done_count:,}")
        print(f"   Remaining    : {len(remaining):,}")
        print(f"   Est. time    : ~{len(remaining) * DELAY_SECONDS / 3600:.1f} hours at {DELAY_SECONDS}s/request\n")

        if not remaining:
            print("✅ All agents already scraped! Nothing left to do.")
            print(f"   Your data is in: {OUTPUT_FILE}")
            return

        batch     = []
        step_start = time.time()

        for i, url in enumerate(remaining, 1):
            overall = done_count + i
            print_progress(overall, total, step_start)

            try:
                result = await crawler.arun(url=url, config=run_cfg)
                if result.success:
                    contact = get_contact_details(result.markdown.raw_markdown, url)
                    batch.append(contact)
                    mark_scraped(url)
                else:
                    # log failures silently — don't stop the whole run
                    mark_scraped(url + "  [FAILED]")
            except Exception:
                pass

            # Auto-save every SAVE_EVERY agents so we don't lose progress
            if len(batch) >= SAVE_EVERY:
                save_csv(batch, mode="a")
                batch = []

            await asyncio.sleep(DELAY_SECONDS)

        # Save any remaining in the last batch
        if batch:
            save_csv(batch, mode="a")

        print()  # newline after progress bar

    # ── STEP 3: Final summary ─────────────────────────────────────────────────
    scraped_count = len(load_scraped_urls())
    print(f"\n{'=' * 60}")
    print(f"  🎉 DONE!")
    print(f"{'=' * 60}")
    print(f"  Agents scraped : {scraped_count:,}")
    print(f"  Output file    : {OUTPUT_FILE}")
    print(f"  Open the CSV in Excel or Google Sheets to view your data.")
    print(f"{'=' * 60}\n")


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    total_start = time.time()
    asyncio.run(scrape())
    elapsed = time.time() - total_start
    h, rem = divmod(int(elapsed), 3600)
    m, s   = divmod(rem, 60)
    print(f"⏱️  Total runtime: {h}h {m}m {s}s")
