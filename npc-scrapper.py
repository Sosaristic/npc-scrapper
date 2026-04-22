"""
============================================================
  Nigeria Property Centre — Agent Profile Scraper
  Site: https://nigeriapropertycentre.com/agents
============================================================

SETUP (run once in your terminal):
    pip install crawl4ai
    crawl4ai-setup

RUN:
    python npc_profiles.py

OUTPUT:
    agent_profiles.csv  — name, address, profile_url for all agents
============================================================
"""

import asyncio
import csv
import re
import time

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode


# ─────────────────────────────────────────────
#  SETTINGS
# ─────────────────────────────────────────────
BASE_URL    = "https://nigeriapropertycentre.com/agents"
DELAY       = 2.0
OUTPUT_FILE = "agent_profiles.csv"
# ─────────────────────────────────────────────


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

CSV_FIELDS = ["name", "address", "profile_url"]


def parse_agents_from_page(markdown: str) -> list:
    """
    Extract agent name, address, and profile URL
    directly from a listing page — no need to visit each profile.
    """
    agents = []

    # Each agent block looks like:
    # ## [**Agent Name**](https://nigeriapropertycentre.com/agents/slug-id)
    #   Address text
    blocks = re.findall(
        r'## \[\*\*(.+?)\*\*\]\((https://nigeriapropertycentre\.com/agents/[a-z0-9\-]+-\d+)\)'
        r'\s*\n+\s*\n*\s*(.+?)(?=\n\n|\Z)',
        markdown,
        re.DOTALL
    )

    for name, url, rest in blocks:
        # Address is the first non-empty line after the heading
        lines = [l.strip() for l in rest.strip().splitlines() if l.strip()]
        address = lines[0] if lines else ""
        # Skip lines that are links or image tags
        if address.startswith("!") or address.startswith("[") or address.startswith("http"):
            address = ""

        agents.append({
            "name":        name.strip(),
            "address":     address,
            "profile_url": url.strip(),
        })

    return agents


def save_to_csv(agents: list, mode: str = "a"):
    file_exists = __import__("os").path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if mode == "w" or not file_exists:
            writer.writeheader()
        writer.writerows(agents)


async def collect():
    total_agents = 0
    page = 1

    # Start fresh
    save_to_csv([], mode="w")

    print("\n🚀 Starting agent profile collection...\n")

    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        while True:
            url = BASE_URL if page == 1 else f"{BASE_URL}?page={page}"
            print(f"→ Page {page}: {url}")

            result = await crawler.arun(url=url, config=run_cfg)

            if not result.success:
                print(f"  ✗ Failed: {result.error_message}")
                print("  Saving what we have and stopping.")
                break

            agents = parse_agents_from_page(result.markdown.raw_markdown)

            if not agents:
                print(f"  ✓ No more agents — reached end of listings.")
                break

            save_to_csv(agents, mode="a")
            total_agents += len(agents)
            print(f"  ✓ {len(agents)} agents saved | Total so far: {total_agents:,}")

            page += 1
            await asyncio.sleep(DELAY)

    print(f"\n🎉 Done! {total_agents:,} agent profiles saved to '{OUTPUT_FILE}'")


if __name__ == "__main__":
    start = time.time()
    asyncio.run(collect())
    elapsed = time.time() - start
    m, s = divmod(int(elapsed), 60)
    print(f"⏱️  Total time: {m}m {s}s\n")
