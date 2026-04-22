"""
============================================================
  Nigeria Property Centre — Agent Contact Scraper
  Site: https://nigeriapropertycentre.com/agents
============================================================

SETUP (run these once in your terminal):
    pip install crawl4ai beautifulsoup4 requests
    crawl4ai-setup

RUN:
    python npc_scraper.py

OUTPUT:
    agents_contacts.csv  (name, phone, whatsapp, address, website, profile_url)
============================================================
"""

import asyncio
import csv
import re
import time

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode


# ─────────────────────────────────────────────
#  SETTINGS  — change these as needed
# ─────────────────────────────────────────────
BASE_URL      = "https://nigeriapropertycentre.com/agents"
MAX_PAGES     = 5          # pages of agent listings to crawl (20 agents per page)
                           # change to None to scrape ALL 16,000+ agents (takes hours)
DELAY_SECONDS = 2.0        # wait between requests — keeps you from getting blocked
OUTPUT_FILE   = "agents_contacts.csv"
# ─────────────────────────────────────────────


# Browser config — mimics a real Chrome user
browser_cfg = BrowserConfig(
    headless=True,         # set False if you want to watch the browser open
    user_agent=(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    java_script_enabled=True,
)

# Crawl config — waits for page to fully load, simulates human behaviour
run_cfg = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,      # always fetch fresh (not cached)
    wait_until="networkidle",         # wait for JS to finish loading
    page_timeout=30000,               # 30 second timeout per page
    simulate_user=True,               # simulate mouse movement
    magic=True,                       # extra anti-bot bypass
    delay_before_return_html=2.0,     # extra wait for dynamic content
)


# ─────────────────────────────────────────────
#  HELPER: extract all agent profile links
#  from a listing page
# ─────────────────────────────────────────────
def get_agent_links(markdown: str) -> list:
    pattern = r'https://nigeriapropertycentre\.com/agents/[a-z0-9\-]+-\d+'
    links = re.findall(pattern, markdown)
    # remove duplicates, keep order
    seen, unique = set(), []
    for link in links:
        if link not in seen:
            seen.add(link)
            unique.append(link)
    return unique


# ─────────────────────────────────────────────
#  HELPER: extract contact details from
#  an individual agent profile page
# ─────────────────────────────────────────────
def get_contact_details(markdown: str, url: str) -> dict:
    contact = {
        "name":        "",
        "phone":       "",
        "whatsapp":    "",
        "address":     "",
        "website":     "",
        "profile_url": url,
    }

    # Agent name — the first H1 heading on the page
    name = re.search(r'^# (.+)$', markdown, re.MULTILINE)
    if name:
        contact["name"] = name.group(1).strip()

    # Address
    address = re.search(r'\*\*Address\*\*\s*\n+(.+?)(?=\n\n|\*\*)', markdown, re.DOTALL)
    if address:
        contact["address"] = address.group(1).strip().replace('\n', ', ')

    # Phone number(s)
    phone = re.search(r'\*\*Phone\*\*\s*\n+(.+?)(?=\n\n|\*\*)', markdown, re.DOTALL)
    if phone:
        contact["phone"] = phone.group(1).strip()

    # WhatsApp number
    whatsapp = re.search(r'\*\*Whatsapp\*\*\s*\n+(.+?)(?=\n\n|\*\*)', markdown, re.DOTALL)
    if whatsapp:
        contact["whatsapp"] = whatsapp.group(1).strip()

    # Website
    website = re.search(r'\*\*Website\*\*\s*\n+<?(https?://[^\s>]+)>?', markdown)
    if website:
        contact["website"] = website.group(1).strip()

    return contact


# ─────────────────────────────────────────────
#  MAIN SCRAPER
# ─────────────────────────────────────────────
async def scrape():
    all_contacts = []

    async with AsyncWebCrawler(config=browser_cfg) as crawler:

        # ── STEP 1: Collect all agent profile URLs ──────────────────────────
        print("=" * 55)
        print("  STEP 1: Collecting agent profile links")
        print("=" * 55)

        agent_urls = []
        page = 1

        while True:
            if MAX_PAGES is not None and page > MAX_PAGES:
                break

            url = BASE_URL if page == 1 else f"{BASE_URL}?page={page}"
            print(f"\n→ Listing page {page}: {url}")

            result = await crawler.arun(url=url, config=run_cfg)

            if not result.success:
                print(f"  ✗ Could not load page: {result.error_message}")
                print("    Tip: check your internet connection or try increasing DELAY_SECONDS")
                break

            links = get_agent_links(result.markdown.raw_markdown)

            if not links:
                print("  No agent links found — reached the last page.")
                break

            agent_urls.extend(links)
            print(f"  ✓ Found {len(links)} agents on this page (running total: {len(agent_urls)})")

            page += 1
            await asyncio.sleep(DELAY_SECONDS)

        print(f"\n{'=' * 55}")
        print(f"  STEP 2: Scraping {len(agent_urls)} agent profiles")
        print(f"{'=' * 55}")

        # ── STEP 2: Visit each agent page and extract contacts ───────────────
        for i, url in enumerate(agent_urls, 1):
            print(f"\n[{i}/{len(agent_urls)}] {url}")

            try:
                result = await crawler.arun(url=url, config=run_cfg)

                if result.success:
                    contact = get_contact_details(result.markdown.raw_markdown, url)
                    all_contacts.append(contact)
                    print(f"  ✓ Name:      {contact['name']}")
                    print(f"    Phone:     {contact['phone']}")
                    print(f"    WhatsApp:  {contact['whatsapp']}")
                    print(f"    Address:   {contact['address'][:60]}...")
                else:
                    print(f"  ✗ Failed: {result.error_message}")

            except Exception as e:
                print(f"  ✗ Error: {e}")

            await asyncio.sleep(DELAY_SECONDS)

    # ── STEP 3: Save to CSV ─────────────────────────────────────────────────
    print(f"\n{'=' * 55}")
    print("  STEP 3: Saving results")
    print(f"{'=' * 55}")

    if all_contacts:
        fieldnames = ["name", "phone", "whatsapp", "address", "website", "profile_url"]
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_contacts)
        print(f"\n🎉 SUCCESS! {len(all_contacts)} agent contacts saved to '{OUTPUT_FILE}'")
        print(f"   Open the file in Excel or Google Sheets to view your data.\n")
    else:
        print("\n⚠️  No contacts were collected.")
        print("   Possible reasons:")
        print("   - No internet connection")
        print("   - Site blocked your requests (try increasing DELAY_SECONDS to 3 or 4)")
        print("   - Site structure changed (contact support)\n")


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🚀 Nigeria Property Centre — Agent Scraper Starting...\n")
    start = time.time()
    asyncio.run(scrape())
    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)
    print(f"⏱️  Total time: {mins}m {secs}s")
