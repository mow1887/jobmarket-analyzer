import json
import os
import random
from datetime import datetime
import time
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


class StepStoneFullIngestionPipeline:

    def __init__(self):
        self.output_dir = "data/raw/stepstone"
        os.makedirs(self.output_dir, exist_ok=True)
        self.base_url = "https://www.stepstone.de/jobs/data/in-hamburg?radius=50&administrative_division_id=de_hamburg"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }

    def scrape_all_available_jobs(self, max_pages: int = 50):
        """Iterates through StepStone pages with smart context rotation to bypass HTTP2/Anti-Bot walls."""
        scraped_jobs = []
        current_page = 1

        print("🤖 Launching Resilience-Enhanced Ingestion Pipeline...")

        with sync_playwright() as p:
            # We use chromium but launch it cleanly
            browser = p.chromium.launch(headless=True)
            
            # Helper to create a fresh context
            def create_fresh_context():
                # Slight variation in user agent to simulate different sessions
                version = random.choice(["120.0.0.0", "121.0.0.0", "122.0.0.0"])
                ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36"
                return browser.new_context(user_agent=ua, viewport={"width": 1920, "height": 1080})

            context = create_fresh_context()
            page = context.new_page()

            while current_page <= max_pages:
                print(f"🌐 Ingesting StepStone Hamburg - Page {current_page}...")
                url = f"{self.base_url}&page={current_page}"

                # ROTATION MECHANISM: Every 5 pages, destroy context and build a completely fresh session
                if current_page > 1 and current_page % 5 == 1:
                    print("🔄 Rotating Browser Context & Session Tokens to bypass infrastructure walls...")
                    page.close()
                    context.close()
                    time.sleep(random.uniform(4.0, 8.0)) # Take a short breath
                    context = create_fresh_context()
                    page = context.new_page()

                try:
                    # We increase timeout slightly for higher pages
                    page.goto(url, wait_until="commit", timeout=25000)
                    page.wait_for_timeout(random.uniform(1500, 2500))

                    # Human-like staggered scrolling
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight / 3);")
                    page.wait_for_timeout(random.uniform(800, 1500))
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight / 1.5);")
                    page.wait_for_timeout(random.uniform(800, 1500))
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    page.wait_for_timeout(1000)

                    html_content = page.content()
                    soup = BeautifulSoup(html_content, "html.parser")
                    job_cards = soup.find_all("article")

                    if not job_cards:
                        print(f"🛑 No more job cards visible on page {current_page}. Reached the organic end.")
                        break

                    page_jobs_count = 0
                    for card in job_cards:
                        title_element = card.find("h2") or card.find(attrs={"data-testid": "job-item-title"})
                        if not title_element:
                            continue

                        title = title_element.get_text(strip=True)

                        job_link = "No Link"
                        anchor = card.find("a", href=lambda x: x and "stellenangebote" in x) or card.find("a", href=True)
                        if anchor:
                            href = anchor.get("href")
                            job_link = f"https://www.stepstone.de{href}" if href.startswith("/") else href

                        company = None
                        logo_img = card.find("img")
                        if logo_img and logo_img.get("alt"):
                            alt_text = logo_img.get("alt").strip()
                            company = alt_text.lower().replace("logo von", "").strip().title() if "logo von" in alt_text.lower() else alt_text

                        if not company:
                            company_div = card.find("div", class_="res-ewgtgq")
                            if company_div:
                                raw_text = company_div.get_text(strip=True)
                                company = company_div.find("span").get_text(strip=True) if title in raw_text and company_div.find("span") else raw_text

                        company = company or "Spannendes Unternehmen"

                        card_text = card.get_text(" | ", strip=True)
                        remote_status = "No Info"
                        if "home-office" in card_text.lower() or "remote" in card_text.lower():
                            remote_status = "Teilweise Home-Office" if "teilweise" in card_text.lower() else "Vollständig Remote"

                        scraped_jobs.append({
                            "title": title,
                            "company": company,
                            "link": job_link,
                            "location": "Hamburg",
                            "remote_status": remote_status,
                            "scraped_at": datetime.now().isoformat(),
                            "source_page": current_page
                        })
                        page_jobs_count += 1

                    print(f"✅ Page {current_page} successfully processed ({page_jobs_count} jobs stored).")
                    current_page += 1

                    # SMART JITTER: Anti-pattern sleep interval
                    sleep_duration = random.uniform(3.5, 6.5)
                    time.sleep(sleep_duration)

                except Exception as e:
                    print(f"❌ Error encountered on page {current_page}: {e}")
                    print("🚀 Safe-Fail: Saving accumulated pipeline items before shutdown...")
                    break

            context.close()
            browser.close()

        # Finalize and persist structural state
        if scraped_jobs:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(self.output_dir, f"stepstone_hamburg_V4_{timestamp}.json")

            output_payload = {
                "metadata": {
                    "source": "stepstone.de",
                    "extracted_at": timestamp,
                    "total_pages_scraped": current_page - 1,
                    "total_jobs_scraped": len(scraped_jobs),
                },
                "jobs": scraped_jobs,
            }

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(output_payload, f, ensure_ascii=False, indent=4)

            print(f"\n🎯 [PIPELINE COMPLETED] Captured a total of {len(scraped_jobs)} raw jobs across pages!")
            print(f"💾 Persistent file landed at: {file_path}")


if __name__ == "__main__":
    pipeline = StepStoneFullIngestionPipeline()
    pipeline.scrape_all_available_jobs(max_pages=50)