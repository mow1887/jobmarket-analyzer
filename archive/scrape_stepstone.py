import json
import os
from datetime import datetime
import time
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


class StepStoneFullIngestionPipeline:

    def __init__(self):
        self.output_dir = "data/raw/stepstone"
        os.makedirs(self.output_dir, exist_ok=True)
        self.base_url = "https://www.stepstone.de/jobs/data/in-hamburg?radius=10&administrative_division_id=de_hamburg"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def scrape_all_available_jobs(self):
        """Durchläuft ALLE verfügbaren Seiten dynamisch und holt Titel, Firma UND Job-Link."""
        scraped_jobs = []
        current_page = 1
        keep_scraping = True

        print("🤖 Starte Full Ingestion Headless Browser via Playwright...")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.headers["User-Agent"])
            page = context.new_page()

            while keep_scraping:
                print(
                    f"\n🌐 Rufe StepStone Hamburg ab - Seite {current_page}..."
                )
                url = f"{self.base_url}&page={current_page}"

                try:
                    page.goto(url, wait_until="networkidle", timeout=30000)

                    # Automatisches Scrollen für Lazy Loading
                    page.evaluate(
                        "window.scrollTo(0, document.body.scrollHeight / 2);"
                    )
                    page.wait_for_timeout(1500)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    page.wait_for_timeout(1500)

                    html_content = page.content()
                    soup = BeautifulSoup(html_content, "html.parser")
                    job_cards = soup.find_all("article")

                    if not job_cards:
                        print(
                            f"🛑 Keine Job-Karten mehr auf Seite {current_page} gefunden. Beende Ingestion."
                        )
                        break

                    page_jobs_count = 0
                    for card in job_cards:
                        # 1. Titel-Element suchen
                        title_element = card.find("h2") or card.find(
                            attrs={"data-testid": "job-item-title"}
                        )

                        if not title_element:
                            continue

                        title = title_element.get_text(strip=True)

                        # NEW: 2. URL/Link-Extraktion
                        job_link = "Kein Link verfügbar"
                        # Wir suchen das <a> Tag, das entweder direkt das <h2> umschließt oder darin liegt
                        anchor_tag = card.find("a", href=True)
                        if anchor_tag:
                            href = anchor_tag.get("href")
                            # Sicherstellen, dass es ein valider Job-Link ist und die Domain davorhängen, falls relativ
                            if href.startswith("/"):
                                job_link = f"https://www.stepstone.de{href}"
                            elif href.startswith("http"):
                                job_link = href

                        # 3. Firmen-Extraktion via Logo-Alt-Text
                        company = None
                        logo_img = card.find("img")
                        if logo_img and logo_img.get("alt"):
                            alt_text = logo_img.get("alt").strip()
                            if "logo von" in alt_text.lower():
                                company = (
                                    alt_text.lower()
                                    .replace("logo von", "")
                                    .strip()
                                    .title()
                                )
                            else:
                                company = alt_text

                        # Fallback auf Klasse 'res-ewgtgq'
                        if not company:
                            company_div = card.find("div", class_="res-ewgtgq")
                            if company_div:
                                raw_text = company_div.get_text(strip=True)
                                if title in raw_text:
                                    span_element = company_div.find("span")
                                    company = (
                                        span_element.get_text(strip=True)
                                        if span_element
                                        else "Spannendes Unternehmen"
                                    )
                                else:
                                    company = raw_text

                        if not company:
                            company = "Spannendes Unternehmen"

                        # Datenstruktur befüllen (jetzt mit Link!)
                        scraped_jobs.append(
                            {
                                "title": title,
                                "company": company,
                                "link": job_link,  # Speicher den extrahierten Link
                                "scraped_at": datetime.now().isoformat(),
                                "source_page": current_page,
                            }
                        )
                        page_jobs_count += 1

                    print(
                        f"✅ Seite {current_page} erfolgreich verarbeitet ({page_jobs_count} Jobs extrahiert)."
                    )

                    if page_jobs_count < 5:
                        print(
                            "ℹ Letzte Seite der Ergebnisse erreicht (geringe Jobdichte)."
                        )
                        break

                    current_page += 1
                    time.sleep(3)  # Polite Layer gegen IP-Sperren

                except Exception as e:
                    print(
                        f"❌ Fehler oder Timeout auf Seite {current_page}: {e}"
                    )
                    break

            browser.close()

        # Speicher-Prozess für die Full-JSON
        if scraped_jobs:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(
                self.output_dir, f"stepstone_hamburg_FULL_{timestamp}.json"
            )

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

            print(
                f"\n🎯 [PIPELINE SUCCESS] Insgesamt {len(scraped_jobs)} Hamburger Jobs inklusive URLs extrahiert!"
            )
            print(f"💾 Datensatz im Data Lake gesichert unter: {file_path}")


if __name__ == "__main__":
    pipeline = StepStoneFullIngestionPipeline()
    pipeline.scrape_all_available_jobs()