import json
import os
from datetime import datetime
import time
import requests


class JobmarketIngestionPipeline:

    def __init__(self):
        self.api_url = "https://www.arbeitnow.com/api/job-board-api"
        self.headers = {
            "User-Agent": "JobmarketAnalyzerPipeline/1.0",
            "Accept": "application/json",
        }

    def fetch_all_active_jobs(self, max_pages: int = 5) -> list:
        """Inkrementeller Abruf über mehrere API-Seiten hinweg."""
        all_fetched_jobs = []
        current_page = 1
        next_page_url = self.api_url

        print("📡 Starte inkrementellen API-Abruf (Arbeitnow)...")

        while next_page_url and current_page <= max_pages:
            print(f"📄 lade Seite {current_page}...")
            try:
                response = requests.get(
                    next_page_url, headers=self.headers, timeout=10
                )
                response.raise_for_status()
                payload = response.json()

                page_jobs = payload.get("data", [])
                all_fetched_jobs.extend(page_jobs)

                # Nächste Seite aus den API-Metadaten auslesen
                next_page_url = payload.get("links", {}).get("next")
                current_page += 1

                # Kurze Pause zum Schutz der API
                time.sleep(0.5)

            except requests.exceptions.RequestException as e:
                print(f"❌ Fehler auf Seite {current_page}: {e}")
                break

        return all_fetched_jobs


def filter_and_save_data(jobs: list):
    """Filtert die Rohdaten nach Data-Bezug & Hamburg und speichert sie."""
    hamburg_data_jobs = []

    # Relevante Keywords für den inhaltlichen Filter
    data_keywords = ["data", "analytics", "bi", "business intelligence"]

    for job in jobs:
        location = job.get("location", "").lower()
        title = job.get("title", "").lower()

        # Bedingung: Muss in Hamburg sein UND einen Data-Bezug im Titel haben
        is_hamburg = "hamburg" in location
        has_data_relation = any(kw in title for kw in data_keywords)

        if is_hamburg and has_data_relation:
            hamburg_data_jobs.append(job)

    print("\n--- Pipeline Ingestion Report ---")
    print(f"Gesamt von API geladen: {len(jobs)} Jobs.")
    print(f"🎯 Nach Filter verbleiben: {len(hamburg_data_jobs)} Data-Jobs in Hamburg.")

    # Speichern im lokalen Data Lake (Staging)
    output_dir = "data/raw/arbeitnow_filtered"
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(
        output_dir, f"hamburg_data_jobs_{timestamp}.json"
    )

    output_payload = {
        "metadata": {
            "extracted_at": timestamp,
            "total_extracted": len(hamburg_data_jobs),
            "source": "arbeitnow.com",
        },
        "jobs": hamburg_data_jobs,
    }

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=4)

    print(f"💾 Strukturierte Rohdaten erfolgreich gesichert: {file_path}")

    # Preview
    if hamburg_data_jobs:
        print("\n--- 🔍 Top 3 Hamburg Data Jobs Preview ---")
        for i, job in enumerate(hamburg_data_jobs[:3]):
            print(f"{i+1}. {job.get('title')} ({job.get('company_name')})")


if __name__ == "__main__":
    pipeline = JobmarketIngestionPipeline()
    # Wir laden die ersten 5 Seiten (ca. 500 Jobs), um eine gute Basis zu haben
    raw_job_pool = pipeline.fetch_all_active_jobs(max_pages=5)
    filter_and_save_data(raw_job_pool)