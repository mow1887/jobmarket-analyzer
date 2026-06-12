import json
import os
import glob
from datetime import datetime
import time
import requests
from bs4 import BeautifulSoup

class JobDatasetEnricher:
    def __init__(self):
        self.raw_dir = "data/raw/stepstone"
        self.processed_dir = "data/processed"
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Deine Tech-Matrix: Wonach soll im Volltext gesucht werden?
        self.tech_keywords = {
            "SQL": ["sql", "postgres", "mysql", "oracle", "pl/sql", "tsql"],
            "Python": ["python", "pandas", "numpy", "scikit"],
            "Cloud": ["aws", "azure", "gcp", "google cloud", "cloud"],
            "Databases": ["snowflake", "bigquery", "redshift", "databricks"],
            "BI Tools": ["power bi", "powerbi", "tableau", "looker", "qlik", "excel"],
            "Data Eng Tools": ["spark", "airflow", "dbt", "docker", "kubernetes", "kafka", "hadoop"]
        }
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def _get_latest_raw_file(self) -> str:
        """Findet die neueste JSON-Datei aus dem Ingestion-Schritt."""
        files = glob.glob(os.path.join(self.raw_dir, "stepstone_hamburg_FULL_*.json"))
        if not files:
            raise FileNotFoundError("❌ Keine rohen JSON-Dateien im Data Lake gefunden!")
        return max(files, key=os.path.getctime)

    def extract_skills_from_text(self, text: str) -> list:
        """Scannt den Text nach der vordefinierten Tech-Matrix."""
        text_lower = text.lower()
        found_skills = set()
        
        for standardized_name, synonyms in self.tech_keywords.items():
            for synonym in synonyms:
                # Einfacher Substring-Match (kann später durch RegEx verfeinert werden)
                if synonym in text_lower:
                    found_skills.add(standardized_name)
                    break # Wenn ein Synonym matcht, reicht das für diese Technologie
                    
        return list(found_skills)

    def enrich_dataset(self):
        raw_file_path = self._get_latest_raw_file()
        print(f"📖 Lade rohe Daten zur Erweiterung aus: {raw_file_path}")
        
        with open(raw_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        jobs = data.get("jobs", [])
        enriched_jobs = []
        
        print(f"🚀 Starte Text-Scraping und Skill-Extraktion für {len(jobs)} Jobs...")
        
        for i, job in enumerate(jobs):
            url = job.get("link")
            title = job.get("title")
            company = job.get("company")
            
            print(f"[{i+1}/{len(jobs)}] Scanne: {title} bei {company}...")
            
            detected_skills = []
            has_full_text = False
            
            # Falls kein valider Link extrahiert wurde, überspringen wir das HTTP-Request
            if url and url.startswith("http"):
                try:
                    # Unterseite laden
                    response = requests.get(url, headers=self.headers, timeout=10)
                    
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, "html.parser")
                        
                        # Wir holen den sichtbaren Text des gesamten Body-Elements
                        # (Perfekt, um keine Anforderungen zu verpassen)
                        full_text = soup.get_text(separator=" ", strip=True)
                        
                        # Skills extrahieren
                        detected_skills = self.extract_skills_from_text(full_text)
                        has_full_text = True
                    else:
                        print(f"   ⚠ Status Code {response.status_code} für diesen Link.")
                except Exception as e:
                    print(f"   ⚠ Fehler beim Abrufen der Details: {e}")
            
            # Wenn gar nichts im Text gefunden wurde, machen wir einen schnellen Check auf den Jobtitel
            if not detected_skills:
                detected_skills = self.extract_skills_from_text(title)
                
            # Erweitertes Job-Objekt bauen
            enriched_job = job.copy()
            enriched_job["detected_skills"] = detected_skills if detected_skills else ["General Data Skill"]
            enriched_job["has_full_text"] = has_full_text
            
            enriched_jobs.append(enriched_job)
            
            # Ein kleiner "Polite Delay", um Stepstone auf den Unterseiten nicht zu stressen
            time.sleep(1)

        # Speichern im Processed/Staging Layer
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.processed_dir, f"enriched_hamburg_jobs_{timestamp}.json")
        
        output_payload = {
            "metadata": {
                "source": "stepstone.de",
                "enriched_at": timestamp,
                "total_jobs": len(enriched_jobs)
            },
            "jobs": enriched_jobs
        }
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_payload, f, ensure_ascii=False, indent=4)
            
        print(f"\n🎯 Erweiterung abgeschlossen! Datei gespeichert unter: {output_file}")

if __name__ == "__main__":
    enricher = JobDatasetEnricher()
    enricher.enrich_dataset()