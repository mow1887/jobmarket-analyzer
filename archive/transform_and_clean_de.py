import json
import os
import glob
import re
from datetime import datetime
import time
from playwright.sync_api import sync_playwright

class GermanyJobTransformer:
    def __init__(self):
        self.raw_dir = "data/raw/arbeitsagentur"
        self.processed_dir = "data/processed"
        self.master_file = os.path.join(self.processed_dir, "master_enriched_jobs_de.json")
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Erweitere Technologie- und Kriterien-Matrix
        self.tech_keywords = {
            "Python": ["python", "pandas", "numpy", "scikit", "pyspark"],
            "SQL": ["sql", "postgres", "mysql", "oracle", "pl/sql", "tsql"],
            "R": [" r ", "r-studio", "rstudio"],
            "Excel": ["excel", "tabellenkalkulation", "vba"],
            "Power BI": ["power bi", "powerbi", "ms power bi"],
            "Tableau": ["tableau"],
            "Looker": ["looker", "google looker"],
            "Qlik": ["qlik", "qlikview", "qliksense"],
            "AWS": ["aws", "amazon web services"],
            "Azure": ["azure", "microsoft azure"],
            "GCP": ["gcp", "google cloud"],
            "Snowflake": ["snowflake"],
            "BigQuery": ["bigquery"],
            "Databricks": ["databricks"],
            "Airflow": ["airflow", "apache airflow"],
            "dbt": ["dbt", "data build tool"],
            "Spark": ["spark", "apache spark"],
            "Kafka": ["kafka", "apache kafka"],
            "Docker": ["docker"],
            "Kubernetes": ["kubernetes", "k8s"],
            # NEUE ARCHITEKTUR-KEYWORDS
            "ETL": ["etl", "elt", "data pipeline", "datenpipeline", "integration"]
        }
        
        # Neue akademische & formelle Filter-Keywords
        self.education_keywords = {
            "Wirtschaftsinformatik": ["wirtschaftsinformatik", "business informatics"],
            "Abgeschlossenes Studium": ["abgeschlossenes studium", "hochschulstudium", "universitätsabschluss", "bachelor", "master", "diplom", "studium"]
        }

        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    def _load_existing_master(self) -> dict:
        """Loads already processed jobs to enable incremental delta-loading."""
        if os.path.exists(self.master_file):
            try:
                with open(self.master_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {"jobs": []}
        return {"jobs": []}

    def _get_latest_raw_file(self) -> str:
        files = glob.glob(os.path.join(self.raw_dir, "arbeitsagentur_germany_*.json"))
        if not files:
            raise FileNotFoundError("❌ Keine regionalen oder bundesweiten Rohdaten gefunden!")
        return max(files, key=os.path.getctime)

    def extract_salary(self, text: str, html_content: str = "") -> str:
        """Extracts salary indications using both raw text regex and structured lookups."""
        # 1. Regex für typische Gehaltsstrukturen (z.B. 72.000 € - 80.000 €, 65000 brutto)
        salary_pattern = r"(\d{2,3}\.\d{3})\s*€?\s*(?:-|bis)?\s*(\d{2,3}\.\d{3})\s*€?\s*/?\s*(?:jahr|monat|an)?"
        match = re.search(salary_pattern, text.lower())
        if match:
            return f"{match.group(1)} € - {match.group(2)} € / Jahr (Regex-Match)"
        
        # 2. Fallback: Suche nach dem isolierten Euro-Zeichen bei fünfstelligen Beträgen
        fallback_pattern = r"(\d{2,3}\.\d{3})\s*€"
        matches = re.findall(fallback_pattern, text)
        if len(matches) >= 2:
            return f"{matches[0]} € - {matches[1]} € / Jahr (Text-Funde)"
            
        return "Keine Angabe"

    def determine_experience_level(self, title: str, full_text: str) -> str:
        title_lower = title.lower()
        text_lower = full_text.lower()
        
        # Jahre extrahieren (identisch zu unserem v4-Erfolg)
        extracted_years = 0
        years_patterns = [
            r"(\d+)\s*(?:-\s*\d+)?\s*(?:\+|plus)?\s*jahr",
            r"(\d+)\s*(?:-\s*\d+)?\s*(?:\+|plus)?\s*year",
            r"berufserfahrung\s*(?:von|v.):?\s*(\d+)"
        ]
        for pattern in years_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                try:
                    max_found = max(int(m) for m in matches if m.isdigit())
                    if max_found > extracted_years: extracted_years = max_found
                except ValueError: continue

        if re.search(r"\b(lead|principal|chief|head|leiter|teamlead|manager)\b", title_lower): return "Lead"
        if re.search(r"\b(senior|sr\b|experienced|erfahren|professional)\b", title_lower): return "Senior"
        if re.search(r"\b(junior|jr\b|entry|trainee|absolvent|praktikum)\b", title_lower): return "Junior"

        if re.search(r"\b(teamleitung|abteilungsleitung|fachliche leitung)\b", text_lower) or extracted_years >= 7: return "Lead"
        if re.search(r"\b(senioren|langjährige erfahrung|expert)\b", text_lower) or 4 <= extracted_years < 7: return "Senior"
        if re.search(r"\b(berufseinstieg|traineeprogramm)\b", text_lower) or (0 < extracted_years <= 2 and "junior" in text_lower): return "Junior"

        return "Regular"

    def process_incremental_enrichment(self):
        raw_file = self._get_latest_raw_file()
        master_data = self._load_existing_master()
        
        # Erstelle ein Set aus bereits verarbeiteten Links für O(1) Lookups
        processed_links = {j.get("link") for j in master_data["jobs"] if j.get("link")}
        
        with open(raw_file, "r", encoding="utf-8") as f:
            raw_payload = json.load(f)
            
        raw_jobs = raw_payload.get("jobs", [])
        print(f"📖 Rohdaten geladen: {len(raw_jobs)} Jobs gefunden.")
        
        # Delta-Filterung: Nur Jobs verarbeiten, die noch NICHT im Master sind
        delta_jobs = [j for j in raw_jobs if j.get("link") not in processed_links]
        print(f"🔄 Delta-Analyse: {len(delta_jobs)} von {len(raw_jobs)} Jobs sind brandneu und werden verarbeitet.")
        
        if not delta_jobs:
            print("✨ Alles up-to-date! Keine neuen Datensätze zum Enricher-Abgleich vorhanden.")
            return

        print("🤖 Starte Headless Browser für kaskadierende Ingestion & Gehalts-Extraktion...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.user_agent)
            page = context.new_page()
            
            for i, job in enumerate(delta_jobs):
                primary_url = job.get("link")
                fallback_url = job.get("externe_url")
                title = job.get("title")
                company = job.get("company")
                
                print(f"[{i+1}/{len(delta_jobs)}] Verarbeite: {title} ({company})...")
                
                visible_text = ""
                salary_info = "Keine Angabe"
                target_url_used = primary_url
                
                # --- STRATEGIE: KASKADIERENDES SCRAPING ---
                # Versuch 1: Primärer Link (Arbeitsagentur)
                if primary_url and primary_url.startswith("http"):
                    try:
                        page.goto(primary_url, wait_until="commit", timeout=12000)
                        page.wait_for_timeout(500)
                        visible_text = page.locator("body").inner_text()
                        
                        # Prüfen, ob wir Gehaltsklassen direkt aus dem HTML-Baustand parsen können
                        salary_info = self.extract_salary(visible_text)
                    except Exception as e:
                        print(f"   ⚠ Primär-URL fehlgeschlagen. Wechsle auf Kaskaden-Fallback...")
                
                # Versuch 2: Fallback auf Externe URL, falls Primärseite leer oder geblockt war
                if (not visible_text or "fehler" in visible_text.lower()) and fallback_url and fallback_url.startswith("http"):
                    try:
                        print(f"   🔗 Rufe externe Kaskaden-URL auf: {fallback_url[:50]}...")
                        page.goto(fallback_url, wait_until="commit", timeout=12000)
                        page.wait_for_timeout(500)
                        visible_text = page.locator("body").inner_text()
                        target_url_used = fallback_url
                        
                        if salary_info == "Keine Angabe":
                            salary_info = self.extract_salary(visible_text)
                    except Exception as e:
                        print(f"   ⚠ Externe Kaskaden-URL ebenfalls fehlgeschlagen: {e}")

                # --- EXTRAKTION & MAPPING ---
                text_lower = visible_text.lower()
                
                # Tech Keywords scannen
                detected_techs = set()
                for tech, synonyms in self.tech_keywords.items():
                    for syn in synonyms:
                        if syn in text_lower:
                            detected_techs.add(tech)
                            break
                
                # Bildungs- und Studienkriterien filtern
                detected_edu = {}
                for criteria, synonyms in self.education_keywords.items():
                    detected_edu[criteria] = any(syn in text_lower for syn in synonyms)
                
                # Senioritätslevel bestimmen
                experience_level = self.determine_experience_level(title, visible_text)
                
                # Robustes Fallback-Mapping für Tech-Stack falls komplett ohne Text
                tech_list = list(detected_techs) if detected_techs else ["Klassische Tools / Offene Recherche"]

                enriched_job = {
                    "title": title,
                    "company": company,
                    "link": primary_url,
                    "externe_url": fallback_url,
                    "resolved_url": target_url_used,
                    "location": job.get("location"),
                    "remote_status": job.get("remote_status"),
                    "chiffrenummer": job.get("chiffrenummer"),
                    "veroeffentlicht_am": job.get("veroeffentlicht_am"),
                    "aenderungsdatum": job.get("aenderungsdatum"),
                    "experience_level": experience_level,
                    "salary_extracted": salary_info,
                    "technologies": tech_list,
                    "matches_wirtschaftsinformatik": detected_edu["Wirtschaftsinformatik"],
                    "verlangt_studium": detected_edu["Abgeschlossenes Studium"],
                    "processed_at": datetime.now().isoformat()
                }
                
                master_data["jobs"].append(enriched_job)
                
                # Zwischenspeichern alle 20 Jobs (Sicherheits-Checkpoints für große Datenmengen)
                if i % 20 == 0:
                    with open(self.master_file, "w", encoding="utf-8") as f:
                        json.dump(master_data, f, ensure_ascii=False, indent=4)

            browser.close()

        # Finales Abspeichern in die langlebige Master-Datei
        with open(self.master_file, "w", encoding="utf-8") as f:
            json.dump(master_data, f, ensure_ascii=False, indent=4)
            
        print(f"\n🎯 Inkrementelle Pipeline abgeschlossen! Datenpool unter: {self.master_file}")

if __name__ == "__main__":
    transformer = GermanyJobTransformer()
    transformer.process_incremental_enrichment()