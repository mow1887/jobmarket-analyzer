import json
import os
import glob
from datetime import datetime
import time
from playwright.sync_api import sync_playwright

class JobDataEnricher:
    def __init__(self):
        self.raw_dir = "data/raw/stepstone"
        self.processed_dir = "data/processed"
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Technology matrix for multi-label filtering
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
            "Kubernetes": ["kubernetes", "k8s"]
        }
        
        # Hierarchische Keywords für die Bestimmung der Seniorität
        self.experience_keywords = {
            "Lead": ["lead", "principal", "chief", "leiter", "head of", "teamlead"],
            "Senior": ["senior", "sr.", "sr ", "experienced", "erfahren", "professional"],
            "Junior": ["junior", "jr.", "jr ", "entry level", "berufseinstieg", "trainee", "absolvent"]
        }
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def _get_latest_raw_file(self) -> str:
        files = glob.glob(os.path.join(self.raw_dir, "stepstone_hamburg_V4_*.json"))
        if not files:
            raise FileNotFoundError("❌ No recent V4 raw data found!")
        return max(files, key=os.path.getctime)

    def extract_all_technologies(self, text: str) -> list:
        text_lower = text.lower()
        found_techs = set()
        for standardized_name, synonyms in self.tech_keywords.items():
            for synonym in synonyms:
                if synonym in text_lower:
                    found_techs.add(standardized_name)
                    break
        return list(found_techs)

    def determine_experience_level(self, title: str, full_text: str) -> str:
        """
        Evaluates the job title and description context to classify the seniority.
        Prioritizes the title as it contains the most explicit intent.
        """
        title_lower = title.lower()
        text_lower = full_text.lower()
        
        # 1. Check Title first (highest precision)
        for level, keywords in self.experience_keywords.items():
            for keyword in keywords:
                if keyword in title_lower:
                    return level
                    
        # 2. Fallback to Full Text analysis if title is generic
        for level, keywords in self.experience_keywords.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return level
                    
        # Default level if no specific tags are found
        return "Regular"

    def process_dataset(self):
        raw_file_path = self._get_latest_raw_file()
        print(f"📖 Loading raw ingestion data from: {raw_file_path}")
        
        with open(raw_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        jobs = data.get("jobs", [])
        enriched_dataset = []
        
        print(f"🤖 Launching Headless Browser for Text & Seniority Extraction...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.headers["User-Agent"])
            page = context.new_page()
            
            for i, job in enumerate(jobs):
                url = job.get("link")
                title = job.get("title")
                company = job.get("company")
                
                print(f"[{i+1}/{len(jobs)}] Processing: {title} ({company})...")
                
                visible_text = ""
                detected_techs = []
                experience_level = "Regular"
                
                if url and url.startswith("http"):
                    try:
                        page.goto(url, wait_until="commit", timeout=15000)
                        page.wait_for_timeout(1000) 
                        
                        visible_text = page.locator("body").inner_text()
                        
                        # Parse Tech Stack and Experience Level
                        detected_techs = self.extract_all_technologies(visible_text)
                        experience_level = self.determine_experience_level(title, visible_text)
                            
                    except Exception as e:
                        print(f"   ⚠ Text extraction bypassed: {e}")
                
                # Sichert Absicherung falls Navigation komplett fehlschlug
                if not detected_techs:
                    detected_techs = self.extract_all_technologies(title)
                if not detected_techs:
                    detected_techs = ["Klassische Tools / Sonstige"]
                    
                if experience_level == "Regular" and visible_text == "":
                    experience_level = self.determine_experience_level(title, "")

                print(f"   📊 [ENRICHED] Level: {experience_level} | Techs: {len(detected_techs)}")

                enriched_job = {
                    "title": title,
                    "company": company,
                    "link": url,
                    "remote_status": job.get("remote_status"),
                    "location": job.get("location"),
                    "experience_level": experience_level, # Neues, sauberes Klassifizierungsfeld
                    "technologies": detected_techs,
                    "analyzed_at": datetime.now().isoformat()
                }
                enriched_dataset.append(enriched_job)
                time.sleep(0.5)
                
            browser.close()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.processed_dir, f"enriched_hamburg_jobmarket_{timestamp}.json")
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump({"jobs": enriched_dataset}, f, ensure_ascii=False, indent=4)
            
        print(f"\n🎯 Pipeline Complete! Enriched dataset safely stored under: {output_file}")


if __name__ == "__main__":
    enricher = JobDataEnricher()
    enricher.process_dataset()