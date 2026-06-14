import json
import os
import glob
import re
from datetime import datetime
import time
from playwright.sync_api import sync_playwright
import boto3
from botocore.exceptions import ClientError

class GermanyJobTransformerV3:
    def __init__(self):
        # Pfade für die V3 Staging-Zone (Wechsel auf Silver-Layer Nomenklatur)
        self.raw_dir = "data/raw/arbeitsagentur"
        self.silver_dir = "data/silver"
        self.master_file = os.path.join(self.silver_dir, "master_enriched_jobs_de.json")
        os.makedirs(self.silver_dir, exist_ok=True)
        
        # AWS S3 Konfiguration
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name="eu-central-1"
        )
        self.bucket_name = "jobmarket-analyzer-data-lake"
        self.s3_key = "silver/master_enriched_jobs_de.json"
        
        # Deine optimierte Tech-Keyword-Matrix
        self.tech_keywords = {
            "Python": ["python", "pandas", "numpy", "scikit", "pyspark", "scipy", "statsmodels"],
            "R": [" r ", "r-studio", "rstudio", "ggplot2"],
            "SQL": ["sql", "postgres", "mysql", "oracle", "pl/sql", "tsql", "sqlite", "mariadb"],
            "Unix/Shell": ["unix", "command line", "kommandozeile", "bash", "shell", "powershell"],
            "Git/Version Control": ["git", "github", "gitlab", "bitbucket", "versionskontrolle"],
            "AWS": ["aws", "amazon web services", "s3", "ec2", "rds", "redshift", "glue", "lambda", "iam", "vpc"],
            "GCP": ["gcp", "google cloud", "google cloud platform", "bigquery", "vertex ai", "looker studio"],
            "Azure": ["azure", "microsoft azure", "synapse", "data factory", "azure devops"],
            "Terraform": ["terraform", "infrastructure as code", "iac"],
            "ETL/ELT": ["etl", "elt", "data pipeline", "datenpipeline", "integration", "data integration"],
            "Airflow": ["airflow", "apache airflow"],
            "Prefect/Dagster": ["prefect", "dagster"],
            "dbt": ["dbt", "data build tool", "data-build-tool"],
            "Webhooks/APIs": ["webhook", "webhooks", "rest api", "rest-api", "api integration"],
            "n8n/Low-Code": ["n8n", "low-code", "low code", "workflow-automatisierung", "make.com", "zapier"],
            "Spark": ["spark", "apache spark", "pyspark", "databricks"],
            "Databricks": ["databricks"],
            "Kafka": ["kafka", "apache kafka", "event streaming", "data streaming"],
            "Snowflake": ["snowflake", "snowflake data cloud"],
            "Hadoop/Hive": ["hadoop", "hive", "mapreduce"],
            "NoSQL": ["nosql", "mongodb", "cassandra", "redis", "dynamodb"],
            "Docker": ["docker", "docker-compose", "docker compose"],
            "Kubernetes": ["kubernetes", "k8s", "helm"],
            "Power BI": ["power bi", "powerbi", "ms power bi", "dax"],
            "Tableau": ["tableau", "tableau desktop", "tableau server"],
            "Metabase": ["metabase"],
            "Qlik": ["qlik", "qlikview", "qliksense", "qlik sense"],
            "Looker": ["looker", "google looker"],
            "Excel": ["excel", "tabellenkalkulation", "vba", "spreadsheets"],
            "Machine Learning": ["machine learning", "ml", "scikit-learn", "regression", "classification", "clustering", "k-means", "random forest"],
            "Deep Learning": ["deep learning", "tensorflow", "keras", "pytorch", "neural networks", "neuronale netze", "ann", "rnn"],
            "NLP": ["nlp", "natural language processing", "text mining", "textanalyse"],
            "Time Series": ["time series", "zeitreihen", "zeitreihenanalyse", "forecasting"],
            "LLM & Prompting": ["llm", "large language model", "prompt engineering", "structured prompting", "prompting", "gpt-4", "claude"],
            "AI Agents": ["ai agents", "ki-agenten", "ki agenten", "autonomous agents", "langchain", "crewai", "autogen"],
            "RAG & Vector DBs": ["rag", "retrieval-augmented", "knowledge engineering", "rag canvas", "vector database", "chromadb", "pinecone"]
        }
        
        # Deine Kriterien-Matrix für formelle Anforderungen
        self.education_keywords = {
            "Wirtschaftsinformatik": ["wirtschaftsinformatik", "business informatics"],
            "Informatik": ["informatik", "computer science", "tech-studium", "software engineering"],
            "Mathematik/Statistik": ["mathematik", "statistik", "mathematics", "statistics", "data science studium"],
            "Abgeschlossenes Studium": ["abgeschlossenes studium", "hochschulstudium", "universitätsabschluss", "bachelor", "master", "diplom", "studium", "degree", "university degree"],
            "Promotion/PhD": ["phd", "promotion", "dr.", "doctorate", "wissenschaftlicher mitarbeiter"],
            "EU AI Act Compliance": ["eu ai act", "ai act", "ki-gesetz", "ki governance", "ai governance", "ai-act"],
            "Data Governance & Privacy": ["data governance", "daten-governance", "datenschutz", "gdpr", "dsgvo", "data privacy"],
            "Data Quality & QA": ["data quality", "datenqualität", "data qa", "data cleaning", "datenbereinigung"]
        }

        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    def download_master_from_s3(self):
        """Holt den bestehenden Master-Pool aus dem S3 Data Lake."""
        try:
            print("🪣  Lade bestehenden Master-Pool aus AWS S3...")
            self.s3_client.download_file(self.bucket_name, self.s3_key, self.master_file)
            print("✅ Master-Pool erfolgreich aus S3 heruntergeladen.")
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("ℹ️ Keine Master-Datei im S3 Bucket gefunden. Initialisiere leeren Pool.")
                with open(self.master_file, "w", encoding="utf-8") as f:
                    json.dump({"jobs": []}, f)
            else:
                print(f"❌ Unerwarteter AWS S3 Fehler beim Download: {e}")
                raise e

    def upload_master_to_s3(self):
        """Sichert den aktualisierten Master-Datenbestand im zentralen S3 Bucket."""
        try:
            print("🪣  Synchronisiere aktualisierte Master-Daten mit AWS S3...")
            self.s3_client.upload_file(self.master_file, self.bucket_name, self.s3_key)
            print("🚀 [SUCCESS] Upload abgeschlossen. AWS S3 Data Lake ist up-to-date!")
        except Exception as e:
            print(f"❌ Fehler beim AWS S3 Upload: {e}")

    def _load_existing_master(self) -> dict:
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
            raise FileNotFoundError("❌ Keine Rohdaten im Ingestion-Ordner gefunden!")
        return max(files, key=os.path.getctime)

    def extract_salary(self, text: str) -> str:
        salary_pattern = r"(\d{2,3}\.\d{3})\s*€?\s*(?:-|bis)?\s*(\d{2,3}\.\d{3})\s*€?\s*/?\s*(?:jahr|monat|an)?"
        match = re.search(salary_pattern, text.lower())
        if match:
            return f"{match.group(1)} € - {match.group(2)} € / Jahr"
        
        fallback_pattern = r"(\d{2,3}\.\d{3})\s*€"
        matches = re.findall(fallback_pattern, text)
        if len(matches) >= 2:
            return f"{matches[0]} € - {matches[1]} € / Jahr"
            
        return "Keine Angabe"

    def determine_experience_level(self, title: str, full_text: str) -> str:
        title_lower = title.lower()
        text_lower = full_text.lower()
        
        extracted_years = 0
        years_patterns = [
            r"(\d+)\s*(?:-\s*\d+)?\s*(?:\+|plus)?\s*jahr",
            r"(\d+)\s*(?:-\s*\d+)?\s*(?:\+|plus)?\s*year",
            r"berufserfahrung\s*(?:von|v.):ql*(\d+)"
        ]
        for pattern in years_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                try:
                    max_found = max(int(m) for m in matches if m.isdigit())
                    if max_found > extracted_years: extracted_years = max_found
                except ValueError: continue

        if re.search(r"\b(lead|principal|chief|head|leiter|teamlead|manager|architect|cto)\b", title_lower): return "Lead"
        if re.search(r"\b(senior|sr\b|experienced|erfahren|professional|expert)\b", title_lower): return "Senior"
        if re.search(r"\b(junior|jr\b|entry|trainee|absolvent|praktikum|praktikant|associate)\b", title_lower): return "Junior"

        if re.search(r"\b(teamleitung|abteilungsleitung|fachliche leitung|führungsverantwortung|personalverantwortung)\b", text_lower) or extracted_years >= 7: return "Lead"
        if re.search(r"\b(senioren|langjährige erfahrung|experte|expertin)\b", text_lower) or 4 <= extracted_years < 7: return "Senior"
        if re.search(r"\b(berufseinstieg|traineeprogramm|erste erfahrungen)\b", text_lower) or (0 < extracted_years <= 2 and "junior" in text_lower): return "Junior"

        return "Regular"

    def process_incremental_enrichment(self, process_all: bool = False):
        # 1. Vorab-Synchronisation mit der Cloud
        if not process_all:
            self.download_master_from_s3()
            
        raw_file = self._get_latest_raw_file()
        master_data = self._load_existing_master()
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        if process_all:
            print("⚠️  [RESET MODE] Gesamter Pool wird lokal neu aufgebaut und überschrieben!")
            master_data = {"jobs": []}

        with open(raw_file, "r", encoding="utf-8") as f:
            raw_payload = json.load(f)
            
        raw_jobs = raw_payload.get("jobs", [])
        print(f"📖 Rohdaten aus Staging geladen: {len(raw_jobs)} Jobs gefunden.")
        
        # In-Memory Mapping über Dictionary für schnellen O(1) Zugriff
        master_dict = {j["link"]: j for j in master_data.get("jobs", []) if j.get("link")}
        
        delta_jobs = []
        
        # --- Timestamps & Inkrementelles Lifecycle-Tracking ---
        for job in raw_jobs:
            link = job.get("link")
            if not link:
                continue
                
            if link in master_dict:
                # Job ist heute wieder in der API -> Er ist aktiv! last_seen auf heute setzen
                master_dict[link]["last_seen"] = today_str
                # Flüchtige Meta-Updates mitnehmen
                master_dict[link]["remote_status"] = job.get("remote_status") or master_dict[link]["remote_status"]
                master_dict[link]["aenderungsdatum"] = job.get("aenderungsdatum") or master_dict[link]["aenderungsdatum"]
            else:
                # Job ist komplett neu -> Muss tiefenanalysiert werden
                delta_jobs.append(job)

        print(f"🔄 Delta-Analyse aktiv: {len(delta_jobs)} brandneue Jobs werden tiefenanalysiert.")
        
        if not delta_jobs:
            print("✨ S3 Datenbestand und lokale Ingestion sind synchron. Keine neuen Jobs zu verarbeiten.")
            # Dennoch Master abspeichern, da bestehende Jobs ein last_seen-Update bekommen haben könnten
            master_data["jobs"] = list(master_dict.values())
            with open(self.master_file, "w", encoding="utf-8") as f:
                json.dump(master_data, f, ensure_ascii=False, indent=4)
            self.upload_master_to_s3()
            return

        print("🤖 Starte Headless Browser für inhaltsbasierte Kaskaden-Extraktion...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.user_agent)
            page = context.new_page()
            
            for i, job in enumerate(delta_jobs):
                title = job.get("title") or "Data Professional"
                company = job.get("company") or "Spannendes Unternehmen"
                primary_url = job.get("link")
                fallback_url = job.get("externe_url")
                
                print(f"[{i+1}/{len(delta_jobs)}] Verarbeite: {title} ({company})...")
                
                visible_text = ""
                salary_info = "Keine Angabe"
                target_url_used = primary_url
                detected_techs = set()
                
                # --- PRIO 1: Hauptseite der Bundesagentur für Arbeit einlesen ---
                if primary_url and primary_url.startswith("http"):
                    try:
                        page.goto(primary_url, wait_until="domcontentloaded", timeout=10000)
                        page.wait_for_timeout(400)
                        visible_text = page.locator("body").inner_text()
                        salary_info = self.extract_salary(visible_text)
                        
                        text_lower_ba = re.sub(r'\s+', ' ', visible_text.lower())
                        for tech, synonyms in self.tech_keywords.items():
                            for syn in synonyms:
                                if syn in text_lower_ba:
                                    if syn == "rag" and not re.search(r"\brag\b", text_lower_ba):
                                        continue
                                    detected_techs.add(tech)
                                    break
                    except Exception:
                        pass
                
                # --- PRIO 2: INHALTSBASIERTER KASKADEN-CHECK ---
                text_lower_primary = visible_text.lower() if visible_text else ""
                is_placeholder_or_empty = (
                    not visible_text 
                    or len(detected_techs) == 0 
                    or "kooperationspartner" in text_lower_primary 
                    or (fallback_url and "ams.at" in fallback_url)
                )

                if is_placeholder_or_empty and fallback_url and fallback_url.startswith("http") and fallback_url != "Keine externe URL":
                    try:
                        print(f"    🔗 Kaskade getriggert. Deep Scan auf externem Portal...")
                        page.goto(fallback_url, wait_until="networkidle", timeout=15000)
                        page.wait_for_timeout(1500)
                        
                        combined_text = ""
                        for frame in page.frames:
                            try:
                                frame_text = frame.locator("body").inner_text()
                                if frame_text:
                                    combined_text += "\n" + frame_text
                            except Exception:
                                continue
                        
                        if combined_text and len(combined_text) > 200:
                            visible_text = combined_text
                            target_url_used = fallback_url
                            
                            detected_techs = set() 
                            text_lower_ext = re.sub(r'\s+', ' ', visible_text.lower())
                            for tech, synonyms in self.tech_keywords.items():
                                for syn in synonyms:
                                    if syn in text_lower_ext:
                                        if syn == "rag" and not re.search(r"\brag\b", text_lower_ext):
                                            continue
                                        detected_techs.add(tech)
                                        break
                            
                            if salary_info == "Keine Angabe":
                                salary_info = self.extract_salary(visible_text)
                    except Exception as e:
                        print(f"    ⚠ Kaskaden-Fallback fehlgeschlagen oder geblockt: {e}")

                # --- FINALES TEXT-MAPPING & FORMALIA ---
                text_lower = re.sub(r'\s+', ' ', visible_text.lower())
                
                detected_edu = {}
                for criteria, synonyms in self.education_keywords.items():
                    detected_edu[criteria] = any(syn in text_lower for syn in synonyms)
                
                experience_level = self.determine_experience_level(title, visible_text)
                tech_list = list(detected_techs) if detected_techs else ["Klassische Tools / Offene Recherche"]

                # Erstellung des strukturierten JSON-Objekts mit Injektion der Zeitstempel
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
                    "matches_wirtschaftsinformatik": detected_edu.get("Wirtschaftsinformatik", False),
                    "matches_informatik": detected_edu.get("Informatik", False),
                    "matches_mathematik_statistik": detected_edu.get("Mathematik/Statistik", False),
                    "verlangt_studium": detected_edu.get("Abgeschlossenes Studium", False),
                    "requires_phd": detected_edu.get("Promotion/PhD", False),
                    "eu_ai_act_relevant": detected_edu.get("EU AI Act Compliance", False),
                    "data_governance_required": detected_edu.get("Data Governance & Privacy", False),
                    "focuses_on_data_quality": detected_edu.get("Data Quality & QA", False),
                    "processed_at": datetime.now().isoformat(),
                    # 🔥 TIMESTAMPS FÜR HISTORISIERUNG INJIZIEREN
                    "first_seen": today_str,
                    "last_seen": today_str
                }
                
                # In den Master-Pool übernehmen
                master_dict[primary_url] = enriched_job
                
                # Checkpoint-Backup alle 50 Jobs gegen Datenverlust schreiben
                if i % 50 == 0:
                    master_data["jobs"] = list(master_dict.values())
                    with open(self.master_file, "w", encoding="utf-8") as f:
                        json.dump(master_data, f, ensure_ascii=False, indent=4)

            browser.close()

        # 4. Zusammenführung & Metadaten schreiben
        final_jobs_list = list(master_dict.values())
        master_data["metadata"] = {
            "source": "rest.arbeitsagentur.de (Smart Multi-Cluster V3)",
            "last_updated_at": datetime.now().isoformat(),
            "total_jobs": len(final_jobs_list)
        }
        master_data["jobs"] = final_jobs_list

        # Finale lokale Speicherung
        with open(self.master_file, "w", encoding="utf-8") as f:
            json.dump(master_data, f, ensure_ascii=False, indent=4)
            
        print(f"\n🎯 Transformation lokal abgeschlossen! Datei gesichert unter: {self.master_file}")
        print(f"📊 Gesamtpool enthält jetzt {len(final_jobs_list)} lückenlos historisierte Jobs.")
        
        # 5. Finaler Cloud-Upload in den Silver Layer
        self.upload_master_to_s3()

if __name__ == "__main__":
    transformer = GermanyJobTransformerV3()
    transformer.process_incremental_enrichment(process_all=False)