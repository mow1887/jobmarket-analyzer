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
        # Paths for the V3 staging zone (switching to Silver Layer nomenclature)
        self.raw_dir = "data/raw/arbeitsagentur"
        self.silver_dir = "data/silver"
        self.master_file = os.path.join(self.silver_dir, "master_enriched_jobs_de.json")
        os.makedirs(self.silver_dir, exist_ok=True)
        
        # AWS S3 Configuration
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name="eu-central-1"
        )
        self.bucket_name = "jobmarket-analyzer-data-lake"
        self.s3_key = "silver/master_enriched_jobs_de.json"
        
        # Optimized tech keyword matrix (matching German job postings)
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
            "ETL/ELT": ["etl", "elt", "data pipeline", "datenpipeline", "data integration"],
            "Airflow": ["airflow", "apache airflow"],
            "Prefect/Dagster": ["prefect", "dagster"],
            "dbt": ["dbt", "data build tool", "data-build-tool"],
            "Webhooks/APIs": ["webhook", "webhooks", "rest api", "rest-api", "api integration"],
            "n8n/Low-Code": ["n8n", "low-code", "low code", "workflow-automatisierung", "make.com", "zapier"],
            "Spark": ["spark", "apache spark", "pyspark"],
            "Databricks": ["databricks"],
            "Kafka": ["kafka", "apache kafka", "event streaming", "data streaming"],
            "Snowflake": ["snowflake", "snowflake data cloud"],
            "Microsoft Fabric": ["fabric", "microsoft fabric", "ms fabric"],
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
            "Deep Learning": ["deep learning", "tensorflow", "keras", "pytorch", "neural networks", "neuronale netze"],
            "NLP": ["nlp", "natural language processing", "text mining", "textanalyse"],
            "Time Series": ["time series", "zeitreihen", "zeitreihenanalyse", "forecasting"],
            "LLM & Prompting": ["llm", "large language model", "prompt engineering", "structured prompting", "prompting", "gpt-4", "claude"],
            "AI Agents": ["ai agents", "ki-agenten", "ki agenten", "autonomous agents", "langchain", "crewai", "autogen"],
            "RAG & Vector DBs": ["rag", "retrieval-augmented", "knowledge engineering", "rag canvas", "vector database", "chromadb", "pinecone"]
        }
        
        # Criteria matrix for formal requirements (matching German job postings)
        self.education_keywords = {
            "Wirtschaftsinformatik": ["wirtschaftsinformatik", "business informatics"],
            "Informatik": ["informatik", "computer science", "tech-studium", "software engineering"],
            "Mathematik/Statistik": ["mathematik", "statistik", "mathematics", "statistics", "data science studium"],
            "Abgeschlossenes Studium": ["abgeschlossenes studium", "hochschulstudium", "universitätsabschluss", "bachelor", "master", "diplom", "studium", "degree", "university degree"],
            "Bachelor": ["bachelor", "b.sc.", "b.a."],
            "Master": ["master", "m.sc.", "m.a."],
            "Promotion/PhD": ["phd", "promotion", "dr.", "doctorate", "wissenschaftlicher mitarbeiter"],
            "EU AI Act Compliance": ["eu ai act", "ai act", "ki-gesetz", "ki governance", "ai governance", "ai-act"],
            "Data Governance & Privacy": ["data governance", "daten-governance", "datenschutz", "gdpr", "dsgvo", "data privacy"],
            "Data Quality & QA": ["data quality", "datenqualität", "data qa", "data cleaning", "datenbereinigung"]
        }

        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    def download_master_from_s3(self):
        """Fetches the existing master pool from the S3 Data Lake."""
        try:
            print("🪣  Downloading existing master pool from AWS S3...")
            self.s3_client.download_file(self.bucket_name, self.s3_key, self.master_file)
            print("✅ Master pool successfully downloaded from S3.")
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("ℹ️ No master file found in S3 bucket. Initializing empty pool.")
                with open(self.master_file, "w", encoding="utf-8") as f:
                    json.dump({"jobs": []}, f)
            else:
                print(f"❌ Unexpected AWS S3 error during download: {e}")
                raise e

    def upload_master_to_s3(self):
        """Secures the updated master dataset in the central S3 bucket."""
        try:
            print("🪣  Synchronizing updated master data with AWS S3...")
            self.s3_client.upload_file(self.master_file, self.bucket_name, self.s3_key)
            print("🚀 [SUCCESS] Upload completed. AWS S3 Data Lake is up-to-date!")
        except Exception as e:
            print(f"❌ Error during AWS S3 upload: {e}")

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
            raise FileNotFoundError("❌ No raw data found in the ingestion folder!")
        return max(files, key=os.path.getctime)

    def extract_salary(self, text: str) -> str:
        salary_pattern = r"(\d{2,3}\.\d{3})\s*€?\s*(?:-|bis)?\s*(\d{2,3}\.\d{3})\s*€?\s*/?\s*(?:jahr|monat|an)?"
        match = re.search(salary_pattern, text.lower())
        if match:
            return f"{match.group(1)} € - {match.group(2)} € / Year"
        
        fallback_pattern = r"(\d{2,3}\.\d{3})\s*€"
        matches = re.findall(fallback_pattern, text)
        if len(matches) >= 2:
            return f"{matches[0]} € - {matches[1]} € / Year"
            
        return "Not specified"

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
        if not process_all:
            self.download_master_from_s3()
            
        raw_file = self._get_latest_raw_file()
        master_data = self._load_existing_master()
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        if process_all:
            print("⚠️  [RESET MODE] Entire pool is being rebuilt locally and overwritten!")
            master_data = {"jobs": []}

        with open(raw_file, "r", encoding="utf-8") as f:
            raw_payload = json.load(f)
            
        raw_jobs = raw_payload.get("jobs", [])
        print(f"📖 Raw data loaded from staging: {len(raw_jobs)} jobs found.")
        
        master_dict = {j["link"]: j for j in master_data.get("jobs", []) if j.get("link")}
        delta_jobs = []
        
        for job in raw_jobs:
            link = job.get("link")
            if not link:
                continue
                
            if link in master_dict:
                master_dict[link]["last_seen"] = today_str
                master_dict[link]["remote_status"] = job.get("remote_status") or master_dict[link]["remote_status"]
                master_dict[link]["modification_date"] = job.get("modification_date") or master_dict[link].get("modification_date")
            else:
                delta_jobs.append(job)

        print(f"🔄 Delta analysis active: {len(delta_jobs)} brand new jobs will be analyzed in depth.")
        
        if not delta_jobs:
            print("✨ S3 dataset and local ingestion are synchronized. No new jobs to process.")
            master_data["jobs"] = list(master_dict.values())
            with open(self.master_file, "w", encoding="utf-8") as f:
                json.dump(master_data, f, ensure_ascii=False, indent=4)
            self.upload_master_to_s3()
            return

        print("🤖 Starting headless browser for content-based cascade extraction...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.user_agent)
            page = context.new_page()
            
            for i, job in enumerate(delta_jobs):
                title = job.get("title") or "Data Professional"
                company = job.get("company") or "Exciting Company"
                primary_url = job.get("link")
                fallback_url = job.get("external_url")
                
                print(f"[{i+1}/{len(delta_jobs)}] Processing: {title} ({company})...")
                
                visible_text = ""
                salary_info = "Not specified"
                target_url_used = primary_url
                detected_techs = set()
                
                # --- PRIO 1: Read main page of the Bundesagentur für Arbeit ---
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
                                    # 🔥 ENHANCED: Word boundary safeguards for short substrings
                                    if syn in ["etl", "elt"] and not re.search(r"\b(etl|elt)\b", text_lower_ba):
                                        continue
                                    if syn == "rag" and not re.search(r"\brag\b", text_lower_ba):
                                        continue
                                    if syn == "git" and not re.search(r"\bgit\b", text_lower_ba):
                                        continue
                                    if syn == "ml" and not re.search(r"\bml\b", text_lower_ba):
                                        continue
                                    detected_techs.add(tech)
                                    break
                    except Exception:
                        pass
                
                # --- PRIO 2: CONTENT-BASED CASCADE CHECK ---
                text_lower_primary = visible_text.lower() if visible_text else ""
                is_placeholder_or_empty = (
                    not visible_text 
                    or len(detected_techs) == 0 
                    or "kooperationspartner" in text_lower_primary 
                    or (fallback_url and "ams.at" in fallback_url)
                )

                if is_placeholder_or_empty and fallback_url and fallback_url.startswith("http") and fallback_url != "No external URL":
                    try:
                        print(f"    🔗 Cascade triggered. Deep scan on external portal...")
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
                                        # 🔥 ENHANCED: Word boundary safeguards for short substrings
                                        if syn in ["etl", "elt"] and not re.search(r"\b(etl|elt)\b", text_lower_ext):
                                            continue
                                        if syn == "rag" and not re.search(r"\brag\b", text_lower_ext):
                                            continue
                                        if syn == "git" and not re.search(r"\bgit\b", text_lower_ext):
                                            continue
                                        if syn == "ml" and not re.search(r"\bml\b", text_lower_ext):
                                            continue
                                        detected_techs.add(tech)
                                        break
                            
                            if salary_info == "Not specified":
                                salary_info = self.extract_salary(visible_text)
                    except Exception as e:
                        print(f"    ⚠ Cascade fallback failed or blocked: {e}")

                # --- FINAL TEXT MAPPING & FORMALITIES ---
                text_lower = re.sub(r'\s+', ' ', visible_text.lower())
                
                detected_edu = {}
                for criteria, synonyms in self.education_keywords.items():
                    detected_edu[criteria] = any(syn in text_lower for syn in synonyms)
                
                experience_level = self.determine_experience_level(title, visible_text)
                tech_list = list(detected_techs) if detected_techs else ["Classic Tools / Open Research"]

                enriched_job = {
                    "title": title,
                    "company": company,
                    "link": primary_url,
                    "external_url": fallback_url,
                    "resolved_url": target_url_used,
                    "location": job.get("location"),
                    "remote_status": job.get("remote_status"),
                    "chiffrenummer": job.get("chiffrenummer"),
                    "published_at": job.get("published_at"),
                    "modification_date": job.get("modification_date"),
                    "experience_level": experience_level,
                    "salary_extracted": salary_info,
                    "technologies": tech_list,
                    "matches_wirtschaftsinformatik": detected_edu.get("Wirtschaftsinformatik", False),
                    "matches_informatik": detected_edu.get("Informatik", False),
                    "matches_mathematik_statistik": detected_edu.get("Mathematik/Statistik", False),
                    "verlangt_studium": detected_edu.get("Abgeschlossenes Studium", False),
                    "requires_bachelor": detected_edu.get("Bachelor", False), 
                    "requires_master": detected_edu.get("Master", False),     
                    "requires_phd": detected_edu.get("Promotion/PhD", False),
                    "eu_ai_act_relevant": detected_edu.get("EU AI Act Compliance", False),
                    "data_governance_required": detected_edu.get("Data Governance & Privacy", False),
                    "focuses_on_data_quality": detected_edu.get("Data Quality & QA", False),
                    "processed_at": datetime.now().isoformat(),
                    "first_seen": today_str,
                    "last_seen": today_str
                }
                
                master_dict[primary_url] = enriched_job
                
                if i % 50 == 0:
                    master_data["jobs"] = list(master_dict.values())
                    with open(self.master_file, "w", encoding="utf-8") as f:
                        json.dump(master_data, f, ensure_ascii=False, indent=4)

            browser.close()

        final_jobs_list = list(master_dict.values())
        master_data["metadata"] = {
            "source": "rest.arbeitsagentur.de (Smart Multi-Cluster V3)",
            "last_updated_at": datetime.now().isoformat(),
            "total_jobs": len(final_jobs_list)
        }
        master_data["jobs"] = final_jobs_list

        with open(self.master_file, "w", encoding="utf-8") as f:
            json.dump(master_data, f, ensure_ascii=False, indent=4)
            
        print(f"\n🎯 Transformation completed locally! File saved under: {self.master_file}")
        print(f"📊 Total pool now contains {len(final_jobs_list)} seamlessly historized jobs.")
        
        self.upload_master_to_s3()

if __name__ == "__main__":
    transformer = GermanyJobTransformerV3()
    transformer.process_incremental_enrichment(process_all=False)