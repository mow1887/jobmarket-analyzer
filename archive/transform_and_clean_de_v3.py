import json
import os
import glob
import re
from datetime import datetime
import time
from playwright.sync_api import sync_playwright

# AI Integration: Hugging Face Pipeline laden
try:
    from transformers import pipeline
    HAS_TRANSFORMERS = True
except ImportError:
    HAS_TRANSFORMERS = False

class GermanyJobTransformerV3:
    def __init__(self):
        self.raw_dir = "data/raw/arbeitsagentur"
        self.processed_dir = "data/processed"
        self.master_file = os.path.join(self.processed_dir, "master_enriched_jobs_de.json")
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # 1. Deine bewährte Regex-Matrix
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

        # 2. Initialisierung des deutschsprachigen Skill-Extraction Modells
        if HAS_TRANSFORMERS:
            print("🤗 Lade deutsches Token-Classification Modell für Skill-Erkennung...")
            self.ai_extractor = pipeline(
                "ner", 
                model="jjzha/escoxlmr_skill_extraction", 
                aggregation_strategy="simple"
            )
            print("✅ KI-Modell einsatzbereit!")
        else:
            print("⚠ 'transformers' nicht gefunden. KI-Extraktion deaktiviert (Fallback auf reine Regex).")
            self.ai_extractor = None

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
            raise FileNotFoundError("❌ Keine bundesweiten Rohdaten im Ordner gefunden!")
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
            r"berufserfahrung\s*(?:von|v.):?\s*(\d+)"
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

    def extract_ai_skills(self, text: str) -> list:
        """Uses the NLP Transformer to extract unmapped or flexible skills."""
        if not self.ai_extractor or not text or len(text) < 50:
            return []
        
        try:
            # Token-Limit-Schutz: Wir schneiden den Text auf die ersten 1800 Zeichen zu
            sliced_text = text[:1800]
            
            nlp_results = self.ai_extractor(sliced_text)
            ai_skills = set()
            
            for entity in nlp_results:
                # Da es ein dediziertes Skill-Modell ist, ist JEDE extrahierte Entität ein Skill.
                word = entity.get("word", "").strip()
                
                # Bereinigung von herkunftsspezifischen Subword-Tokens
                word = word.replace(" ", " ").replace("##", "").strip()
                
                # Schneller Filter gegen Satzzeichen, Zahlen und Stopwörter
                if len(word) > 2 and not word.lower() in ["und", "oder", "für", "mit", "von", "das", "die", "der", "eine", "einen"]:
                    ai_skills.add(word)
                        
            return list(ai_skills)
        except Exception as e:
            print(f"   ⚠ Fehler bei KI-Extraktion: {e}")
            return []

    def process_incremental_enrichment(self, process_all: bool = False):
        raw_file = self._get_latest_raw_file()
        master_data = self._load_existing_master()
        
        processed_links = set() if process_all else {j.get("link") for j in master_data["jobs"] if j.get("link")}
        
        if process_all:
            print("⚠️ [RESET MODE] Gesamter Pool wird in V3 neu verarbeitet!")
            master_data = {"jobs": []}

        with open(raw_file, "r", encoding="utf-8") as f:
            raw_payload = json.load(f)
            
        raw_jobs = raw_payload.get("jobs", [])
        print(f"📖 Rohdaten geladen: {len(raw_jobs)} Jobs im Staging.")
        
        delta_jobs = [j for j in raw_jobs if j.get("link") not in processed_links]
        
        # --- HIER IST DIE ANPASSUNG: LIMITIERUNG AUF 20 TEST-EINTRÄGE ---
        print(f"🔄 Delta-Analyse: {len(delta_jobs)} Jobs insgesamt gefunden.")
        delta_jobs = delta_jobs[:20] 
        print(f"🧪 TEST-MODUS: Beschränke Analyse auf die ersten {len(delta_jobs)} Jobs für schnelles Feedback.")
        # -----------------------------------------------------------------
        
        if not delta_jobs:
            print("✨ Alles up-to-date!")
            return

        print("🤖 Starte Browser für kaskadierendes Scraping...")
        
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
                
                # --- PRIO 1: Primärlink ---
                if primary_url and primary_url.startswith("http"):
                    try:
                        page.goto(primary_url, wait_until="domcontentloaded", timeout=10000)
                        page.wait_for_timeout(400)
                        visible_text = page.locator("body").inner_text()
                        salary_info = self.extract_salary(visible_text)
                    except Exception:
                        pass
                
                # --- PRIO 2: Kaskaden-Fallback mit v2-Smart-Wait ---
                is_cookie_or_empty = (
                    not visible_text 
                    or len(visible_text) < 300 
                    or "nicht mehr verfügbar" in visible_text.lower()
                    or ("cookie" in visible_text.lower() and "akzeptieren" in visible_text.lower() and len(visible_text) < 1000)
                )

                if is_cookie_or_empty and fallback_url and fallback_url.startswith("http"):
                    try:
                        print(f"   🔗 Kaskade aktiv. Rufe externes Portal auf...")
                        page.goto(fallback_url, wait_until="networkidle", timeout=15000)
                        page.wait_for_timeout(1500)
                        visible_text = page.locator("body").inner_text()
                        target_url_used = fallback_url
                        
                        if salary_info == "Keine Angabe":
                            salary_info = self.extract_salary(visible_text)
                    except Exception as e:
                        print(f"   ⚠ Kaskade fehlgeschlagen: {e}")

                # --- HYBRID EXTRACTOR ---
                text_lower = visible_text.lower()
                
                # Schritt A: Regex-Matching
                detected_techs = set()
                for tech, synonyms in self.tech_keywords.items():
                    for syn in synonyms:
                        if syn in text_lower:
                            detected_techs.add(tech)
                            break
                
                # Schritt B: KI-NLP-Matching (Jetzt mit gefixtem Label-Filter)
                ai_extracted = self.extract_ai_skills(visible_text)
                
                # Schritt C: Formelle Kriterien
                detected_edu = {}
                for criteria, synonyms in self.education_keywords.items():
                    detected_edu[criteria] = any(syn in text_lower for syn in synonyms)
                
                experience_level = self.determine_experience_level(title, visible_text)
                tech_list = list(detected_techs) if detected_techs else ["Klassische Tools / Offene Recherche"]

                # JSON-Mapping
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
                    "ai_extracted_skills": ai_extracted, 
                    "matches_wirtschaftsinformatik": detected_edu["Wirtschaftsinformatik"],
                    "matches_informatik": detected_edu["Informatik"],
                    "matches_mathematik_statistik": detected_edu["Mathematik/Statistik"],
                    "verlangt_studium": detected_edu["Abgeschlossenes Studium"],
                    "requires_phd": detected_edu["Promotion/PhD"],
                    "eu_ai_act_relevant": detected_edu["EU AI Act Compliance"],
                    "data_governance_required": detected_edu["Data Governance & Privacy"],
                    "focuses_on_data_quality": detected_edu["Data Quality & QA"],
                    "processed_at": datetime.now().isoformat()
                }
                
                master_data["jobs"].append(enriched_job)
                
                if i % 5 == 0: # Checkpoints bei kleinen Datensätzen häufiger machen
                    with open(self.master_file, "w", encoding="utf-8") as f:
                        json.dump(master_data, f, ensure_ascii=False, indent=4)

            browser.close()

        with open(self.master_file, "w", encoding="utf-8") as f:
            json.dump(master_data, f, ensure_ascii=False, indent=4)
            
        print(f"\n🎯 [SUCCESS] Test-Lauf abgeschlossen! Daten unter: {self.master_file}")

if __name__ == "__main__":
    transformer = GermanyJobTransformerV3()
    transformer.process_incremental_enrichment(process_all=True)