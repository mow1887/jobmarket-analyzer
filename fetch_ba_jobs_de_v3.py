import json
import os
import ssl
import urllib.request
import urllib.parse
from datetime import datetime
import time
import boto3  # NEU: Für die AWS S3 Anbindung
from botocore.exceptions import ClientError  # NEU: Für S3-Fehlermeldungen

class BundesAgenturSmartPipeline:
    def __init__(self):
        self.output_dir = "data/raw/arbeitsagentur"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # S3 Konfiguration (wird lokal aus dem Terminal oder auf GitHub aus den Secrets gelesen)
        self.bucket_name = "jobmarket-analyzer-data-lake"
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name="eu-central-1"
        )
        
        # Offizieller v6-Such-Endpunkt der Bundesagentur
        self.api_url = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v6/jobs"
        self.api_key = "jobboerse-jobsuche"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

    def fetch_targeted_germany_jobs(self):
        # Die erweiterten Suchbegriff-Cluster für maximale Marktabdeckung
        search_terms = [
            "Data", 
            "BI", 
            "Business Intelligence", 
            "Analytics", 
            "Analytics Engineer",
            "AI Engineer", 
            "Artificial Intelligence",
            "Künstliche Intelligenz", 
            "Web Analytics", 
            "Webanalyst",
            "Digital Analytics",
            "Machine Learning",
            "Data Science"
        ]
        
        # Dictionary zur automatischen In-Memory-Deduplizierung via URL
        all_combined_jobs = {} 
        
        # FIX: Unverified SSL Context gegen das Blockieren der HTTPS-Verbindung
        context = ssl._create_unverified_context()
        
        for term in search_terms:
            current_page = 1
            term_jobs_count = 0
            max_results_per_term = 2500
            
            print(f"\n🌍 [SMART INGESTION] Starte Cluster-Abfrage für: '{term}'...")
            
            while term_jobs_count < max_results_per_term:
                params = {
                    "was": term,
                    "page": current_page,
                    "size": 50
                }
                full_url = f"{self.api_url}?{urllib.parse.urlencode(params)}"
                
                req = urllib.request.Request(full_url)
                req.add_header("X-API-Key", self.api_key)
                req.add_header("Accept", "application/json")
                req.add_header("User-Agent", self.user_agent)
                
                try:
                    with urllib.request.urlopen(req, context=context, timeout=15) as response:
                        if response.status == 200:
                            data = json.loads(response.read().decode("utf-8"))
                            listings = data.get("ergebnisliste", [])
                            
                            if not listings:
                                break
                            
                            current_page_valid = 0
                            for job in listings:
                                title = job.get("stellenangebotsTitel") or "Data Professional"
                                company = job.get("firma") or "Spannendes Unternehmen"
                                title_lower = title.lower()
                                
                                # --- 1. ERWEITERTE MARKT-VALIDIERUNG (Substrings) ---
                                valid_keywords = [
                                    "data", "analyst", "analyt", "engineer", "intelligence", 
                                    "scientist", "bi", "developer", "entwicklung", "cloud", 
                                    "analytics", "ml", "ai", "ki ", "künstliche intelligenz",
                                    "artificial", "business intelligence", "big data", "dwh",
                                    "data warehouse", "reporting", "dashboard", "statistics",
                                    "statist", "webanalyst", "digital analyt"
                                ]
                                
                                is_valid_data_job = any(keyword in title_lower for keyword in valid_keywords)
                                
                                # --- 2. VORAB-AUSSCHLUSS FÜR NON-TECH & AUSBILDUNG ---
                                noise_blacklist = [
                                    "support", "helpdesk", "systemadministrator", "netzwerk", 
                                    "hardware", "first-level", "anwendersupport",
                                    "ausbildung", "handelsfachwirt", "abiturientenprogramm", 
                                    "duales studium b.a.", "verkäufer", "kaufmann", "kauffrau",
                                    "marktleiter", "betriebswirt", "filialteamleitung"
                                ]
                                
                                if any(noise in title_lower for noise in noise_blacklist):
                                    is_valid_data_job = False
                                    
                                # Falls die Validierung fehlschlägt, vorab verwerfen
                                if not is_valid_data_job:
                                    continue

                                # --- 3. DATEN-EXTRAKTION & GEOLOCATION ---
                                lokationen = job.get("stellenlokationen", [])
                                location_str = "Deutschland"
                                if lokationen and "adresse" in lokationen[0]:
                                    addr = lokationen[0]["adresse"]
                                    location_str = f"{addr.get('plz', '')} {addr.get('ort', 'Deutschland')}".strip()

                                ref_nr = job.get("referenznummer")
                                job_link = f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{ref_nr}" if ref_nr else (job.get("externeURL") or "No Link")

                                all_combined_jobs[job_link] = {
                                    "title": title,
                                    "company": company,
                                    "link": job_link,
                                    "location": location_str,
                                    "remote_status": "Vollständig Remote / Home-Office möglich" if job.get("homeofficemoeglich", False) else "Keine Angabe / Präsenz",
                                    "chiffrenummer": job.get("chiffrenummer") or "Nicht angegeben",
                                    "aenderungsdatum": job.get("aenderungsdatum") or "Nicht angegeben",
                                    "externe_url": job.get("externeURL") or "Keine externe URL",
                                    "veroeffentlicht_am": job.get("veroeffentlichungszeitraum", {}).get("von") or "Nicht angegeben",
                                    "scraped_at": datetime.now().isoformat(),
                                    "source_term_match": term,
                                    "source_page": current_page
                                }
                                current_page_valid += 1
                                
                            term_jobs_count += len(listings)
                            print(f"   ↳ Seite {current_page} verarbeitet ({current_page_valid} valide Data-Jobs extrahiert)...", end="\r")
                            current_page += 1
                            time.sleep(0.6)
                            
                except Exception as e:
                    print(f"\n   ⚠ Netzwerk- oder Parsing-Fehler bei Seite {current_page}: {e}")
                    break
            
            print(f"\n   ✅ Cluster '{term}' verarbeitet. Eindeutige Jobs im Gesamtpool: {len(all_combined_jobs)}")

        # --- 4. DATA LAKE PERSISTIERUNG (Raw Layer Landing) ---
        if all_combined_jobs:
            final_list = list(all_combined_jobs.values())
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"arbeitsagentur_germany_{timestamp}.json"
            file_path = os.path.join(self.output_dir, filename)
            
            payload = {
                "metadata": {
                    "source": "rest.arbeitsagentur.de (Smart Multi-Cluster V3)",
                    "extracted_at": timestamp,
                    "total_jobs": len(final_list)
                },
                "jobs": final_list
            }
            
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=4)
                
            print(f"\n🎯 [SUCCESS] Ingestion-Prozess erfolgreich beendet!")
            print(f"💾 Es wurden {len(final_list)} hochpräzise Data- & AI-Jobs ohne Rauschen extrahiert.")
            print(f"📁 Zielpfad im Data Lake: {file_path}")

            # --- 5. AWS S3 COLD STORAGE (Bronze Layer Upload) ---
            try:
                s3_key = f"raw/arbeitsagentur/{filename}"
                print(f"🪣  Spiegele Bronze-Layer in den Cloud Data Lake: {s3_key}...")
                
                self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
                print("🚀 [SUCCESS] Rohdaten-Payload erfolgreich in AWS S3 gesichert!")
                
            except ClientError as e:
                print(f"❌ AWS S3 Berechtigungsfehler: Haben die Umgebungsvariablen S3-Schreibzugriff? {e}")
            except Exception as e:
                print(f"⚠ S3-Archivierung fehlgeschlagen (Lokale Datei existiert): {e}")

if __name__ == "__main__":
    print("▶ Starte Ingestion Pipeline v3...")
    pipeline = BundesAgenturSmartPipeline()
    pipeline.fetch_targeted_germany_jobs()