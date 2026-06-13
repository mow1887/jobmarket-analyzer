import json
import os
import requests
import time
import ssl
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import urllib3

# --- SSL-WARNUNGEN STUMMSCHALTEN ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- SSL & HOSTNAME PATCH (Sicherheitsnetz für Cloud-Runner) ---
try:
    orig_create_context = urllib3.util.ssl_.create_urllib3_context
    def patched_create_context(*args, **kwargs):
        context = orig_create_context(*args, **kwargs)
        context.check_hostname = False
        return context
    urllib3.util.ssl_.create_urllib3_context = patched_create_context
    print("🔓 Patched urllib3 SSLContext: Hostname-Verifizierung global deaktiviert.")
except Exception as e:
    print(f"⚠️ Konnte Core-SSL-Patch nicht anwenden: {e}")

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass

class GermanyJobIngestionV3:
    def __init__(self):
        self.raw_dir = "data/raw/arbeitsagentur"
        os.makedirs(self.raw_dir, exist_ok=True)
        
        # S3 Target-Bucket (Injektiert über GitHub Secrets)
        self.bucket_name = "jobmarket-analyzer-data-lake"
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name="eu-central-1"
        )

        # --- STABILER ENDPOINT AUS V2 (Öffentliches Web-Gateway) ---
        self.api_url = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v6/jobs"
        self.api_key = "jobboerse-jobsuche"  # Statisch für diesen Endpoint, kein Secret mehr nötig!
        
        self.headers = {
            "X-API-Key": self.api_key,
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }

        # Multi-Cluster Suchbegriffe für maximale Marktabdeckung
        self.search_terms = [
            "Data Engineer", 
            "AI Engineer", 
            "Business Intelligence", 
            "Webanalyst", 
            "Data Scientist"
        ]

        # Deine Noise-Blacklist gegen kaufmännischen Ballast
        self.noise_blacklist = [
            "support", "helpdesk", "systemadministrator", "netzwerk", 
            "hardware", "first-level", "anwendersupport",
            "ausbildung", "handelsfachwirt", "abiturientenprogramm", 
            "duales studium b.a.", "verkäufer", "kaufmann", "kauffrau",
            "marktleiter", "betriebswirt"
        ]

    def fetch_all_clusters(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_filename = f"arbeitsagentur_germany_{timestamp}.json"
        local_raw_path = os.path.join(self.raw_dir, local_filename)
        
        master_payload = {
            "metadata": {
                "extracted_at": datetime.now().isoformat(),
                "scope": "Germany-Wide Multi-Cluster Ingestion (Web-Endpoint)",
                "version": "V3-Bronze-Stable"
            },
            "jobs": []
        }

        seen_chiffren = set()
        print(f"🚀 Starte bundesweite API-Ingestion via rest.arbeitsagentur.de...")

        for term in self.search_terms:
            current_page = 1
            term_jobs_count = 0
            print(f"🔍 Cluster: '{term}' wird abgefragt...")

            while True:
                params = {
                    "was": term,
                    "page": current_page,
                    "size": 50,
                    "zeitarbeit": "false"
                }

                try:
                    response = requests.get(
                        self.api_url, 
                        headers=self.headers, 
                        params=params, 
                        timeout=15, 
                        verify=False
                    )
                    
                    if response.status_code != 200:
                        print(f"ℹ️ Keine weiteren Seiten für '{term}' (Status {response.status_code}).")
                        break

                    try:
                        data = response.json()
                    except Exception:
                        print(f"   ❌ Server lieferte kein gültiges JSON!")
                        print(f"   📄 TEXT-VORSCHAU:\n{response.text[:300]}\n---")
                        break

                    job_results = data.get("stellenangebote", [])
                    if not job_results:
                        break

                    for raw_job in job_results:
                        title = raw_job.get("titel", "")
                        title_lower = title.lower()
                        chiffre = raw_job.get("refnr", "Nicht angegeben")

                        if chiffre in seen_chiffren:
                            continue

                        if any(noise in title_lower for noise in self.noise_blacklist):
                            continue

                        location_info = raw_job.get("arbeitsort", {})
                        location_str = f"{location_info.get('plz', '')} {location_info.get('ort', '')}".strip() or "Deutschland"
                        
                        mapped_job = {
                            "title": title,
                            "company": raw_job.get("arbeitgeber", "Anonymes Unternehmen"),
                            "link": raw_job.get("links", {}).get("details", {}).get("href"),
                            "externe_url": raw_job.get("links", {}).get("externeAnzeige", {}).get("href") or "Keine externe URL",
                            "location": location_str,
                            "remote_status": "Keine Angabe / Präsenz",
                            "chiffrenummer": chiffre,
                            "veroeffentlicht_am": raw_job.get("veroeffentlichtAm", ""),
                            "aenderungsdatum": raw_job.get("modifikationsTimestamp", "")
                        }

                        master_payload["jobs"].append(mapped_job)
                        seen_chiffren.add(chiffre)
                        term_jobs_count += 1

                    print(f"   -> Seite {current_page} verarbeitet ({term_jobs_count} Cluster-Jobs im Staging)...")
                    current_page += 1
                    time.sleep(0.4)

                except Exception as e:
                    print(f"⚠️ Netzwerkfehler im Cluster '{term}' auf Seite {current_page}: {e}")
                    break

            print(f"   ✨ Cluster '{term}' erfolgreich beendet. Netto-Jobs: {term_jobs_count}")

        # Lokales Speichern im Runner-Dateisystem
        with open(local_raw_path, "w", encoding="utf-8") as f:
            json.dump(master_payload, f, ensure_ascii=False, indent=4)
        print(f"\n💾 Rohdaten lokal archiviert: {local_raw_path} ({len(master_payload['jobs'])} Gesamt-Jobs)")

        # --- S3 COLD STORAGE (Bronze Layer Upload) ---
        try:
            s3_key = f"raw/arbeitsagentur/{local_filename}"
            print(f"🪣  Spiegele Bronze-Layer in den Cloud Data Lake: {s3_key}...")
            self.s3_client.upload_file(local_raw_path, self.bucket_name, s3_key)
            print("🚀 [SUCCESS] Rohdaten-Payload erfolgreich in AWS S3 gesichert!")
        except Exception as e:
            print(f"⚠ S3-Archivierung fehlgeschlagen: {e}")

if __name__ == "__main__":
    ingestion = GermanyJobIngestionV3()
    ingestion.fetch_all_clusters()