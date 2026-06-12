import json
import os
import requests
import time
import ssl
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import urllib3

# --- SSL-WARNUNGEN STUMMSCHALTEN (Für saubere GitHub-Actions-Logs) ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- GLOBALER SSL-FIX ---
try:
    ssl._create_default_https_context = ssl._create_unverified_context
    print("🔓 Unverified SSL-Kontext erfolgreich initialisiert.")
except AttributeError:
    pass

class GermanyJobIngestionV3:
    def __init__(self):
        self.raw_dir = "data/raw/arbeitsagentur"
        os.makedirs(self.raw_dir, exist_ok=True)
        
        # S3 Konfiguration (wird von GitHub Actions oder lokalem .env gefüttert)
        self.bucket_name = "jobmarket-analyzer-data-lake"
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name="eu-central-1"
        )

        # Die offizielle Schnittstelle der Bundesagentur für Arbeit (API v6)
        self.api_url = "https://api.arbeitsagentur.de/jobboerse/jobsuche/v1/jobs"
        
        # Dein geschützter API-Schlüssel (wird als Secret übergeben)
        self.api_key = os.getenv("BA_CLIENT_ID", "X-API-KEY-FALLBACK")
        self.headers = {
            "X-API-Key": self.api_key,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        # Multi-Cluster Suchbegriffe für den bundesweiten Data- & AI-Markt
        self.search_terms = [
            "Data Engineer", 
            "AI Engineer", 
            "Business Intelligence", 
            "Webanalyst", 
            "Data Scientist"
        ]

        # Deine perfektionierte Blacklist gegen kaufmännischen & Support-Müll
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
                "scope": "Germany-Wide Multi-Cluster Ingestion",
                "version": "V3-Bronze"
            },
            "jobs": []
        }

        seen_chiffren = set()
        print(f"🚀 Starte bundesweite API-Ingestion für {len(self.search_terms)} Suchcluster...")

        for term in self.search_terms:
            current_page = 1
            term_jobs_count = 0
            print(f"🔍 Cluster: '{term}' wird abgefragt...")

            while True:
                params = {
                    "was": term,
                    "page": current_page,
                    "size": 50,  # Maximale Seitengröße der BA-API
                    "zeitarbeit": "false" # Filtert aggressive Personalvermittler direkt serverseitig
                }

                try:
                    # FIX: verify=False zwingt requests, die fehlerhafte SSL-Kette der BA zu ignorieren
                    response = requests.get(
                        self.api_url, 
                        headers=self.headers, 
                        params=params, 
                        timeout=15, 
                        verify=False
                    )
                    
                    if response.status_code == 401:
                        print("❌ API-Authentifizierung fehlgeschlagen! Bitte BA_CLIENT_ID prüfen.")
                        break
                    elif response.status_code != 200:
                        print(f"ℹ️ Keine weiteren Seiten für '{term}' (Status {response.status_code}).")
                        break

                    data = response.json()
                    job_results = data.get("stellenangebote", [])

                    if not job_results:
                        break

                    for raw_job in job_results:
                        title = raw_job.get("titel", "")
                        title_lower = title.lower()
                        chiffre = raw_job.get("refnr", "Nicht angegeben")

                        # Deduplizierung über Suchcluster-Grenzen hinweg
                        if chiffre in seen_chiffren:
                            continue

                        # Harter Vorab-Ausschluss für Non-Tech & Ausbildung (Die Handelsfachwirt-Bremse)
                        if any(noise in title_lower for noise in self.noise_blacklist):
                            continue

                        # Standardisiertes Feld-Mapping der verschachtelten API-Strukturen
                        location_info = raw_job.get("arbeitsort", {})
                        location_str = f"{location_info.get('plz', '')} {location_info.get('ort', '')}".strip() or "Deutschland"
                        
                        time_info = raw_job.get("veroeffentlichtAm", "")
                        mod_info = raw_job.get("modifikationsTimestamp", "")

                        mapped_job = {
                            "title": title,
                            "company": raw_job.get("arbeitgeber", "Anonymes Unternehmen"),
                            "link": raw_job.get("links", {}).get("details", {}).get("href"),
                            "externe_url": raw_job.get("links", {}).get("externeAnzeige", {}).get("href") or "Keine externe URL",
                            "location": location_str,
                            "remote_status": "Keine Angabe / Präsenz",
                            "chiffrenummer": chiffre,
                            "veroeffentlicht_am": time_info,
                            "aenderungsdatum": mod_info
                        }

                        master_payload["jobs"].append(mapped_job)
                        seen_chiffren.add(chiffre)
                        term_jobs_count += 1

                    # Seitensteuerung steht sauber AUSSERHALB der Job-Schleife
                    current_page += 1
                    time.sleep(0.3)  # Rate-Limiting-Respekt für die Bundesagentur

                except Exception as e:
                    print(f"⚠️ Netzwerkfehler im Cluster '{term}' auf Seite {current_page}: {e}")
                    break

            print(f"   -> Cluster '{term}' beendet. {term_jobs_count} Netto-Jobs extrahiert.")

        # Lokales Backup im Raw-Ordner sichern
        with open(local_raw_path, "w", encoding="utf-8") as f:
            json.dump(master_payload, f, ensure_ascii=False, indent=4)
        print(f"\n💾 Rohdaten lokal archiviert: {local_raw_path} ({len(master_payload['jobs'])} Gesamt-Jobs)")

        # --- S3 ARCHIVIERUNG (Bronze Layer Upload) ---
        try:
            s3_key = f"raw/arbeitsagentur/{local_filename}"
            print(f"🪣  Spiegele Bronze-Layer in den Cloud Data Lake: {s3_key}...")
            
            self.s3_client.upload_file(local_raw_path, self.bucket_name, s3_key)
            print("🚀 [SUCCESS] Rohdaten-Payload erfolgreich in AWS S3 gesichert!")
            
        except ClientError as e:
            print(f"❌ AWS S3 Berechtigungsfehler: Haben die GitHub Secrets vollen S3-Schreibzugriff? {e}")
        except Exception as e:
            print(f"⚠ S3-Archivierung fehlgeschlagen (Lokaler Prozess war trotzdem erfolgreich): {e}")

if __name__ == "__main__":
    ingestion = GermanyJobIngestionV3()
    ingestion.fetch_all_clusters()