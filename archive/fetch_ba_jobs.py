import json
import os
import ssl
import urllib.request
import urllib.parse
from datetime import datetime
import time

class BundesAgenturApiPipeline:
    def __init__(self):
        self.output_dir = "data/raw/arbeitsagentur"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Offizieller v6 Endpunkt der Jobsuche
        self.api_url = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v6/jobs"
        self.api_key = "jobboerse-jobsuche"

    def fetch_jobs(self, search_term: str = "Data", max_results: int = 150):
        """Scans the official BA REST API v6 using the verified response schema mapping."""
        current_page = 1
        all_jobs = []
        
        print(f"🚀 Querying REST API Gateway for '{search_term}' roles in Hamburg...")

        context = ssl.create_default_context()

        while len(all_jobs) < max_results:
            print(f"🌐 Requesting API Page {current_page}...")
            
            params = {
                "was": search_term,
                "wo": "Hamburg",
                "page": current_page,
                "size": 50,
                "umkreis": 10
            }
            url_params = urllib.parse.urlencode(params)
            full_url = f"{self.api_url}?{url_params}"
            
            req = urllib.request.Request(full_url)
            req.add_header("X-API-Key", self.api_key)
            req.add_header("Accept", "application/json")
            req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            
            try:
                with urllib.request.urlopen(req, context=context, timeout=15) as response:
                    if response.status == 200:
                        raw_data = response.read().decode("utf-8")
                        data = json.loads(raw_data)
                        
                        listings = data.get("ergebnisliste", [])
                        
                        if not listings:
                            print("🛑 Reached organic end of API database records.")
                            break
                            
                        for job in listings:
                            # Passgenaues v6-Mapping basierend auf dem Diagnose-Payload
                            title = job.get("stellenangebotsTitel") or "Data Professional"
                            company = job.get("firma") or "Spannendes Unternehmen"
                            
                            # Remote-Status nativ auswerten
                            is_remote = job.get("homeofficemoeglich", False)
                            remote_status = "Vollständig Remote / Home-Office möglich" if is_remote else "Keine Angabe / Präsenz"
                            
                            # Link-Generierung via offizieller Referenznummer
                            ref_nr = job.get("referenznummer")
                            if ref_nr:
                                job_link = f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{ref_nr}"
                            else:
                                job_link = job.get("externeURL") or "No Link"

                            # --- NEUE FELDER ERGÄNZEN ---
                            # Chiffrenummer extrahieren
                            chiffrenummer = job.get("chiffrenummer") or "Nicht angegeben"
                            
                            # Änderungsdatum extrahieren
                            aenderungsdatum = job.get("aenderungsdatum") or "Nicht angegeben"
                            
                            # Externe URL extrahieren
                            externe_url = job.get("externeURL") or "Keine externe URL"
                            
                            # Veröffentlichungsdatum aus dem verschachtelten Objekt extrahieren
                            veroffentlicht_von = "Nicht angegeben"
                            v_zeitraum = job.get("veroeffentlichungszeitraum")
                            if isinstance(v_zeitraum, dict):
                                veroffentlicht_von = v_zeitraum.get("von") or "Nicht angegeben"

                            all_jobs.append({
                                "title": title,
                                "company": company,
                                "link": job_link,
                                "location": "Hamburg",
                                "remote_status": remote_status,
                                # Neue Felder im Datensatz:
                                "chiffrenummer": chiffrenummer,
                                "aenderungsdatum": aenderungsdatum,
                                "externe_url": externe_url,
                                "veroeffentlicht_am": veroffentlicht_von,
                                "scraped_at": datetime.now().isoformat(),
                                "source_page": current_page
                            })
                            
                        print(f"   ✅ Page {current_page} processed. Total pool: {len(all_jobs)} jobs.")
                        current_page += 1
                        time.sleep(1.0)
                        
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print("🛑 API responded with 404: Reached end of pagination stream.")
                else:
                    print(f"❌ API HTTP Error encountered: {e.code} - {e.reason}")
                break
            except Exception as e:
                print(f"❌ Unexpected Error on Page {current_page}: {e}")
                break

        # Datenpersistierung im Data Lake Staging
        if all_jobs:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(self.output_dir, f"arbeitsagentur_hamburg_{timestamp}.json")
            
            payload = {
                "metadata": {
                    "source": "rest.arbeitsagentur.de (v6)",
                    "extracted_at": timestamp,
                    "query": search_term,
                    "total_jobs": len(all_jobs)
                },
                "jobs": all_jobs
            }
            
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=4)
                
            print(f"\n🎯 [SUCCESS] Extracted {len(all_jobs)} structured records from the Agentur API!")
            print(f"💾 File landed safely under: {file_path}")


if __name__ == "__main__":
    pipeline = BundesAgenturApiPipeline()
    pipeline.fetch_jobs(search_term="Data", max_results=1000)