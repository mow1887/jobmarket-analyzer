import json
import os
import ssl
import urllib.request
import urllib.parse
from datetime import datetime
import time

class BundesAgenturSmartPipeline:
    def __init__(self):
        self.output_dir = "data/raw/arbeitsagentur"
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.api_url = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v6/jobs"
        self.api_key = "jobboerse-jobsuche"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    def fetch_targeted_germany_jobs(self):
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
        
        all_combined_jobs = {} 
        
        # --- FIX: Unverified SSL Context gegen das Einfrieren der Verbindung ---
        context = ssl._create_unverified_context()
        
        for term in search_terms:
            current_page = 1
            term_jobs_count = 0
            max_results_per_term = 1500 
            
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
                    # Wir nutzen den gelockerten SSL-Kontext und ein klares Timeout von 10 Sekunden
                    with urllib.request.urlopen(req, context=context, timeout=10) as response:
                        if response.status == 200:
                            data = json.loads(response.read().decode("utf-8"))
                            listings = data.get("ergebnisliste", [])
                            
                            if not listings:
                                break
                                
                            current_term_valid_count = 0
                            for job in listings:
                                title = job.get("stellenangebotsTitel") or "Data Professional"
                                company = job.get("firma") or "Spannendes Unternehmen"
                                title_lower = title.lower()
                                
                                # 1. ERWEITERTE MARKT-VALIDIERUNG
                                valid_keywords = [
                                    "data", "analyst", "analyt", "engineer", "intelligence", 
                                    "scientist", "bi", "developer", "entwicklung", "cloud", 
                                    "analytics", "ml", "ai", "ki ", "künstliche intelligenz",
                                    "artificial", "business intelligence", "big data", "dwh",
                                    "data warehouse", "reporting", "dashboard", "statistics",
                                    "statist", "webanalyst", "digital analyt"
                                ]
                                
                                is_valid_data_job = any(keyword in title_lower for keyword in valid_keywords)
                                
                                # 2. HÄRTER VORAB-AUSSCHLUSS FÜR IT-RAUSCHEN
                                it_noise = ["support", "helpdesk", "systemadministrator", "netzwerk", "hardware", "first-level", "anwendersupport"]
                                if any(noise in title_lower for noise in it_noise):
                                    is_valid_data_job = False
                                    
                                if not is_valid_data_job:
                                    continue

                                # 3. DATEN-EXTRAKTION
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
                                current_term_valid_count += 1
                                
                            term_jobs_count += len(listings)
                            current_page += 1
                            
                            # Kurzer visueller Ticker im Terminal, damit du siehst, dass gearbeitet wird
                            print(f"   ↳ Seite {current_page-1} verarbeitet ({current_term_valid_count} valide Data-Jobs extrahiert)...", end="\r")
                            time.sleep(0.5)
                            
                except Exception as e:
                    print(f"\n   ⚠ Netzwerk- oder Parsing-Fehler bei Seite {current_page}: {e}")
                    break
            
            print(f"\n   ✅ Cluster '{term}' beendet. Eindeutige Jobs im Gesamtpool: {len(all_combined_jobs)}")

        # 4. SPEICHERN
        if all_combined_jobs:
            final_list = list(all_combined_jobs.values())
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(self.output_dir, f"arbeitsagentur_germany_{timestamp}.json")
            
            payload = {
                "metadata": {
                    "source": "rest.arbeitsagentur.de (Smart Multi-Cluster)",
                    "extracted_at": timestamp,
                    "total_jobs": len(final_list)
                },
                "jobs": final_list
            }
            
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=4)
                
            print(f"\n🎯 [SUCCESS] Ingestion-Prozess erfolgreich beendet!")
            print(f"💾 {len(final_list)} hochpräzise Data- & AI-Jobs exportiert.")
            print(f"📁 Pfad: {file_path}")

if __name__ == "__main__":
    pipeline = BundesAgenturSmartPipeline()
    pipeline.fetch_targeted_germany_jobs()