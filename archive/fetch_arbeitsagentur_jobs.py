import json
import os
from datetime import datetime
import time
import requests

class ArbeitsagenturIngestionPipeline:
    def __init__(self):
        self.output_dir = "data/raw/arbeitsagentur"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Public OAuth credentials provided by the Bundesagentur für Arbeit
        self.client_id = "c0077485-4331-434f-a00a-4d103b052b81"
        self.client_secret = "c4b22439-14eb-471a-9f4a-638e0ad53b56"
        
        self.token_url = "https://api.arbeitsagentur.de/oauth/token"
        self.api_url = "https://api.arbeitsagentur.de/jobboerse/jobsuche/v1/jobs"

    def _get_access_token(self) -> str:
        """Fetches a temporary OAuth2 access token from the official gateway."""
        print("🔑 Requesting OAuth2 Access Token from Arbeitsagentur Gateway...")
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        try:
            # FIX: verify=False removed. We trust the system certs now.
            response = requests.post(self.token_url, data=payload, headers=headers, timeout=10)
            
            # Diagnostic check: If the status is not 200, don't try to parse JSON
            if response.status_code != 200:
                print(f"❌ Server responded with Status {response.status_code}")
                print(f"📄 Response Text: {response.text[:500]}") # Shows the actual error page description
                raise RuntimeError("Non-200 response code encountered.")
                
            token_data = response.json()
            return token_data.get("access_token")
        except Exception as e:
            raise RuntimeError(f"❌ OAuth2 Authentication failed: {e}")

    def fetch_hamburg_jobs(self, search_term: str = "Data", max_results: int = 200):
        """Fetches job listings directly via the REST API using pagination."""
        try:
            access_token = self._get_access_token()
        except Exception as e:
            print(e)
            return

        if not access_token:
            print("❌ No access token generated. Aborting fetch.")
            return

        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-API-Key": self.client_id,
            "Accept": "application/json"
        }

        params = {
            "was": search_term,
            "wo": "Hamburg",
            "page": 1,
            "size": 50
        }

        all_jobs = []
        total_fetched = 0

        print(f"🚀 Querying REST API for '{search_term}' jobs in Hamburg...")

        while total_fetched < max_results:
            print(f"🌐 Fetching API Page {params['page']}...")
            try:
                response = requests.get(self.api_url, headers=headers, params=params, timeout=15)
                
                if response.status_code == 404:
                    print("🛑 Reached the end of available API records.")
                    break
                    
                if response.status_code != 200:
                    print(f"❌ API Error {response.status_code}: {response.text[:300]}")
                    break
                    
                data = response.json()
                job_listings = data.get("stellenangebote", [])
                if not job_listings:
                    print("🛑 No more records returned by the endpoint.")
                    break

                for job in job_listings:
                    all_jobs.append({
                        "title": job.get("beruf"),
                        "company": job.get("arbeitgeber", "Anonymes Unternehmen"),
                        "link": job.get("links", {}).get("details", {}).get("href", "No Link"),
                        "location": job.get("arbeitsort", {}).get("ort", "Hamburg"),
                        "remote_status": "No Info",
                        "scraped_at": datetime.now().isoformat(),
                        "source_page": params["page"]
                    })

                total_fetched = len(all_jobs)
                print(f"✅ Page {params['page']} processed. Total jobs in pool: {total_fetched}")
                params["page"] += 1
                time.sleep(1)

            except Exception as e:
                print(f"❌ API Request failed on page {params['page']}: {e}")
                break

        if all_jobs:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(self.output_dir, f"arbeitsagentur_hamburg_{timestamp}.json")
            
            output_payload = {
                "metadata": {
                    "source": "api.arbeitsagentur.de",
                    "extracted_at": timestamp,
                    "search_term": search_term,
                    "total_jobs_extracted": len(all_jobs)
                },
                "jobs": all_jobs
            }

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(output_payload, f, ensure_ascii=False, indent=4)

            print(f"\n🎯 [API INGESTION SUCCESS] Extracted {len(all_jobs)} jobs from official records!")
            print(f"💾 Raw JSON landed safely under: {file_path}")

if __name__ == "__main__":
    pipeline = ArbeitsagenturIngestionPipeline()
    pipeline.fetch_hamburg_jobs(search_term="Data", max_results=200)