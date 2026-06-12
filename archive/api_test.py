import json
import ssl
import urllib.request
import urllib.parse

class BundesAgenturDiagnose:
    def __init__(self):
        self.api_url = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v6/jobs"
        self.api_key = "jobboerse-jobsuche"

    def print_raw_job_structure(self):
        print("🔍 Starte einmaligen Diagnose-Abruf der v6-Struktur...")
        context = ssl.create_default_context()
        
        params = {"was": "Data", "wo": "Hamburg", "page": 1, "size": 5}
        full_url = f"{self.api_url}?{urllib.parse.urlencode(params)}"
        
        req = urllib.request.Request(full_url)
        req.add_header("X-API-Key", self.api_key)
        req.add_header("Accept", "application/json")
        req.add_header("User-Agent", "Mozilla/5.0")
        
        try:
            with urllib.request.urlopen(req, context=context, timeout=10) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode("utf-8"))
                    listings = data.get("ergebnisliste", [])
                    
                    if listings:
                        print("\n💎 ETWAS GEFUNDEN! Hier ist die ROHE Struktur des allerersten Jobs:\n")
                        print(json.dumps(listings[0], indent=4, ensure_ascii=False))
                        print("\n--------------------------------------------------")
                    else:
                        print("🛑 'ergebnisliste' war leer. Hier ist das komplette Haupt-JSON:")
                        print(json.dumps(data, indent=4, ensure_ascii=False))
        except Exception as e:
            print(f"❌ Fehler beim Diagnose-Abruf: {e}")

if __name__ == "__main__":
    diagnose = BundesAgenturDiagnose()
    diagnose.print_raw_job_structure()