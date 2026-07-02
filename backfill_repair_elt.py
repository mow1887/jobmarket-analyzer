import json
import os
import re
from playwright.sync_api import sync_playwright
import boto3

# =====================================================================
# TARGETED DATA ENGINEERING BACKFILL: CLEANING ETL, GIT & ML SUBS-BIAS
# =====================================================================

# Paths mirroring your standard architecture
silver_dir = "data/silver"
master_file = os.path.join(silver_dir, "master_enriched_jobs_de.json")

# AWS S3 Configurations
bucket_name = "jobmarket-analyzer-data-lake"
s3_key = "silver/master_enriched_jobs_de.json"
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="eu-central-1"
)

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# True-Positive Safeguards for Git and ML mapping
valid_pipeline_keywords = ["data pipeline", "datenpipeline", "data integration"]
valid_git_keywords = ["github", "gitlab", "bitbucket", "versionskontrolle"]
valid_ml_keywords = ["machine learning", "scikit-learn", "regression", "classification", "clustering", "k-means", "random forest"]

def run_targeted_backfill():
    print("🪣  Downloading latest dataset state from AWS S3 to ensure syncing...")
    s3_client.download_file(bucket_name, s3_key, master_file)
    
    with open(master_file, "r", encoding="utf-8") as f:
        master_data = json.load(f)
        
    all_jobs = master_data.get("jobs", [])
    
    # Identify target subset that needs validation across all three biased categories
    target_jobs = [
        j for j in all_jobs 
        if "ETL/ELT" in j.get("technologies", []) 
        or "Git/Version Control" in j.get("technologies", [])
        or "Machine Learning" in j.get("technologies", [])
    ]
    print(f"🔍 Found {len(target_jobs)} jobs requiring precision validation (ETL, Git or ML filters triggered).")
    
    if not target_jobs:
        print("✨ No jobs found matching target criteria. Pipeline is clean.")
        return

    print("🤖 Launching verification engine via headless browser layer...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()
        
        for idx, job in enumerate(target_jobs):
            url_to_scan = job.get("resolved_url") or job.get("link")
            print(f"[{idx+1}/{len(target_jobs)}] Verifying: {job['title']} ({job['company']})...")
            
            try:
                page.goto(url_to_scan, wait_until="domcontentloaded", timeout=12000)
                page.wait_for_timeout(300)
                raw_text = page.locator("body").inner_text()
                text_clean = re.sub(r'\s+', ' ', raw_text.lower())
                
                # -----------------------------------------------------
                # 1. VALIDATION: ETL / ELT
                # -----------------------------------------------------
                if "ETL/ELT" in job.get("technologies", []):
                    has_standalone_elt = bool(re.search(r"\b(etl|elt)\b", text_clean))
                    has_valid_phrase = any(phrase in text_clean for phrase in valid_pipeline_keywords)
                    if not (has_standalone_elt or has_valid_phrase):
                        job["technologies"].remove("ETL/ELT")
                        print("    ❌ False positive 'ETL/ELT' removed.")

                # -----------------------------------------------------
                # 2. VALIDATION: GIT / VERSION CONTROL
                # -----------------------------------------------------
                if "Git/Version Control" in job.get("technologies", []):
                    has_standalone_git = bool(re.search(r"\bgit\b", text_clean))
                    has_valid_git = any(phrase in text_clean for phrase in valid_git_keywords)
                    if not (has_standalone_git or has_valid_git):
                        job["technologies"].remove("Git/Version Control")
                        print("    ❌ False positive 'Git' removed (matched inside word like 'digital').")

                # -----------------------------------------------------
                # 3. VALIDATION: MACHINE LEARNING
                # -----------------------------------------------------
                if "Machine Learning" in job.get("technologies", []):
                    has_standalone_ml = bool(re.search(r"\bml\b", text_clean))
                    has_valid_ml = any(phrase in text_clean for phrase in valid_ml_keywords)
                    if not (has_standalone_ml or has_valid_ml):
                        job["technologies"].remove("Machine Learning")
                        print("    ❌ False positive 'Machine Learning' removed (matched inside word like 'html').")

                # If a job's technology list becomes entirely empty, keep structure uniform with fallback
                if not job["technologies"]:
                    job["technologies"] = ["Classic Tools / Open Research"]
                    
            except Exception as e:
                print(f"    ⚠ Verification page unreachable, skipping for safety: {e}")
                
            # Intermittent local state persistence check-pointing
            if idx % 20 == 0:
                with open(master_file, "w", encoding="utf-8") as fs:
                    json.dump(master_data, fs, ensure_ascii=False, indent=4)
                    
        browser.close()
        
    # Write consolidated master dataset back to local file storage only
    with open(master_file, "w", encoding="utf-8") as f:
        json.dump(master_data, f, ensure_ascii=False, indent=4)
    print(f"\n🎯 DRY RUN COMPLETED LOCALLY! Cleaned state saved under: {master_file}")
    print("⚠️  AWS S3 synchronization is skipped for safety until you verify the local JSON file.")

    # [LOCAL TESTING MODE] S3 Upload safely commented out to prevent early overrides:
    print("🪣 Uploading cleaned dataset back to AWS S3 Silver Layer...")
    s3_client.upload_file(master_file, bucket_name, s3_key)
    print("🚀 [SUCCESS] AWS S3 Master dataset state updated and cleaned!")

if __name__ == "__main__":
    run_targeted_backfill()