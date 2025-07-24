# run_etl.py
import subprocess
import time
from src.logger import logger

def run_etl_process():
    """Run complete ETL pipeline"""
    logger.info("🚀 Starting ETL pipeline")
    
    processes = [
        {"name": "dim_etapa", "cmd": ["python", "-m", "src.etl", "dim_etapa"]},
        {"name": "dim_owners", "cmd": ["python", "-m", "src.etl", "dim_owners"]},
        {"name": "fato_deal", "cmd": ["python", "-m", "src.etl", "fato_deal"]}
    ]
    
    for p in processes:
        logger.info(f"🛠 Processing {p['name']}")
        result = subprocess.run(p["cmd"], capture_output=True, text=True)
        
        if result.stdout:
            logger.info(result.stdout)
        if result.stderr:
            logger.error(result.stderr)
            
        if result.returncode != 0:
            logger.error(f"❌ Failed to process {p['name']}")
            return False
    
    logger.info("✅ ETL completed successfully")
    return True

if __name__ == "__main__":
    while True:
        success = run_etl_process()
        wait = 3600 if success else 300  # 1h if success, 5min if failed
        logger.info(f"⏳ Next run in {wait} seconds...")
        time.sleep(wait)