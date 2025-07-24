import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from src.logger import logger

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def run_etl_process():
    """Executa o ETL para dim_owners como subprocesso"""
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "src.etl", "dim_owners"],
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        while process.poll() is None:
            output = process.stdout.readline()
            error = process.stderr.readline()
            
            if output:
                logger.info(f"[DIM_OWNERS] {output.strip()}")
            if error:
                logger.error(f"[DIM_OWNERS] {error.strip()}")

        return process.returncode == 0
        
    except Exception as e:
        logger.error(f"Erro no drone dim_owners: {str(e)}")
        return False

def main():
    logger.info("🛸 DIM_OWNERS Drone initialized - Ctrl+C to stop")
    while True:
        success = run_etl_process()
        wait_time = 3600 if success else 600  # 1h se sucesso, 10min se falha
        next_run = datetime.now() + timedelta(seconds=wait_time)
        logger.info(f"⏳ Next run in {wait_time}s at {next_run.strftime('%H:%M:%S')}")
        time.sleep(wait_time)

if __name__ == "__main__":
    main()