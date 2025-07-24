import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

# Adiciona o diretório pai ao path
sys.path.append(str(Path(__file__).parent.parent))

from src.logger import logger

def run_etl_process():
    """Executa o ETL para dim_etapa como subprocesso"""
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "src.etl", "dim_etapa"],
            cwd=Path(__file__).parent.parent,
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
                logger.info(f"[DIM_ETAPA] {output.strip()}")
            if error:
                logger.error(f"[DIM_ETAPA] {error.strip()}")

        return process.returncode == 0
        
    except Exception as e:
        logger.error(f"Erro no drone dim_etapa: {str(e)}")
        return False

def main():
    logger.info("🛸 DIM_ETAPA Drone initialized - Ctrl+C to stop")
    while True:
        success = run_etl_process()
        wait_time = 3600 if success else 600
        next_run = datetime.now() + timedelta(seconds=wait_time)
        logger.info(f"⏳ Next run in {wait_time}s at {next_run.strftime('%H:%M:%S')}")
        time.sleep(wait_time)

if __name__ == "__main__":
    main()