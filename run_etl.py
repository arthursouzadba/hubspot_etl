import subprocess
import time
from src.logger import logger
from src.database import Database
from src.etl import build_fato_deal_query

class ETLPipeline:
    def __init__(self):
        self.db = Database()
        self.dimension_processes = [
            {"name": "dim_etapa", "cmd": ["python", "-m", "src.etl", "dim_etapa"]},
            {"name": "dim_owners", "cmd": ["python", "-m", "src.etl", "dim_owners"]}
        ]

    def run_dimension_process(self, process):
        """Executa um processo de dimensão como subprocesso"""
        logger.info(f"🛠 Processing {process['name']}")
        result = subprocess.run(
            process["cmd"],
            capture_output=True,
            text=True
        )
        
        # Log de saída
        if result.stdout:
            logger.info(f"[{process['name'].upper()}] {result.stdout.strip()}")
        if result.stderr:
            logger.error(f"[{process['name'].upper()}] {result.stderr.strip()}")
            
        return result.returncode == 0

    def process_fact_table(self):
        """Processa a tabela fato com tratamento robusto de erros"""
        logger.info("🛠 Processing fato_deal (with fallback)")
        try:
            # Processamento principal
            self.db.process_fact_with_fallback()

            # Diagnóstico de referências inválidas
            with self.db.get_connection() as conn:
                self.db.log_invalid_references(conn)
                # Remova a linha abaixo
                # self.db.validate_data_consistency(conn)

            return True

        except Exception as e:
            logger.error(f"❌ Critical error in fact table: {str(e)}")
            return False

    def run(self):
        """Executa o pipeline ETL completo"""
        logger.info("🚀 Starting ETL pipeline")
        start_time = time.time()
        
        # Processa dimensões
        for process in self.dimension_processes:
            if not self.run_dimension_process(process):
                logger.error(f"❌ Pipeline failed at {process['name']}")
                return False

        # Processa tabela fato
        if not self.process_fact_table():
            return False

        # Log final
        duration = time.time() - start_time
        logger.info(f"✅ ETL completed successfully in {duration:.2f} seconds")
        return True

def main():
    pipeline = ETLPipeline()
    
    while True:
        success = pipeline.run()
        wait_time = 3600 if success else 300  # 1h if success, 5min if failed
        logger.info(f"⏳ Next run in {wait_time} seconds...")
        time.sleep(wait_time)

if __name__ == "__main__":
    main()