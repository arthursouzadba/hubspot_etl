# src/etl.py
from datetime import datetime
from src.database import Database
from src.logger import logger
from src.config import Config

def build_dim_etapa_query(config):
    return f"SELECT etapa_id, pipeline, etapa FROM {config.dim_etapa_source}"

def build_dim_owners_query(config):
    return f"SELECT owner_id, owner_name FROM {config.dim_owners_source}"

def build_fato_deal_query(config):
    """Garante o formato DATE para campos de data"""
    return f"""
    SELECT 
        deal_id, 
        -- Garante o formato DATE para PostgreSQL
        CASE
            WHEN data_negocio_criado ~ '^\d{{4}}-\d{{2}}-\d{{2}}$' 
                THEN data_negocio_criado
            ELSE NULL
        END AS data_negocio_criado,
        
        CASE
            WHEN data_agendamento ~ '^\d{{4}}-\d{{2}}-\d{{2}}$' 
                THEN data_agendamento
            ELSE NULL
        END AS data_agendamento,
        
        nome_negocio,
        etapa_id,
        valor::TEXT,
        funil,
        origem,
        canal,
        detalhes,
        owner_id
    FROM {config.fato_deal_source}
    """

def process_dimension(db, table_type):
    """Process a dimension table"""
    logger.info(f"Processing {table_type}")
    with db.get_connection() as conn:
        if table_type == "dim_etapa":
            db.create_dim_etapa_table(conn)
            query = build_dim_etapa_query(db.config)
            target = db.config.dim_etapa_target
        else:
            db.create_dim_owners_table(conn)
            query = build_dim_owners_query(db.config)
            target = db.config.dim_owners_target
        
        db.truncate_and_insert(conn, target, query)

def process_fact(db):
    """Process fact table with dependencies"""
    logger.info("Processing fact table")
    with db.get_connection() as conn:
        # Verify dimensions exist first
        if not db.check_table_exists(conn, db.config.dim_etapa_target) or \
           not db.check_table_exists(conn, db.config.dim_owners_target):
            raise Exception("Dimension tables not found. Load them first.")
        
        # Create initial table
        db.create_fato_deal_table(conn)
        db.truncate_and_insert(conn, db.config.fato_deal_target, build_fato_deal_query(db.config))
        
        # Convert data types
        try:
            db.safe_convert_data_types(conn)
            try:
                db.add_foreign_keys(conn)
            except Exception as fk_error:
                logger.warning(f"FK constraints not added: {fk_error}")
        except Exception as conv_error:
            logger.error(f"Type conversion failed: {conv_error}")

def main(table_type):
    """Main ETL entry point"""
    logger.info(f"Starting ETL for {table_type}")
    start = datetime.now()
    
    try:
        db = Database()
        
        # Ensure schema exists
        with db.get_connection() as conn:
            if not db.check_schema_exists(conn, db.config.TARGET_SCHEMA):
                db.create_schema(conn, db.config.TARGET_SCHEMA)
        
        # Process the requested table
        if table_type in ["dim_etapa", "dim_owners"]:
            process_dimension(db, table_type)
        elif table_type == "fato_deal":
            process_fact(db)
        
        logger.info(f"ETL completed in {datetime.now() - start}")
    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        print("Usage: python -m src.etl [dim_etapa|dim_owners|fato_deal]")