import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from .config import Config
from .logger import logger

class Database:
    def __init__(self):
        self.config = Config()
    
    @contextmanager
    def get_connection(self):
        """Gerenciador de contexto para conexões seguras"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.DB_HOST,
                port=self.config.DB_PORT,
                database=self.config.DB_NAME,
                user=self.config.DB_USER,
                password=self.config.DB_PASSWORD,
                connect_timeout=10,
                options='-c statement_timeout=30000'
            )
            conn.autocommit = False
            logger.info("Database connection established")
            yield conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
        finally:
            if conn and not conn.closed:
                conn.close()
                logger.info("Database connection closed")

    def check_schema_exists(self, conn, schema_name):
        """Check if target schema exists"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);",
                    (schema_name,)
                )
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error checking schema: {e}")
            raise

    def check_table_exists(self, conn, table_name):
        """Check if table exists in the database"""
        try:
            schema, table = table_name.split('.') if '.' in table_name else ('public', table_name)
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND table_name = %s
                    )""", (schema, table))
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False

    def create_schema(self, conn, schema_name):
        """Create target schema if not exists"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
                conn.commit()
                logger.info(f"Schema {schema_name} created/verified")
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            raise

    def create_dim_etapa_table(self, conn):
        """Create dim_etapa table if not exists"""
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.config.dim_etapa_target} (
                etapa_id TEXT PRIMARY KEY,
                pipeline TEXT,
                etapa TEXT
            );"""
            
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()
            logger.info(f"Table {self.config.dim_etapa_target} created/verified")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def create_dim_owners_table(self, conn):
        """Create dim_owners table if not exists"""
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.config.dim_owners_target} (
                owner_id TEXT PRIMARY KEY,
                owner_name TEXT
            );"""
            
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()
            logger.info(f"Table {self.config.dim_owners_target} created/verified")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def create_fato_deal_table(self, conn):
        """Create fato_deal table with TEXT types initially"""
        try:
            create_table_query = f"""
            DROP TABLE IF EXISTS {self.config.fato_deal_target} CASCADE;
            CREATE TABLE {self.config.fato_deal_target} (
                deal_id TEXT,
                data_negocio_criado TEXT,
                data_agendamento TEXT,
                nome_negocio TEXT,
                etapa_id TEXT,
                valor TEXT,
                funil TEXT,
                origem TEXT,
                canal TEXT,
                detalhes TEXT,
                owner_id TEXT
            );"""
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()
            logger.info(f"Table {self.config.fato_deal_target} created with TEXT types")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def execute_query(self, conn, query, params=None):
        """Execute query and return results"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params or ())
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    data = cursor.fetchall()
                    return columns, data
            return None, None
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def truncate_and_insert(self, conn, target_table, source_query):
        """Truncate and insert data safely"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {target_table}")
                cursor.execute(f"INSERT INTO {target_table} {source_query}")
                conn.commit()
            logger.info(f"Data loaded into {target_table}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to load data: {e}")
            raise

    def recreate_fato_table(self, conn):
        """Recria a tabela com estrutura definitiva usando DATE para datas sem hora"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {self.config.fato_deal_target} CASCADE")
                cursor.execute(f"""
                CREATE TABLE {self.config.fato_deal_target} (
                    deal_id TEXT PRIMARY KEY,
                    data_negocio_criado DATE,  -- Alterado para DATE
                    data_agendamento DATE,     -- Alterado para DATE
                    nome_negocio TEXT,
                    etapa_id TEXT,
                    valor NUMERIC(15,2),
                    funil TEXT,
                    origem TEXT,
                    canal TEXT,
                    detalhes TEXT,
                    owner_id TEXT
                );""")
                conn.commit()
            logger.info(f"Tabela {self.config.fato_deal_target} recriada com tipos DATE")
        except Exception as e:
            conn.rollback()
            logger.error(f"Erro ao recriar tabela: {e}")
            raise

    def safe_convert_data_types(self, conn):
        """Conversão para tipos definitivos (DATE para datas)"""
        try:
            with conn.cursor() as cursor:
                # Converter para DATE (formato YYYY-MM-DD)
                cursor.execute(f"""
                ALTER TABLE {self.config.fato_deal_target}
                ALTER COLUMN data_negocio_criado TYPE DATE USING (
                    CASE
                        WHEN data_negocio_criado ~ '^\d{{4}}-\d{{2}}-\d{{2}}$' 
                            THEN data_negocio_criado::DATE
                        ELSE NULL
                    END
                );
                
                ALTER TABLE {self.config.fato_deal_target}
                ALTER COLUMN data_agendamento TYPE DATE USING (
                    CASE
                        WHEN data_agendamento ~ '^\d{{4}}-\d{{2}}-\d{{2}}$' 
                            THEN data_agendamento::DATE
                        ELSE NULL
                    END
                );
                
                -- Conversão do valor monetário mantida
                ALTER TABLE {self.config.fato_deal_target}
                ALTER COLUMN valor TYPE NUMERIC(15,2) USING (
                    NULLIF(regexp_replace(valor, '[^0-9.-]', '', 'g'), '')::NUMERIC
                );""")
                
                conn.commit()
            logger.info("Conversão para DATE concluída com sucesso")
        except Exception as e:
            conn.rollback()
            logger.error(f"Falha na conversão: {str(e)}")
            logger.info("Mantendo tipos TEXT como fallback")

    def check_table_exists(self, conn, table_name):
        """Check if table exists"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'trusted' 
                    AND table_name = %s
                )""", (table_name.split('.')[-1],))
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error checking table: {e}")
            return False

    def add_foreign_keys(self, conn):
        """Add foreign keys safely"""
        try:
            with conn.cursor() as cursor:
                # Check for invalid references
                cursor.execute(f"""
                SELECT COUNT(*) FROM {self.config.fato_deal_target}
                WHERE owner_id IS NOT NULL AND owner_id NOT IN (
                    SELECT owner_id FROM {self.config.dim_owners_target}
                )""")
                invalid_owners = cursor.fetchone()[0]
                
                cursor.execute(f"""
                SELECT COUNT(*) FROM {self.config.fato_deal_target}
                WHERE etapa_id IS NOT NULL AND etapa_id NOT IN (
                    SELECT etapa_id FROM {self.config.dim_etapa_target}
                )""")
                invalid_etapas = cursor.fetchone()[0]
                
                if invalid_owners > 0 or invalid_etapas > 0:
                    raise Exception(
                        f"Invalid references found: "
                        f"{invalid_owners} invalid owner_ids, "
                        f"{invalid_etapas} invalid etapa_ids"
                    )
                
                # Add FKs if no invalid references
                cursor.execute(f"""
                ALTER TABLE {self.config.fato_deal_target}
                ADD CONSTRAINT fk_owner FOREIGN KEY (owner_id) 
                REFERENCES {self.config.dim_owners_target}(owner_id)
                ON DELETE SET NULL;
                
                ALTER TABLE {self.config.fato_deal_target}
                ADD CONSTRAINT fk_etapa FOREIGN KEY (etapa_id) 
                REFERENCES {self.config.dim_etapa_target}(etapa_id)
                ON DELETE SET NULL;""")
                conn.commit()
            logger.info("Foreign keys added successfully")
        except Exception as e:
            conn.rollback()
            raise Exception(f"Failed to add FKs: {str(e)}")

    def fix_invalid_owners(self, conn):
        """Fix invalid owners by setting to NULL"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                UPDATE {self.config.fato_deal_target}
                SET owner_id = NULL
                WHERE owner_id IS NOT NULL
                AND owner_id NOT IN (
                    SELECT owner_id FROM {self.config.dim_owners_target}
                );""")
                affected = cursor.rowcount
                conn.commit()
                logger.warning(f"Set {affected} invalid owner_ids to NULL")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Error fixing invalid owners: {e}")
            return False

    def add_missing_owners(self, conn):
        """Add missing owners to dimension"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                INSERT INTO {self.config.dim_owners_target} (owner_id, owner_name)
                SELECT DISTINCT f.owner_id, 'DESCONHECIDO'
                FROM {self.config.fato_deal_target} f
                WHERE f.owner_id IS NOT NULL
                AND f.owner_id NOT IN (
                    SELECT owner_id FROM {self.config.dim_owners_target}
                )
                ON CONFLICT (owner_id) DO NOTHING;""")
                added = cursor.rowcount
                conn.commit()
                logger.info(f"Added {added} missing owners to dimension")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Error adding missing owners: {e}")
            return False

    def cleanup_temp_table(self, conn):
        """Clean up temporary table"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {self.config.fato_deal_target}_temp")
                conn.commit()
            logger.info("Temporary table cleaned up")
        except Exception as e:
            logger.error(f"Error cleaning up temp table: {e}")
            raise