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

    def validate_dimension_data(db, conn):
        """Validate dimension data before processing fact"""
        with conn.cursor() as cursor:
            # Check for NULL or empty keys in dimensions
            cursor.execute(f"""
            SELECT COUNT(*) FROM {db.config.dim_etapa_target} 
            WHERE etapa_id IS NULL OR etapa_id = ''
            """)
            null_etapas = cursor.fetchone()[0]
            
            cursor.execute(f"""
            SELECT COUNT(*) FROM {db.config.dim_owners_target} 
            WHERE owner_id IS NULL OR owner_id = ''
            """)
            null_owners = cursor.fetchone()[0]
            
            if null_etapas > 0 or null_owners > 0:
                raise Exception(
                    f"Invalid dimension data: {null_etapas} null etapas, {null_owners} null owners"
                )

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
        """Cria a tabela dim_etapa de forma idempotente"""
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.config.dim_etapa_target} (
                etapa_id TEXT PRIMARY KEY,
                pipeline TEXT,
                etapa TEXT
            );
            """
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                conn.commit()
            logger.info(f"Tabela {self.config.dim_etapa_target} verificada/criada")
        except Exception as e:
            logger.error(f"Erro ao criar tabela de etapa: {e}")
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
        """Adiciona FKs com verificação adicional"""
        try:
            with conn.cursor() as cursor:
                # Verifica se ainda existem referências inválidas
                cursor.execute(f"""
                SELECT COUNT(*) FROM {self.config.fato_deal_target}
                WHERE etapa_id IS NOT NULL 
                AND etapa_id NOT IN (
                    SELECT etapa_id FROM {self.config.dim_etapa_target}
                )
                """)
                invalid_etapas = cursor.fetchone()[0]
                
                if invalid_etapas > 0:
                    raise Exception(f"Still found {invalid_etapas} invalid etapa references after cleanup")
                
                # Adiciona as FKs
                cursor.execute(f"""
                ALTER TABLE {self.config.fato_deal_target}
                ADD CONSTRAINT fk_owner FOREIGN KEY (owner_id) 
                REFERENCES {self.config.dim_owners_target}(owner_id)
                ON DELETE SET NULL;
                
                ALTER TABLE {self.config.fato_deal_target}
                ADD CONSTRAINT fk_etapa FOREIGN KEY (etapa_id) 
                REFERENCES {self.config.dim_etapa_target}(etapa_id)
                ON DELETE SET NULL;
                """)
                
                conn.commit()
                logger.info("Foreign keys added successfully")
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to add FKs: {e}")
            raise

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

    def check_table_has_data(self, conn, table_name):
        """Verifica se a tabela contém dados"""
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1)")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Erro ao verificar dados em {table_name}: {e}")
            return False

    def insert_update_data(self, conn, target_table, source_query):
        """Atualiza dados existentes em vez de truncar"""
        try:
            table_name = target_table.split('.')[-1]  # Remove o schema do nome
            temp_table = f"temp_{table_name}"
            
            with conn.cursor() as cursor:
                # Cria tabela temporária
                cursor.execute(f"CREATE TEMP TABLE {temp_table} AS {source_query}")
                
                # Determina a estratégia de update baseada no nome da tabela
                if "dim_etapa" in target_table:
                    # Atualização para dim_etapa
                    cursor.execute(f"""
                    UPDATE {target_table} t
                    SET 
                        pipeline = s.pipeline,
                        etapa = s.etapa
                    FROM {temp_table} s
                    WHERE t.etapa_id = s.etapa_id
                    """)
                    
                    # Insere novos registros para dim_etapa
                    cursor.execute(f"""
                    INSERT INTO {target_table} (etapa_id, pipeline, etapa)
                    SELECT s.etapa_id, s.pipeline, s.etapa
                    FROM {temp_table} s
                    LEFT JOIN {target_table} t ON s.etapa_id = t.etapa_id
                    WHERE t.etapa_id IS NULL
                    """)
                    
                elif "dim_owners" in target_table:
                    # Atualização para dim_owners
                    cursor.execute(f"""
                    UPDATE {target_table} t
                    SET 
                        owner_name = s.owner_name
                    FROM {temp_table} s
                    WHERE t.owner_id = s.owner_id
                    """)
                    
                    # Insere novos registros para dim_owners
                    cursor.execute(f"""
                    INSERT INTO {target_table} (owner_id, owner_name)
                    SELECT s.owner_id, s.owner_name
                    FROM {temp_table} s
                    LEFT JOIN {target_table} t ON s.owner_id = t.owner_id
                    WHERE t.owner_id IS NULL
                    """)
                
                # Limpa a tabela temporária
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                
                conn.commit()
            logger.info(f"Dados atualizados em {target_table}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Falha na atualização: {e}")
            raise

    def process_fact(self, conn):
        """Process fact table with dependencies"""
        logger.info("Processing fact table")
        
        # Verify dimensions exist first
        if not self.check_table_exists(conn, self.config.dim_etapa_target) or \
        not self.check_table_exists(conn, self.config.dim_owners_target):
            raise Exception("Dimension tables not found. Load them first.")
        
        # Create initial table
        self.create_fato_deal_table(conn)
        self.truncate_and_insert(conn, self.config.fato_deal_target, build_fato_deal_query(self.config))
        
        # Convert data types
        try:
            self.safe_convert_data_types(conn)
            try:
                self.add_foreign_keys(conn)
            except Exception as fk_error:
                logger.warning(f"FK constraints not added: {fk_error}")
        except Exception as conv_error:
            logger.error(f"Type conversion failed: {conv_error}")

    def process_fact_with_fallback(self):
        """Processa a tabela fato com fallback caso as FKs falhem"""
        try:
            with self.get_connection() as conn:
                # Verifica se as dimensões existem
                if not self.check_table_exists(conn, self.config.dim_etapa_target) or \
                not self.check_table_exists(conn, self.config.dim_owners_target):
                    raise Exception("Dimension tables not found. Load them first.")
                
                # Cria tabela inicial
                self.create_fato_deal_table(conn)
                
                # Importa a função aqui para evitar circular imports
                from src.etl import build_fato_deal_query
                self.truncate_and_insert(conn, self.config.fato_deal_target, build_fato_deal_query(self.config))
                
                # Conversão de tipos
                self.safe_convert_data_types(conn)
                self.fix_invalid_references(conn)
                self.add_foreign_keys(conn)

                # Tenta adicionar FKs
                try:
                    self.add_foreign_keys(conn)
                except Exception as fk_error:
                    logger.warning(f"FK constraints not added: {fk_error}")
                    
        except Exception as e:
            logger.error(f"Fact processing failed: {e}")
            logger.info("Attempting fallback processing without FKs...")
            with self.get_connection() as conn:
                self.create_fato_deal_table(conn)
                from src.etl import build_fato_deal_query
                self.truncate_and_insert(conn, self.config.fato_deal_target, build_fato_deal_query(self.config))
                self.safe_convert_data_types(conn)

    def log_invalid_references(self, conn):
        """Log details about invalid references between fact and dimensions"""
        with conn.cursor() as cursor:
            # Check invalid etapa references
            cursor.execute(f"""
            SELECT f.etapa_id, COUNT(*) as invalid_count
            FROM {self.config.fato_deal_target} f
            WHERE f.etapa_id IS NOT NULL 
            AND f.etapa_id NOT IN (
                SELECT etapa_id FROM {self.config.dim_etapa_target}
            )
            GROUP BY f.etapa_id
            ORDER BY invalid_count DESC
            LIMIT 10;
            """)
            invalid_etapas = cursor.fetchall()
            
            if invalid_etapas:
                logger.warning("Top invalid etapa references:")
                for etapa_id, count in invalid_etapas:
                    logger.warning(f"etapa_id: {etapa_id} - {count} records")

            # Check invalid owner references
            cursor.execute(f"""
            SELECT f.owner_id, COUNT(*) as invalid_count
            FROM {self.config.fato_deal_target} f
            WHERE f.owner_id IS NOT NULL 
            AND f.owner_id NOT IN (
                SELECT owner_id FROM {self.config.dim_owners_target}
            )
            GROUP BY f.owner_id
            ORDER BY invalid_count DESC
            LIMIT 10;
            """)
            invalid_owners = cursor.fetchall()
            
            if invalid_owners:
                logger.warning("Top invalid owner references:")
                for owner_id, count in invalid_owners:
                    logger.warning(f"owner_id: {owner_id} - {count} records")

    def validate_data_consistency(self, conn):
        """Valida a consistência dos dados nas tabelas relacionadas"""
        try:
            with conn.cursor() as cursor:
                # Verifica referências inválidas
                cursor.execute(f"""
                SELECT COUNT(*) FROM {self.config.fato_deal_target}
                WHERE etapa_id IS NOT NULL 
                AND etapa_id NOT IN (
                    SELECT etapa_id FROM {self.config.dim_etapa_target}
                )
                """)
                invalid_etapas = cursor.fetchone()[0]
                
                cursor.execute(f"""
                SELECT COUNT(*) FROM {self.config.fato_deal_target}
                WHERE owner_id IS NOT NULL 
                AND owner_id NOT IN (
                    SELECT owner_id FROM {self.config.dim_owners_target}
                )
                """)
                invalid_owners = cursor.fetchone()[0]
                
                if invalid_etapas > 0 or invalid_owners > 0:
                    logger.warning(f"Data consistency issues: {invalid_etapas} invalid etapa references, {invalid_owners} invalid owner references")
                else:
                    logger.info("Data consistency validated - no invalid references found")
                    
        except Exception as e:
            logger.error(f"Error validating data consistency: {e}")
            raise

    def fix_invalid_references(self, conn):
        """Corrige referências inválidas antes de aplicar FKs"""
        try:
            with conn.cursor() as cursor:
                # 1. Adiciona etapas faltantes à dimensão
                cursor.execute(f"""
                INSERT INTO {self.config.dim_etapa_target} (etapa_id, pipeline, etapa)
                SELECT DISTINCT f.etapa_id, 'DESCONHECIDO', 'DESCONHECIDO'
                FROM {self.config.fato_deal_target} f
                WHERE f.etapa_id IS NOT NULL
                AND f.etapa_id NOT IN (
                    SELECT etapa_id FROM {self.config.dim_etapa_target}
                )
                ON CONFLICT (etapa_id) DO NOTHING;
                """)
                
                # 2. Adiciona owners faltantes à dimensão
                cursor.execute(f"""
                INSERT INTO {self.config.dim_owners_target} (owner_id, owner_name)
                SELECT DISTINCT f.owner_id, 'DESCONHECIDO'
                FROM {self.config.fato_deal_target} f
                WHERE f.owner_id IS NOT NULL
                AND f.owner_id NOT IN (
                    SELECT owner_id FROM {self.config.dim_owners_target}
                )
                ON CONFLICT (owner_id) DO NOTHING;
                """)
                
                # 3. Remove referências completamente inválidas (se necessário)
                cursor.execute(f"""
                UPDATE {self.config.fato_deal_target}
                SET etapa_id = NULL
                WHERE etapa_id IS NOT NULL
                AND etapa_id NOT IN (
                    SELECT etapa_id FROM {self.config.dim_etapa_target}
                );
                """)
                
                conn.commit()
                logger.info("Invalid references fixed successfully")
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error fixing references: {e}")
            raise