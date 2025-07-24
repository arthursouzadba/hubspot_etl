import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    SOURCE_SCHEMA = "public"
    TARGET_SCHEMA = "trusted"
    
    @property
    def dim_etapa_source(self):
        return f"{self.SOURCE_SCHEMA}.dim_id_etapa_hubspot"
    
    @property
    def dim_etapa_target(self):
        return f"{self.TARGET_SCHEMA}.dim_id_etapa_hubspot"
    
    @property
    def dim_owners_source(self):
        return f"{self.SOURCE_SCHEMA}.dim_id_owners_hubspot"
    
    @property
    def dim_owners_target(self):
        return f"{self.TARGET_SCHEMA}.dim_id_owners_hubspot"
    
    @property
    def fato_deal_source(self):
        return f"{self.SOURCE_SCHEMA}.fato_id_deal_hubspot"
    
    @property
    def fato_deal_target(self):
        return f"{self.TARGET_SCHEMA}.fato_id_deal_hubspot"