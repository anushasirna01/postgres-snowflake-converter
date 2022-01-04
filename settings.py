import os
import snowflake.connector

class Settings:
    SF_ACCOUNT = os.environ.get("SF_ACCOUNT", "yrt76")
    SF_USER = os.environ.get("SF_USER", "DB_USER")
    SF_PASSWORD = os.environ.get("SF_PASSWORD", "%******")
    SF_ROLE = "SYSADMIN"
    SF_DATABASE = "warehouse"
    SF_SCHEMA = "dbo"
    SF_WAREHOUSE = "DATA_LOADING_WH"
    TIMESTAMP_NTZ_OUTPUT_FORMAT = "YYYY-MM-DD HH24:MI:SS"
    INPUT_FOLDER_PATH = 'D:\\DS\\Snowflake\\Git\\dbo\\Function\\'
    ERROR_FOLDER_PATH = 'D:\\DS\\Snowflake\\Git\\dbo\\Function\\Errors'
    SF_CONVERTED_FOLDER_PATH = 'D:\\DS\\Snowflake\\Git\\dbo\\Function\\Converted'
    SF_ERROR_FOLDER_PATH = 'D:\\DS\\Snowflake\\Git\\Function\\SFError'
    
    def get_snowflake_connection(self):
        ctx = snowflake.connector.connect(
            user=f'{self.SF_USER}',
            password=f'{self.SF_PASSWORD}',
            account=f'{self.SF_ACCOUNT}',
            role=f'{self.SF_ROLE}',
            warehouse=f'{self.SF_WAREHOUSE}',
            database=f'{self.SF_DATABASE}',
            schema=f'{self.SF_SCHEMA}'
            
            )
        return ctx
