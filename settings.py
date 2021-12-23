import os
import snowflake.connector

class Settings:
    SF_ACCOUNT = os.environ.get("SF_ACCOUNT", "dza89750")
    SF_USER = os.environ.get("SF_USER", "TALEND_CLOUD")
    SF_PASSWORD = os.environ.get("SF_PASSWORD", "%8kxmsQe4&p6")
    SF_ROLE = "SYSADMIN"
    SF_DATABASE = "retail_datawarehouse"
    SF_SCHEMA = "dbo"
    SF_WAREHOUSE = "DATA_LOADING_WH"
    TIMESTAMP_NTZ_OUTPUT_FORMAT = "YYYY-MM-DD HH24:MI:SS"
    INPUT_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\'
    ERROR_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\Errors'
    SF_CONVERTED_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\Converted'
    SF_ERROR_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\SFError'
    
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
