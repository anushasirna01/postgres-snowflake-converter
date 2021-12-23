import re
import sys
import os
import string
import traceback
#from settings import Settings
#import pandas as pd
from datetime import datetime

#TODO: 1. IF THEN END IF constructs are to be handled 
#TODO: 3. If the original script contains permissions, they have to be removed 
#class Settings:
    # SF_ACCOUNT = os.environ.get("SF_ACCOUNT", "dza89750")
    # SF_USER = os.environ.get("SF_USER", "TALEND_CLOUD")
    # SF_PASSWORD = os.environ.get("SF_PASSWORD", "%8kxmsQe4&p6")
    # SF_ROLE = "SYSADMIN"
    # SF_DATABASE = "retail_datawarehouse"
    # SF_SCHEMA = "dbo"
    # SF_WAREHOUSE = "snowflake_wh"
    # TIMESTAMP_NTZ_OUTPUT_FORMAT = "YYYY-MM-DD HH24:MI:SS"
    # INPUT_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\'
    # ERROR_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\Errors'
    # SF_CONVERTED_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\Converted'
    # SF_ERROR_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\SFError'
    
    # def get_snowflake_connection(self):
    #     ctx = snowflake.connector.connect(
    #         user=f'{self.SF_USER}',
    #         password=f'{self.SF_PASSWORD}',
    #         account=f'{self.SF_ACCOUNT}',
    #         role=f'{self.SF_ROLE}'
    #         )
    #     return ctx

def read_input_file(file_name):
    fp = open(file_name,encoding="utf16")
    #fp = open(file_name)
    postgresql_proc = fp.read()
    return postgresql_proc

def apply_regex_sub(regex, sub_string, expression):
    p = re.compile(regex, flags=re.MULTILINE.IGNORECASE)
    return re.sub(p, sub_string, expression)


def postgresql_convert(postgresql_proc, file_name, database, schema, warehouse, timestamp_ntz_output_format):
    result = apply_regex_sub(r"[\t]", "    ", postgresql_proc )  # Replace Tab with 4 spaces
    result = apply_regex_sub(r".+DROP TABLE IF EXISTS.+\n", "", result)  # Remove DROP Table command
    result = apply_regex_sub(r"(AS\n\s+\n\n)|(AS\n\n)", "AS\n", result)  # Remove blank line after AS
    #result = apply_regex_sub(r" +CREATE INDEX.+\n", "", result)  # Remove CREATE INDEX
    #result = "\n".join([ll.rstrip() for ll in result.splitlines() if ll.strip()]) #remove empty lines
        
    altered_stmts = []
    #split the beginning part of the code i.e. function header before BEGIN statement 
    proc_header = re.compile(r"BEGIN\n|BEGIN ").split(result)[0]
    #Get the comments from the header
    comments = ''
    if re.search('(?s)\/\*(.*)\*\/', proc_header):
        comments = re.search('(?s)\/\*(.*)\*\/', proc_header).group(0)
    proc_body = re.compile(r"BEGIN\n|BEGIN ").split(result,maxsplit=1)[1]
    
    #get parameters 
    function_parameters = proc_header[proc_header.index('(') + 1:proc_header.index(')')].split(',')
    parameter = ''
    data_type = ''
    time_datatype = ['timestamp','date','time','interval']
    boolean_datatype = ['boolean','bit']
    semistructure_datatype = ['json','xml','jsonb','[]']
    character_datatype = ['varying','varchar','character','text']
    integer_datatype = ['smallint','integer','bigint','decimal','numeric','real','double','smallserial','serial','bigserial']
    for i in range(0, len(function_parameters)):
        if 'OUT' not in function_parameters[i]:
            if any(x in function_parameters[i] for x in character_datatype):
               data_type = 'VARCHAR' 
            elif any(x in function_parameters[i] for x in time_datatype):
                 data_type = 'TIMESTAMP_NTZ'
            elif any(x in function_parameters[i] for x in boolean_datatype):
                 data_type = 'BOOLEAN'
            elif any(x in function_parameters[i] for x in semistructure_datatype):
                 data_type = 'VARIANT'
            elif any(x in function_parameters[i] for x in integer_datatype):
                 data_type = 'NUMBER'           
            parameter += function_parameters[i].strip()[:function_parameters[i].strip().index(' ')] + ' '+ data_type +' ,' 

       
    result = "\ncreate or replace procedure "+file_name+"(" + parameter[:-1] + ")\n    returns VARIANT\n    language javascript\n    strict\n    execute as caller\n    " +  comments + "\n as\n    $$\n    try{\n    snowflake.execute ( {sqlText: \"use database "+database+";\"} );\n    snowflake.execute ( {sqlText: \"use schema "+schema+";\"} );\n    snowflake.execute ( {sqlText: \"use warehouse "+warehouse+";\"} );\n    snowflake.execute ( {sqlText: \"alter session set timestamp_ntz_output_format = '"+timestamp_ntz_output_format+"';\"} );\n"  # Replace BEGIN with Snowflake Procedure Header
    
    #get individual statement- assuming they will be terminated by ; and wrap them in snowflake.execute
    altered_stmts = []
    catch_block_append = '''
           sqlCmd = `SELECT object_construct('inserted_rows',"number of rows inserted",'updated_rows',"number of rows updated",'status','success') as response FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))`;
                    sqlStmt = snowflake.createStatement( {sqlText: sqlCmd} );
                    rs = sqlStmt.execute();
                    rs.next();
                    sqlResult =  rs.getColumnValue(1);
            }
           catch(err)
            {
                    sqlResult =  err;
            }


            return sqlResult;

        $$
        ;
        '''
    if 'END IF' in proc_body:
         #print(proc_body)
        #if_array = []
        #not_if_array = []
        outFile_get = open("D:\\ASM\\Snowflake\\Output\\test.txt", "w")
        outFile_get1 = open("D:\\ASM\\Snowflake\\Output\\test1.txt", "w")

        copy = False
        for position, line in enumerate(proc_body.splitlines()):
             print(line.strip())
             if 'END;' in line:
                if os.stat("D:\\ASM\\Snowflake\\Output\\test1.txt").st_size != 0:
                    fil =  open("D:\\ASM\\Snowflake\\Output\\test1.txt")
                    file_content = fil.read()
                    non_if_array = file_content
                    altered_stmts = []
                    snow_code(non_if_array, altered_stmts) 
                    result = result + '\n \n'.join(altered_stmts)
                    altered_stmts = []
                    outFile_get1 = open("D:\\ASM\\Snowflake\\Output\\test1.txt", "w")
                copy = True
                continue    
             #if 'IF' in line and '(' in line and ')' in line and 'NULLIF' not in line:
             if 'IF' in line and 'THEN' in line and 'NULLIF' not in line:    
              copy = True
              #print('if')
              #print(os.stat("D:\\ASM\\Snowflake\\Output\\test1.txt").st_size)
              if os.stat("D:\\ASM\\Snowflake\\Output\\test1.txt").st_size != 0:
               #  exit()
                fil =  open("D:\\ASM\\Snowflake\\Output\\test1.txt")
                file_content = fil.read()
                non_if_array = file_content
                altered_stmts = []
                snow_code(non_if_array, altered_stmts) 
                result = result + '\n \n'.join(altered_stmts)
                result = result + '\n' + line.replace('IF', 'if') + '\n  { \n'
                altered_stmts = []
                outFile_get1 = open("D:\\ASM\\Snowflake\\Output\\test1.txt", "w")
                
             # outFile_get.write(line.strip() + '\n')
              continue
             elif 'END IF' in line:
             # outFile_get.write(line.strip() + '\n')
              #print(os.stat("D:\\ASM\\Snowflake\\Output\\test.txt").st_size)
              #print('end if')
              if os.stat("D:\\ASM\\Snowflake\\Output\\test.txt").st_size != 0:
                # print(os.stat("D:\\ASM\\Snowflake\\Output\\test.txt").st_size)
                 fil2 =  open("D:\\ASM\\Snowflake\\Output\\test.txt")
                 file_content2 = fil2.read()
                 if_array = file_content2
                 altered_stmts = []
                 snow_code(if_array, altered_stmts) 
                 result = result + '\n \n'.join(altered_stmts)
                 result = result +  '\n  } \n '
                 altered_stmts = []
                 outFile_get1 = open("D:\\ASM\\Snowflake\\Output\\test.txt", "w")
              copy = False
              continue
             elif copy:
              outFile_get = open("D:\\ASM\\Snowflake\\Output\\test.txt", "a")
              if line.strip() == 'ELSE':
                  print(line.strip())
                  print(len(line.strip()))
                  outFile_get.write('\n' + 'ELSE PLACE HOLDER \n')
              else: 
                  if line.strip() == 'THEN':   
                    outFile_get.write(line.strip().replace('THEN', '') + '\n')
                  else:
                    outFile_get.write(line.strip() + '\n')        
              outFile_get.close()
             elif copy == False: 
              outFile_get1 = open("D:\\ASM\\Snowflake\\Output\\test1.txt", "a")
              if line.strip() == 'THEN':   
                    outFile_get1.write(line.strip().replace('THEN', '') + '\n')
              else:
                    outFile_get1.write(line.strip() + '\n')  
              outFile_get1.close()
             
    else:
        snow_code(proc_body,altered_stmts)
   
    result = result + '\n \n'.join(altered_stmts)
    result = result + '\n \n' + catch_block_append
    #result = apply_regex_sub(r"CREATE TEMP TABLE", "CREATE OR REPLACE TEMP TABLE", result)  #  wrap in snowflake.execute
    #result = apply_regex_sub(r"(?<=[^)]);(?=[^\"]) *\n", ";`} );\n\n", result)  # Close snowflake.execute statement
    result = apply_regex_sub(r"(?<=\n) +(?=\n)", "", result)  # Remove spaces in empty lines

    
    #result = apply_regex_sub(r'DATETIME NULL', result, 'DATETIME')  # Remove NULL when INT NULL
    #result = apply_regex_sub(r'INT NULL', result, 'INT')  # Remove NULL when INT NULL
    #result = apply_regex_sub(r'\) NULL', result, ')')  # Remove NULL when a precision is specified
    #result = apply_regex_sub(r'((GO\n)|(GO\s\n))', result, '')  # Remove GO
    #result = apply_regex_sub(r'[\t]', result, '    ')  # Replace Tab with 4 spaces
    #result = apply_regex_sub(r'dbo\.', result, '')  # Remove dbo.
    #result = apply_regex_sub(r'(DROP(.)+)\n', result, '')  # Remove DROP Table reference
    #result = apply_regex_sub(r'\sDEFAULT(.+,)', result, '')  # Remove DEFAULT
    #result = apply_regex_sub(r'\sBIT', result, ' BOOLEAN')  # Replace BIT with BOOLEAN
    #result = apply_regex_sub(r'\sBIT NULL', result, ' BOOLEAN')  # Replace BIT with BOOLEAN
    result = "\n".join([ll.rstrip() for ll in result.splitlines() if ll.strip()]) #remove empty lines
    return result

def snow_code(proc_body,altered_stmts):
    for stmt in re.compile(r";").split(proc_body):
        
        if 'ON CONFLICT' in stmt:
            #print(stmt)
            sql_str = stmt
            table_name = sql_str[sql_str.index('dbo.'):][:sql_str[sql_str.index('dbo.'):].index(' ')]
            src_query = sql_str[sql_str.index('SELECT'):][:sql_str[sql_str.index('SELECT'):].index('ON CONFLICT')]
            business_keys = sql_str[sql_str.index('ON CONFLICT('):][:sql_str[sql_str.index('ON CONFLICT'):].index(')')].replace('ON CONFLICT(', '').split(',')
            key_str = ""
            for i in range(0, len(business_keys)):    
                key_str += "trgt." + business_keys[i].strip() + ' = src.' + business_keys[i].strip() +  ' AND ' 
            insert_query = "INSERT " + sql_str[sql_str.index('INSERT INTO'):][sql_str[sql_str.index('INSERT INTO'):].index('('):sql_str[sql_str.index('INSERT INTO'):].index(')') + 1]
            # value query logic 
            final_value_query = ''
            #get the content between SELECT and FROM
            search_groups = re.search('SELECT(.|\n)*FROM|select(.|\n)*from', src_query)
            if search_groups:
                src_cols = search_groups.group(0)
            #remove distinct 
            src_cols = src_cols.replace('SELECT','').replace('select','')
            src_cols = src_cols.replace(' DISTINCT','').replace('distinct','')
            src_cols = src_cols.replace('FROM','').replace('from','')

            #Split by comma followed by
            #src_col_list = re.compile(r" ,").split(src_cols)
            src_col_list = re.split(' ,|, |,\n|\n,', src_cols)
            value_query = ' VALUES ('
            for col in src_col_list:
                if 'md5' in col:
                    value_query += 'audit_hash_md5' + ')'
                    break
                if '.' in col :
                    value_query += col[col.index('.') + 1:].strip() + ',' 
                if ' AS ' in col:
                    value_query += col[col.index(' AS ') + 4:].strip() + ','
                if not ('.' in col or ' AS ' in col):
                    value_query += col.strip() + ','
                

            # if ' md5' in src_query:
            #     insert_value_query = src_query.replace(src_query[src_query.index('md5'):][:src_query[src_query.index('md5'):].index('AS')],'')
            # else:
            #     insert_value_query = src_query   
            # if 'DISTINCT' in insert_value_query:
            #     insert_value_query = insert_value_query[:insert_value_query.index('FROM')].replace('SELECT', '').replace('DISTINCT', '').split(',')
            # else:
            #     insert_value_query = insert_value_query.replace('SELECT', '').split(',')

            # for i in range(0, len(insert_value_query)):
            #     if '.' in insert_value_query[i]:
            #         insert_value_query[i] = insert_value_query[i][insert_value_query[i].index('.') + 1:]      
            #     elif ' AS ' in insert_value_query[i]:
            #         insert_value_query[i] = insert_value_query[i][insert_value_query[i].index('AS') + 2:]
            #     else:
            #         insert_value_query[i]
            #     final_value_query += ',' + insert_value_query[i].strip() + '\n\n'  
      
            if "excluded." in sql_str:
               if "WHERE" in sql_str[sql_str.index('UPDATE'):]:
                  update_query = sql_str[sql_str.index('UPDATE'):][:sql_str[sql_str.index('UPDATE'):].index('WHERE')].replace('excluded.', 'src.')
               else:
                  update_query = sql_str[sql_str.index('UPDATE'):][:sql_str[sql_str.index('UPDATE'):].index('\n\n')].replace('excluded.', 'src.')    
            else:
               update_query = sql_str[sql_str.index('UPDATE'):][:sql_str[sql_str.index('UPDATE'):].index('\n\n')]    
            stmt = 'MERGE INTO ' + table_name +' AS trgt USING ( ' + src_query.strip() + ' \n) AS src \n  ON ' +  key_str[:-4].strip() + '\n WHEN NOT MATCHED THEN \n' + insert_query.strip() + '\n '+ value_query + ' \n WHEN MATCHED AND trgt.audit_hash_md5 <> src.audit_hash_md5 THEN \n ' + update_query.strip() 
        
        if '$BODY$' in stmt or ('RETURN QUERY' in stmt and 'table_log' in stmt) or len(stmt.strip()) < 7 or 'ALTER FUNCTION' in stmt or 'GRANT EXECUTE' in stmt or 'CREATE INDEX' in stmt or '$function$' in stmt:
            continue
            #altered_stmts.append(catch_block_append)
        elif 'ELSE PLACE HOLDER' in stmt:
            stmt = stmt.replace('ELSE PLACE HOLDER',' } \n else \n {\n')  
            altered_stmts.append(stmt)
            continue
        else:
            if ' jobid' in stmt or ',jobid' in stmt:
                parameters = ',binds: [JOBID]'
                stmt = stmt.replace(' jobid',' :1 ')
                stmt = stmt.replace(',jobid',', :1 ') 
            elif '_audit_datetime' in stmt:
                parameters = ',binds: [audit_datetime]'
                stmt = stmt.replace(' _audit_datetime',' :1 ')
                #stmt = stmt.replace(',jobid',', :1 ') 
            else:
                parameters = ''

            if 'UUID' in stmt:
                stmt = stmt.replace('UUID', 'varchar')

            if '::character varying[]' in stmt:
                stmt = stmt.replace('::character varying[]', '::varchar')   

            if '::interval' in stmt:
                stmt = stmt.replace('::interval', '::time')         

            # if 'generate_series' in stmt:
            #     #get the column
            #     generate_series = stmt[stmt.index('generate_series('):stmt.index(' AS ')]
            #     series_params = stmt[stmt.index('generate_series('):stmt.index(' AS ')].replace('generate_series(','').replace(')','').split(',')
            #     stmt = stmt.replace(generate_series, f'row_number() OVER(ORDER BY (SELECT 1)) - 1 + {series_params[0]}')
            #     stmt = stmt + f', table(generator(rowcount => {series_params[1]} + {series_params[0]})) v'
            
        altered_stmts.append('  snowflake.execute ( {sqlText: `\n    ' + stmt.replace('now()','CURRENT_TIMESTAMP').replace('CREATE TEMP TABLE', ' CREATE OR REPLACE TEMPORARY TABLE ') + ';`' + parameters + '} );')
    return altered_stmts    
def main():
    #result_list = []
    #input_folder_path = 'D:\\ASM\\Snowflake\\InputProcs\\'
    #output_folder_path = 'D:\\ASM\\Snowflake\\Converted'
    #file_name = sys.argv[1]
    SF_ACCOUNT = os.environ.get("SF_ACCOUNT", "dza89750")
    SF_USER = os.environ.get("SF_USER", "TALEND_CLOUD")
    SF_PASSWORD = os.environ.get("SF_PASSWORD", "%8kxmsQe4&p6")
    SF_ROLE = "SYSADMIN"
    SF_DATABASE = "retail_datawarehouse"
    SF_SCHEMA = "dbo"
    SF_WAREHOUSE = "snowflake_wh"
    TIMESTAMP_NTZ_OUTPUT_FORMAT = "YYYY-MM-DD HH24:MI:SS"
    INPUT_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\'
    ERROR_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\Errors'
    SF_CONVERTED_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\Converted'
    SF_ERROR_FOLDER_PATH = 'D:\\ASM\\Snowflake\\Git\\ASM Retail DW\\awsdatawarehouse\\dbo\\Function\\SFError'

    files = [f for f in os.listdir(INPUT_FOLDER_PATH) if os.path.isfile(os.path.join(INPUT_FOLDER_PATH, f))]
    for file_name in filter(lambda x: 1 == 1, files): #'usp_fact_order_image_sas_cont' in x

        postgresql_proc = read_input_file(os.path.join(INPUT_FOLDER_PATH,file_name))
        #Extract only function name
        if '(' in file_name and ')' in file_name:
            file_name = file_name[:file_name.index('(')]
        else:
             file_name = file_name[:-4] 
        # try:
        #     detail_dict = dict()
        #     detail_dict['StoredProc'] = file_name
        result = postgresql_convert(postgresql_proc, file_name, SF_DATABASE, SF_SCHEMA, SF_WAREHOUSE, TIMESTAMP_NTZ_OUTPUT_FORMAT)
        # except Exception as e:
        #     out_file = open(f'{settings.ERROR_FOLDER_PATH}/{file_name}.js','w')
        #     out_file.write('/* \n' + str(traceback.format_exc()) + ' */\n\n' + postgresql_proc)
        #     out_file.close()
        #     print(f'{file_name} error while converting.')
        #     detail_dict['ConversionError'] = str(traceback.format_exc())
        #     result_list.append(detail_dict)
        #     continue
        #print(result)
        #try to run the script in snowflake and and test execution
        # try:
        #     output = run_script_in_snowflake(result, file_name)
        # except Exception as e:
        #     out_file = open(f'{settings.SF_ERROR_FOLDER_PATH}/{os.path.splitext(file_name)[0]}.js','w')
        #     out_file.write('/* \n' + str(traceback.format_exc()) + ' */\n\n' + result)
        #     out_file.close()
        #     print(f'{file_name} error in Snowflake.')
        #     detail_dict['SnowflakeError'] = str(traceback.format_exc())
        #     result_list.append(detail_dict)
        #     continue
        
        #detail_dict['Result'] = output
        out_file = open(f'{SF_CONVERTED_FOLDER_PATH}/{os.path.splitext(file_name)[0]}.js','w')
        out_file.write(result)
        out_file.close()

        #result_list.append(detail_dict)
        #result_df.append(pd.DataFrame.from_dict(detail_dict))
    #pd.DataFrame(result_list).to_csv(f"ConversionResults_{datetime.now().strftime('%d_%m_%Y_%H_%M_%S')}.csv",index=False)

        #print(f'{file_name} done.')
# def run_script_in_snowflake(sql_command, proc_name):
#     ctx = settings.get_snowflake_connection()
#     cs = ctx.cursor()
#     try:
#         test_job_id = 'TestJobID'
#         ctx.cursor().execute(f"USE DATABASE {settings.SF_DATABASE}")
#         ctx.cursor().execute(f"USE WAREHOUSE {settings.SF_WAREHOUSE}")
#         ctx.cursor().execute(f"USE SCHEMA {settings.SF_SCHEMA}")
#         ctx.cursor().execute(f"USE ROLE {settings.SF_ROLE}")
#         cs.execute(f"{sql_command};")
#         cs.execute(f"CALL {proc_name}('{test_job_id}');")
#         one_row = cs.fetchone()
#         print(f'Ran the stored proc {proc_name} in Snowflake. Result: {one_row[0]}')
#         return one_row[0]
#     finally:
#         cs.close()
#     ctx.close()

if __name__ == "__main__":
    #run_script_in_snowflake('test')
    #settings = Settings()
    main()
