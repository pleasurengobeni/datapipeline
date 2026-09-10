
import psycopg2
import zipfile
import io   
import os
import json
import csv
import re
from datetime import datetime
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mssql_hook import MsSqlHook



import shutil
from slack_sdk import WebClient
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine


def get_hook(conn_id):
    conn = BaseHook.get_connection(conn_id)
    conn_type = (conn.conn_type or "").lower()

    if conn_type in ("postgres", "postgresql"):
        return PostgresHook(postgres_conn_id=conn_id)
    if conn_type == "mysql":
        return MySqlHook(mysql_conn_id=conn_id)
    if conn_type in ("mssql", "ms_sql", "sqlserver"):
        return MsSqlHook(mssql_conn_id=conn_id)

    raise ValueError(f"Unsupported connection type: {conn_type}")

def send_slack_message(message):
    # Usage example
    slack_token = Variable.get('slack_token')
    channel = "#data-pipeline-alerts"
    
    # message = message if message else "but error occurred"

    # client = WebClient(token=slack_token)
    # response = client.chat_postMessage(
    #     channel=channel,
    #     text=message
    # )
    
    # if response["ok"]:
    #     print("Slack message sent successfully!")
    # else:
    #     print(f"Failed to send Slack message. Error: {response['error']}")
        
        
def map_data_types(data_type):
    data_type = data_type.lower()
    
    data_type_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'TEXT',
            'datetime64[ns]': 'TIMESTAMP WITHOUT TIME ZONE',
            'datetime64': 'TIMESTAMP',
            'datetime': 'timestamp',
            'bool': 'BOOLEAN'
            # Add more mappings as needed
    }
    
    return data_type_mapping.get(data_type, 'TEXT')  # Default to TEXT for unknown types       
# def create_table_from_csv_structure(csv_file_path, schema, table_name, sep, conn_string):

#     # Establish a connection to the PostgreSQL database
#     conn = psycopg2.connect(conn_string)
#     cursor = conn.cursor()
    
  
#     # Read the first line of the CSV file to extract column names and data types
#     with open(csv_file_path, 'r') as csv_file:
#         header = csv_file.readline().strip()
#         column_names = header.split(sep)
        
#         # Assuming all columns are of type VARCHAR, you can modify this as needed
#         column_definitions = [f"{clean_column_name(column_name)} VARCHAR" for column_name in column_names]
#         create_table_query = f"CREATE TABLE {schema}.{table_name} ( upload_date timestamp, file_name varchar, {', '.join(column_definitions)});"
#         create_table_query_stage = create_table_query.replace(f"{schema}.{table_name}", f"pipeline.{schema}_{table_name}_stage")
        
#         cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s", (schema,))
#         existing_schema = cursor.fetchone()

#         if not existing_schema:
#             # Create the schema
#             cursor.execute("CREATE SCHEMA {}".format(schema))
        
        
#         # Execute the CREATE TABLE statement
#         cursor.execute(create_table_query)
#         cursor.execute(create_table_query_stage)

#     # Commit the changes and close the database connection
#     conn.commit()
#     cursor.close()
#     conn.close()

def create_table_from_csv_structure(csv_file_path, shema_file_path, schema, table_name, sep, conn_string, transformations):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    with open(csv_file_path, 'r') as csv_file:
        header = csv_file.readline().strip()
        column_names = header.split(sep)

    # Stage table name
    stage_schema = "pipeline"
    stage_table = f"{schema}_{table_name}_stage"

    # Check if tables exist
    target_exists = check_table_exists(schema, table_name, conn_string)
    stage_exists = check_table_exists(stage_schema, stage_table, conn_string)
    
    print(table_name, stage_table)

    if not stage_exists:
        # Stage table: All columns as VARCHAR
        transformation_map = {k: v.upper() for d in transformations for k, v in d.items()}
        column_definitions_stage = [
            f"{clean_column_name(col)} {transformation_map.get(col, 'VARCHAR')}"
            for col in column_names
        ]
        
        create_stage_query = f"""
            create schema if not exists {stage_schema};
            CREATE TABLE {stage_schema}.{stage_table} (
                upload_date TIMESTAMP,
                file_name VARCHAR,
                {', '.join(column_definitions_stage)}
            );
        """
        create_query = f"""
            create schema if not exists {schema};
            CREATE TABLE {schema}.{table_name} (
                upload_date TIMESTAMP,
                file_name VARCHAR,
                {', '.join(column_definitions_stage)}
            );
        """
        
        print(create_stage_query)
        cursor.execute(create_stage_query)
        cursor.execute(create_query)
        
        
        print(f"Tables created.")
    else:
        print(f"Stage table '{stage_schema}.{stage_table}' already exists. Skipping creation.")

    conn.commit()
    cursor.close()
    conn.close()
    
    
    
def create_zip_file(file_path, zip_path):
    
    if os.path.exists(zip_path):
        os.remove(zip_path)
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(file_path, arcname=os.path.basename(file_path))

# def copy_data_from_zipped_csv_with_metadata(connection, zip_file_path, schema_file, sep, schema, table_name, replace_values_config):
    
#     with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        
#         csv_file_name = zip_ref.namelist()[0]  # Assuming the first file in the zip is the CSV file
#         with zip_ref.open(csv_file_name) as csv_file:
#             # Extract the CSV file from the zip archive
#             csv_content = io.TextIOWrapper(csv_file, encoding='utf-8')
            
#             # Get the current timestamp as the data import timestamp
#             import_timestamp = datetime.now()
            
#             cols =get_column_names_from_zipped_csv(zip_file_path, sep)
#             # Create a cursor and copy data from the CSV file
            
#             table_name_stage = f"pipeline.{schema}_{table_name}_stage"
            
            
#             next(csv_content)
#             with connection.cursor() as cursor:
#                 cursor.execute(f'set search_path to pipeline')
                
#                 cursor.execute(f"truncate table {table_name_stage};")
#                 cursor.copy_from(csv_content, f"{schema}_{table_name}_stage", sep=sep, null='', columns=cols)
                
#                 # Set the import timestamp for each imported row
#                 cursor.execute(f"UPDATE {table_name_stage} SET upload_date = %s", (import_timestamp,))
#                 cursor.execute(f"UPDATE {table_name_stage} SET file_name = %s", (zip_file_path,))
                
#                 if replace_values_config:
#                     for _replace in replace_values_config:
#                         for column, transf in _replace.items():
#                             if "int" in transf[0].lower():
#                                 cursor.execute(f"UPDATE {table_name_stage} SET {column} = {transf[2]} where {column}={transf[1]}")
#                             elif {transf[2]} == 'null':
#                                 cursor.execute(f"UPDATE {table_name_stage} SET {column} = {transf[2]} where {column}='{transf[1]}'")
#                             elif {transf[1]} == 'null':
#                                 cursor.execute(f"UPDATE {table_name_stage} SET {column} = {transf[2]} where {column}={transf[1]}")
#                             else:
#                                 cursor.execute(f"UPDATE {table_name_stage} SET {column} = '{transf[2]}' where {column}='{transf[1]}'")
                                
#                 cursor.execute(f"insert into {schema}.{table_name} select * from {table_name_stage};")
#                 cursor.execute(f"truncate table {table_name_stage};")
                
#             connection.commit()

def copy_data_from_zipped_csv_with_metadata(connection, zip_file_path, schema_file, sep, schema, table_name, replace_values_config, transformations):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        csv_file_name = zip_ref.namelist()[0]  # Assuming the first file in the zip is the CSV file
        with zip_ref.open(csv_file_name) as csv_file:
           
            import chardet
            
            raw = csv_file.read(10000)
            csv_file.seek(0)
            detected = chardet.detect(raw)
            detected_encoding = detected['encoding'] or 'utf-8'
            
            print('ENCODING: ', detected_encoding)
            
            # csv_content = io.TextIOWrapper(csv_file, encoding='utf-8')
            csv_content = io.TextIOWrapper(csv_file, encoding=detected_encoding, errors='replace')
            
            import_timestamp = datetime.now()  # Get current timestamp
            
            cols = get_column_names_from_zipped_csv(zip_file_path, sep)  # Extract column names
            table_name_stage = f"{schema}_{table_name}_stage"
            
            print(cols)
            
            next(csv_content)  # Skip header row before processing
            corrected_lines = []  # Store modified lines

            for line in csv_content:
                columns = line.strip().split(sep)  # Split the line into columns
                print('line: ', line)
                
                if len(columns) < len(cols):
                    # Add empty columns if fewer
                    columns += [''] * (len(cols) - len(columns))
                elif len(columns) > len(cols):
                    # Remove extra columns if more
                    columns = columns[:len(cols)]
                    
                #if columns contains this string '<<<|SEP_PLACEHOLDER|>>>' replace it with sep
                #columns=[col.replace('<<<|SEP_PLACEHOLDER|>>>',sep).replace('<<<|BACKSLASH_PLACEHOLDER|>>>', '\\') if str(col) else col for col in columns]
                
                
                corrected_lines.append(sep.join(columns))  # Reconstruct the line

            corrected_csv_content = "\n".join(corrected_lines)  # Reconstruct full content
            corrected_csv_io = io.StringIO(corrected_csv_content)  # Convert back to file-like object

            #in corrected_csv_io change any occurrence of b'\x01' or b'\x00'
            #extract the numeric using regex and remain with the value
            corrected_csv_io.seek(0)  # Rewind to the beginning of the StringIO buffer
            corrected_csv_content = corrected_csv_io.getvalue() 
            corrected_csv_content = corrected_csv_content.replace("b'<<<|BACKSLASH_PLACEHOLDER|>>>x01'",'01')  # Remove any occurrences of b'\x01' or b'\x00'
            corrected_csv_content = corrected_csv_content.replace("b'<<<|BACKSLASH_PLACEHOLDER|>>>x00'",'00')
            corrected_csv_io = io.StringIO(corrected_csv_content)  # Convert back to file-like object
            
            
            with connection.cursor() as cursor:
                cursor.execute("SET search_path TO pipeline")
                cursor.execute(f"TRUNCATE TABLE {table_name_stage}")
                
                connection.commit()
                
                #cursor.copy_from(corrected_csv_io, f"{schema}_{table_name}_stage", sep=sep, null='', columns=cols)

            # --- Step 1: Load corrected_csv_io into a DataFrame ---
            corrected_csv_io.seek(0)  # Rewind to the beginning of the StringIO buffer
            df_loaded = pd.read_csv(corrected_csv_io, header=None, names=cols, sep=sep, dtype=str)

            
            sep_placeholder = '<<<|SEP_PLACEHOLDER|>>>'
            backslash_placeholder = '<<<|BACKSLASH_PLACEHOLDER|>>>'
            
            # --- Step 2: Reverse the replacements ---
            for col in df_loaded.select_dtypes(include=['object', 'string']).columns:
                df_loaded[col] = df_loaded[col].apply(
                    lambda x: x.replace(sep_placeholder, sep).replace(backslash_placeholder, '\\') if pd.notnull(x) else x
                )

            file_name = os.path.basename(zip_file_path)
            
            df_loaded.insert(0, 'file_name', file_name)
            df_loaded.insert(0, 'upload_date', import_timestamp.strftime('%Y-%m-%d %H:%M:%S'))
            
            engine = create_engine(
                'postgresql+psycopg2://',
                creator=lambda: connection
            )
            
            df_loaded = df_loaded.replace('None', np.nan)
            
            new_cols = (df_loaded.columns.to_list())
            print('df_loaded cols: ', new_cols)
            try:
                df_loaded.to_sql(
                    name=table_name_stage,
                    schema='pipeline',
                    con=engine,
                    index=False,
                    if_exists='append'
                )
            except Exception as e:
               raise RuntimeError(f"❌ Failed to save to Stage SQL: {e}")

            
            with connection.cursor() as cursor:
                cursor.execute("SET search_path TO pipeline")

                # Apply value replacements if provided
                if replace_values_config:
                    for _replace in replace_values_config:
                        for column, transf in _replace.items():
                            old_value, new_value = transf[1], transf[2]
                            if "int" in transf[0].lower():
                                cursor.execute(f"UPDATE {table_name_stage} SET {column} = {new_value} WHERE {column} = %s", (old_value,))
                            elif new_value.lower() == 'null':
                                cursor.execute(f"UPDATE {table_name_stage} SET {column} = NULL WHERE {column} = %s", (old_value,))
                            elif old_value.lower() == 'null':
                                cursor.execute(f"UPDATE {table_name_stage} SET {column} = %s WHERE {column} IS NULL", (new_value,))
                            else:
                                cursor.execute(f"UPDATE {table_name_stage} SET {column} = %s WHERE {column} = %s", (new_value, old_value))

                # **NEW: Generate a SELECT statement with type conversions**
                schema_df = pd.read_csv(schema_file, sep=sep)  # Load schema from file
                column_mappings = []
                transformation_map = {clean_column_name(k): v.upper() for d in transformations for k, v in d.items()}

                
                print(transformation_map)
                
                for _, row in schema_df.iterrows():
                    col_name = clean_column_name(row["Column Name"])

                    # Use transformation if present
                    if col_name in transformation_map:
                        data_type = transformation_map[col_name]
                        if any(t in data_type for t in ["INT", "BIGINT", "SMALLINT"]):
                            column_mappings.append(
                                f"CAST(NULLIF(TRIM({col_name}::TEXT), '')::NUMERIC AS {data_type}) AS {col_name}"
                            )
                        else:
                            column_mappings.append(f"{col_name}::{data_type} AS {col_name}")
                    else:
                        data_type = map_data_types(row["Data Type"]).upper()

                        if any(t in data_type for t in ["INT", "BIGINT", "SMALLINT"]):
                            # Stage table holds TEXT; values like "1234.0" can't cast directly to integer.
                            # Cast via NUMERIC first to strip the decimal part, then to the target int type.
                            column_mappings.append(
                                f"CAST(NULLIF(TRIM({col_name}::TEXT), '')::NUMERIC AS {data_type}) AS {col_name}"
                            )
                        elif any(t in data_type for t in ["DECIMAL", "NUMERIC", "FLOAT", "REAL"]):
                            column_mappings.append(f"CAST(NULLIF({col_name}, '') AS {data_type}) AS {col_name}")
                        elif any(t in data_type for t in ["DATE", "TIMESTAMP"]):
                            column_mappings.append(f"CAST(NULLIF({col_name}, '') AS {data_type}) AS {col_name}")
                        else:
                            column_mappings.append(f"{col_name}::TEXT AS {col_name}")

                column_query = ",\n    ".join(column_mappings)
                
                print('column_query: ', column_query)

                insert_query = f"""
                    INSERT INTO {schema}.{table_name} (
                         
                        {', '.join(new_cols)} 
                    ) 
                    SELECT 
                        upload_date, file_name, 
                        {column_query} 
                    FROM pipeline.{table_name_stage};
                """
            
                print(insert_query)  # Debugging

                cursor.execute(insert_query)
                inserted_rows = cursor.rowcount  # Get number of inserted rows
                cursor.execute(f"TRUNCATE TABLE pipeline.{table_name_stage}")

            connection.commit()
    
    return import_timestamp, inserted_rows  # Return values

def get_column_names_from_zipped_csv(zip_file_path, separator):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        csv_file_name = zip_ref.namelist()[0]  # Assuming the first file in the zip is the CSV file
        with zip_ref.open(csv_file_name) as csv_file:
            csv_content = io.TextIOWrapper(csv_file, encoding='utf-8')
            reader = csv.reader(csv_content, delimiter=separator)
            column_names = tuple(next(reader))
            
            
    return tuple(clean_column_name(value) for value in column_names)


def test_postgres_connection(conn_string):
        
        conn = psycopg2.connect(conn_string)
        conn.close()
        
        return True

def check_file_exists(file_path):

    if os.path.isfile(file_path):
        return True
    else:
        return False
   
        
# def check_table_exists(schema, table_name, conn_string):
#     try:
#         conn = psycopg2.connect(conn_string)
#         cursor = conn.cursor()

#         query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' and table_schema='{schema}')"
#         cursor.execute(query)
#         exists = cursor.fetchone()[0]

#         if exists:
#             print(f"Table '{table_name}' exists.")
#             cursor.close()
#             conn.close()
#             return True
#         else:
#             print(f"Table '{table_name}' does not exist.")
#             cursor.close()
#             conn.close()
#             return False
    
    # except psycopg2.Error as e:
    #     print(f"Error connecting to PostgreSQL: {e}")
    #     cursor.close()
    #     conn.close()
    #     return False

def check_table_exists(schema, table_name, conn_string):
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    print(schema, table_name)

    query = f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        );
    """
    cursor.execute(query, (schema, table_name))
    exists = cursor.fetchone()[0]

    cursor.close()
    conn.close()
    return exists     

def count_records_in_table(file_name, schema, table_name, conn_string):
    
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    query = f"SELECT COUNT(*) FROM {schema}.{table_name} where file_name = '{file_name}'"
    cursor.execute(query)
    actual_count = cursor.fetchone()[0]

    cursor.close()
    conn.close()
    
    return actual_count
            
def read_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data



def count_csv_rows(zip_file_path):
    
    with zipfile.ZipFile(zip_file_path, 'r') as zipf:
        
        for file_info in zipf.infolist():
            if file_info.filename.endswith('.csv'):
                
                csv_data = zipf.read(file_info.filename).decode('utf-8')

                csv_file = io.StringIO(csv_data)
                
                next(csv_file) 
                
                row_count = sum(1 for _ in csv_file)
            
    return row_count

def get_ingested_files(connection, table_name, schema, file):

    conn = connection

    cur = conn.cursor()
    cur.execute(f"SELECT distinct file_name FROM {schema}.{table_name} where file_name = '{file}'")  
    rows = cur.fetchall()
    
    if rows:
        
        print('File was found')
        return True

    return  False


def create_folder_if_not_exists(folder_path):

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder '{folder_path}' created.")
    else:
        print(f"Folder '{folder_path}' already exists.")
        
        
def clean_column_name(column_name):
    # Replace spaces with underscores
    column_name = column_name.replace(' ', '_')
    # Remove special characters except underscores and alphanumeric characters
    column_name = re.sub(r'[^\w\s]', '', column_name)
    # Check if column name is a reserved word
    reserved_words = {'start', 'begin', 'if', 'exists', 'end', 'having', 'user', 'group', 'select', 'insert', 'update', 'delete', 'order', 'where', 'from', 'group'}
    if column_name.lower() in reserved_words:
        column_name += '_'
    return column_name.lower()


def get_filename(path):
    return os.path.basename(path)


def delete_files(folder, delete_all):
    
    if os.path.isfile(folder):
        os.remove(folder)
        
    elif os.path.isdir(folder):
        # List all files in the folder
        files = os.listdir(folder)
        
        regex_pattern = r'\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}'
        
        # Compile the regex pattern
        pattern = re.compile(regex_pattern)
        
        # Iterate over each file in the folder
        for filename in files:
            
            if delete_all:
                if filename.endswith('.csv') or filename.endswith('.schema') or filename.endswith('.zip'):
                    # Build the full path to the file
                    file_path = os.path.join(folder, filename)
                    # Attempt to remove the file
                    try:
                        os.remove(file_path)
                        print(f"File '{filename}' deleted successfully.")
                    except Exception as e:
                        print(f"Error: {e.strerror} - {filename}")

            else:
            # Check if the filename matches the regex pattern
                if pattern.search(filename) and (filename.endswith('.csv') or filename.endswith('.schema')):
                    # Build the full path to the file
                    file_path = os.path.join(folder, filename)
                    # Attempt to remove the file
                    try:
                        os.remove(file_path)
                        print(f"File '{filename}' deleted successfully.")
                    except Exception as e:
                        print(f"Error: {e.strerror} - {filename}")


    def test(test_string):
        new_test = test_string + "my_test"
        print(test_string)
        return new_test
        