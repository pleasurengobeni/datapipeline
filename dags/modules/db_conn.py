from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, MetaData
import os
import re

from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mssql_hook import MsSqlHook

import sys
from airflow.models import Variable

modules_path = Variable.get('modules_path', default_var=os.path.join(os.getcwd(), 'dags', 'modules'))
if modules_path not in sys.path:
    sys.path.insert(1, modules_path)
from modules.utils import *


def test_db_connection(db_type, conn_id):
    
    hook = None

    if db_type == "mysql":
        hook = MySqlHook(mysql_conn_id=conn_id)
    elif db_type == "postgresql":
        hook = PostgresHook(postgres_conn_id=conn_id)
    elif db_type == "mssql":
        hook = MsSqlHook(mssql_conn_id=conn_id)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    hook.get_conn()

def connect_to_db(conn_uri):
    
    engine = create_engine(conn_uri)
    db_type = engine.dialect.name
    
    # Create database connection string based on the database type
    if db_type == "mysql":
        engine = create_engine(f"mysql://{username}:{password}@{host}:{port}/{db_name}")
    elif db_type == "postgres":
        engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{db_name}")
    elif db_type == "mssql":
        engine = create_engine(f"mssql+pyodbc://{username}:{password}@{host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server")
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    return engine

def query_database(engine, db_type, schema, table_name, incremental_column, incremental_column_datatype, last_loaded_max_value, limit=None, sep=';'):
    
    # Query database table with incremental load
    if incremental_column_datatype == 'int':  # Assuming integers for incremental load

        if db_type == "mysql":
            query = f"SELECT * FROM {schema}.{table_name} WHERE cast({incremental_column} as signed) > {last_loaded_max_value} order by cast({incremental_column} as signed) limit {limit}"

        elif db_type == "postgresql":
            query = f"SELECT * FROM {schema}.{table_name} WHERE cast({incremental_column} as int) > {last_loaded_max_value} order by cast({incremental_column} as int) limit {limit}"

        elif db_type == "mssql":
            query = f"SELECT top {limit} * FROM {schema}.{table_name} WHERE cast({incremental_column} as int) > {last_loaded_max_value} order by cast({incremental_column} as int)"

        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
    elif incremental_column_datatype in ('date','datetime', 'timestamp'):  # Assuming datetime for incremental load
        if db_type == "mysql":

            query = f"""

                SELECT * FROM (
                    SELECT * FROM {schema}.{table_name}
                    WHERE cast({incremental_column} as datetime) > '{last_loaded_max_value}'
                    ORDER BY cast({incremental_column} as datetime)
                    LIMIT {limit}
                    LOCK IN SHARE MODE
                    
                ) AS data
                UNION
                SELECT * FROM {schema}.{table_name}
                WHERE cast({incremental_column} as datetime) = (
                    SELECT MAX(cast({incremental_column} as datetime)) FROM (
                        SELECT * FROM {schema}.{table_name}
                        WHERE cast({incremental_column} as datetime) > '{last_loaded_max_value}'
                        ORDER BY cast({incremental_column} as datetime)
                        LIMIT {limit}
                        LOCK IN SHARE MODE
                        
                    ) AS data
                )

            """

        elif db_type == "postgresql":
            query = f"""

                WITH data AS (
                    SELECT * FROM {schema}.{table_name}
                    WHERE cast({incremental_column} as datetime) > '{last_loaded_max_value}'
                    ORDER BY cast({incremental_column} as datetime)
                    LIMIT {limit}
                )
                SELECT * FROM data
                UNION
                SELECT * FROM {schema}.{table_name}
                WHERE cast({incremental_column} as datetime) = (
                    SELECT MAX(cast({incremental_column} as datetime)) FROM data
                )

            """

        elif db_type == "mssql":
            query = f"""
                WITH data AS (
                    SELECT TOP {limit} * 
                    FROM {schema}.{table_name} WITH (NOLOCK)
                    WHERE cast({incremental_column} as datetime) > '{last_loaded_max_value}' 
                    order by cast({incremental_column} as datetime)
                )
                SELECT * 
                FROM data
                
                UNION
                
                SELECT * 
                FROM {schema}.{table_name} 
                WHERE cast({incremental_column} as datetime) = (SELECT MAX(cast({incremental_column} as datetime)) FROM data)
            """

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

            
    else:
        raise ValueError(f"Unsupported data type for incremental column: {incremental_column_datatype}")

    df = pd.read_sql(query, engine)

    
    # Get only string or object columns
    string_cols = df.select_dtypes(include=['object', 'string']).columns

    # Replace \n with space (or empty string '') in those columns
    
    for col in df.select_dtypes(include=['object', 'string']).columns:
        try:
            df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False)
        except Exception as e:
            print(f"Error cleaning column '{col}': {e}")


    # Replace sep only in object (string) columns
    sep_placeholder = '<<<|SEP_PLACEHOLDER|>>>'
    backslash_placeholder = '<<<|BACKSLASH_PLACEHOLDER|>>>'
    
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].apply(
            lambda x: x.replace(sep, sep_placeholder).replace('\\', backslash_placeholder)
            if isinstance(x, str) else x
        )

    return df


def get_last_max_incremental_value(engine, schema, table_name, incremental_column, datatype):
        try:
            # Get a connection from the engine
            conn = engine.connect()
            
            # Get a cursor from the connection
            cursor = conn.connection.cursor()
            
            # Check if the table exists
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);", (schema, table_name))
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                if datatype == 'int':
                    return -1
                elif datatype == 'datetime' or datatype == 'date' or datatype == 'timestamp':
                    return '1900-01-01'

            # Table exists, calculate max value of the incremental column
            cursor.execute(f"SELECT MAX({incremental_column}) FROM {schema}.{table_name};")
            max_value = cursor.fetchone()[0]

            if max_value is None:
                if datatype == 'int':
                    return -1
                elif datatype == 'datetime' or datatype == 'date' or datatype == 'timestamp':
                    return '1900-01-01'

            return max_value
        
        except Exception as e:
            
            print(f"Error: {e}")
            
            if datatype == 'int':
                return -1
            elif datatype == 'datetime' or datatype == 'date' or datatype == 'timestamp':
                return '1900-01-01'
                     
        finally:
            
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()


def save_to_csv(df, folder, filename, schema_filename, sep, transformation_config):
    # Generate timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    # Create subfolder if it doesn't exist
    subfolder = os.path.join(folder, filename)
    create_folder_if_not_exists(subfolder)
    
    # Correct data before saving to CSV
    df.columns = [clean_column_name(col) for col in df.columns]
    
    for transform in transformation_config:
        for column, dtype in transform.items():
            colname = clean_column_name(column)
            
            if colname in df.columns and "int" in dtype.lower():
                print(f"🛠 Processing column: {colname}")
                
                df = df.replace("None", np.nan)
                df = df.replace("nan", np.nan)
                df = df.replace("NaN", np.nan)
                df = df.replace("", np.nan)
                df = df.replace(" ", np.nan)
                df = df.replace("null", np.nan)
                df = df.replace("NULL", np.nan)
                

                # Try conversion — use pd.to_numeric first so float strings like
                # "2011.0" are handled before casting to Int64 (avoids COPY failures)
                try:
                    df[colname] = pd.to_numeric(df[colname], errors='coerce').round(0).astype("Int64")
                    print(f"✅ Converted {colname} to Int64")
                except Exception as e:
                    print(f"❌ Failed to convert {colname}: {e}")
                    print("⚠️ Sample values:")
                    print(df[colname].dropna().unique())


                
                
        for column in df.columns:
            if df[column].dtype == 'object':  # Check if column is of string type
                
                if pd.api.types.is_datetime64_any_dtype(df[column]):
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                    df[column].fillna(pd.to_datetime('1900-01-01'), inplace=True)
                
            else:
                try:
                    df[column] = df[column].str.replace('\n', ' ')
                    df[column] = df[column].str.replace('"', '')
                    df[column] = df[column].str.strip()
                    df[column] = df[column].str.replace(sep, ' ')
                except Exception as e:
                    print(f"Error processing column {column}: {e}")
                    #send_slack_message(str(e))

    #df = df.astype({col: 'int64' for col in df.select_dtypes(include='Int64').columns})

    # Save DataFrame to CSV file with UTF-8 encoding and timestamp in the filename
    df.to_csv(f"{subfolder}/{filename}_{timestamp}.csv", sep=sep, index=False, na_rep='')
    
    # Save column names and data types to a separate file with timestamp in the filename
    schema_df = pd.DataFrame(df.dtypes.reset_index())
    schema_df.columns = ['Column Name', 'Data Type']
    schema_df.to_csv(f"{subfolder}/{schema_filename}_{timestamp}.schema", sep=sep, index=False, encoding='utf-8')
    # print(schema_df)
    
    return f"{subfolder}/{filename}_{timestamp}.csv", f"{subfolder}/{schema_filename}_{timestamp}.schema"
    
    
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

def generate_sql_alter_queries(schema_file, sep, table_schema, table_name, transformations, engine, db_source):
    
    alter_queries = []
    
    sql_query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name = '{table_name}';"
    df = pd.read_sql(sql_query, engine)
    
    if db_source:
        
        schema_df = pd.read_csv(schema_file, sep=sep, skipinitialspace=True)
        schema_df['Column Name'] = schema_df['Column Name'].apply(clean_column_name)

        for table_index, table_column in df.iterrows():
            for schema_index, schema_column in schema_df.iterrows():
                table_column['column_name'] = schema_column['Column Name']
                data_type = map_data_types(str(schema_column['Data Type']))
                
                alter_queries.append(f"ALTER TABLE {table_schema}.{table_name} ALTER COLUMN {table_column['column_name']} TYPE {data_type} USING {table_column['column_name']}::{data_type};")
                # alter_queries.append(f"ALTER TABLE pipeline.{table_schema}_{table_name}_stage ALTER COLUMN {table_column['column_name']} TYPE {data_type} USING {table_column['column_name']}::{data_type};")
            
    for transformation in transformations:
        for column, dtype in transformation.items():
            for schema_index, schema_column in schema_df.iterrows():
                if clean_column_name(schema_column['Column Name']) == clean_column_name(column):
                    
                    _dtype = 'timestamp' if 'date' in dtype.lower() else map_data_types(dtype.lower())

                    
                    alter_queries.append(f"ALTER TABLE {table_schema}.{table_name} ALTER COLUMN {clean_column_name(schema_column['Column Name']).upper()} TYPE {_dtype.upper()} USING {clean_column_name(schema_column['Column Name']).upper()}::{_dtype};")
                    # alter_queries.append(f"ALTER TABLE pipeline.{table_schema}_{table_name}_stage ALTER COLUMN {schema_column['Column Name']} TYPE {_dtype} USING {schema_column['Column Name']}::{_dtype};")
  
    # Execute the generated queries
    with engine.connect() as conn:
        for query in alter_queries:
            conn.execute(query)