from airflow.decorators import dag, task
from datetime import datetime

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv

@dag(
    dag_id="sqlserver_to_adls",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mssql", "adls", "elt", "azure"],
)
def sqlserver_to_adls_dag():

    @task()
    def extract_sqlserver_and_upload_azure_adls():

        load_dotenv()

        # Azure Data Lake
        account_name = os.getenv("ADLS_ACCOUNT_NAME")
        file_system_name = os.getenv("ADLS_FILE_SYSTEM_NAME")
        directory_name = os.getenv("ADLS_DIRECTORY_NAME")
        sas_token = os.getenv("ADLS_SAS_TOKEN")

        # SQL Server
        server = os.getenv("SQL_SERVER")
        database = os.getenv("SQL_DATABASE")
        schema = os.getenv("SQL_SCHEMA")
        username = os.getenv("SQL_USERNAME")
        password = quote_plus(os.getenv("SQL_PASSWORD"))

        conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str)

        query_tables = f"SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{schema}'"

        # Cliente ADLS
        file_system_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=sas_token,
            api_version="2020-02-10",
        )
        directory_client = file_system_client.get_file_system_client(file_system_name).get_directory_client(directory_name)

        try:
            directory_client.create_directory()
        except ResourceExistsError:
            print(f"O diretório '{directory_name}' já existe.")

        df_tables = pd.read_sql(query_tables, engine)

        for _, row in df_tables.iterrows():
            table_name = row["table_name"]
            df = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", conn_str)
            file_client = directory_client.get_file_client(f"{table_name}.csv")
            file_client.upload_data(df.to_csv(index=False).encode(), overwrite=True)
            print(f"Dados da tabela '{table_name}' enviados com sucesso.")

    extract_sqlserver_and_upload_azure_adls()

dag = sqlserver_to_adls_dag()
