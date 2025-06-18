from airflow.decorators import dag, task
from datetime import datetime
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, upper, regexp_replace
from pyspark.sql.types import StringType

@dag(
    dag_id="bronze_to_silver",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["adls", "pyspark", "elt", "azure", "silver", "data-quality"],
)
def bronze_to_silver_dag():

    @task()
    def process_bronze_to_silver():
        # Carrega variáveis do .env
        dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
        load_dotenv(dotenv_path)

        account_name = os.getenv("ADLS_ACCOUNT_NAME")
        landing_container = os.getenv("ADLS_FILE_SYSTEM_NAME")
        bronze_container = os.getenv("ADLS_BRONZE_CONTAINER")
        silver_container = os.getenv("ADLS_SILVER_CONTAINER")
        client_id = os.getenv("ADLS_SP_CLIENT_ID")
        client_secret = os.getenv("ADLS_SP_CLIENT_SECRET")
        tenant_id = os.getenv("ADLS_SP_TENANT_ID")

        if not all([account_name, landing_container, bronze_container, client_id, client_secret, tenant_id]):
            raise ValueError("Variáveis de ambiente não carregadas corretamente. Verifique o .env.")

        # Criação da SparkSession com autenticação via OAuth 2.0
        spark = (
            SparkSession.builder
            .appName("LandingToBronze")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-azure:3.3.4")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.azurebfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")
            .config(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "OAuth")
            .config(f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            .config(f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net", client_id)
            .config(f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net", client_secret)
            .config(f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
            .getOrCreate()
        )

        # Reflete as configurações no Hadoop Configuration também (essencial para `FileSystem.get()`)
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "OAuth")
        hadoop_conf.set(f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        hadoop_conf.set(f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net", client_id)
        hadoop_conf.set(f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net", client_secret)
        hadoop_conf.set(f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

        # Define os caminhos ABFSS
        subdir = os.getenv("ADLS_DIRECTORY_NAME", "")
        bronze_path = f"abfss://{bronze_container}@{account_name}.dfs.core.windows.net/{subdir}/"
        silver_path = f"abfss://{silver_container}@{account_name}.dfs.core.windows.net/{subdir}/"

        try:
            # List all Delta tables in bronze layer
            bronze_tables = []
            try:
                bronze_files = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark.sparkContext._jsc.hadoopConfiguration()
                ).listStatus(
                    spark.sparkContext._jvm.org.apache.hadoop.fs.Path(bronze_path)
                )
                
                for file in bronze_files:
                    if file.isDirectory():
                        table_name = file.getPath().getName()
                        bronze_tables.append(table_name)
            except Exception as e:
                print(f"Atenção: Não foi possível listar as tabelas bronze: {e}")
                # If listing fails, try to process common table names
                bronze_tables = ["apolice", "cliente", "carro", "estado", "estudante", "municipio", "paciente", "regiao", "sinistro"]  # Add your actual table names

            print(f"Tabelas bronze encontradas: {bronze_tables}")

            for table_name in bronze_tables:
                print(f"Processando tabela: {table_name}")
                
                # Read the Delta table from bronze layer
                bronze_table_path = f"{bronze_path}{table_name}"
                df = spark.read.format("delta").load(bronze_table_path)
                
                print(f"Esquema original para {table_name}:")
                df.printSchema()
                print(f"Contagem de linhas: {df.count()}")
                
                # 1. Remove duplicates
                df = df.dropDuplicates()
                print(f"Após remoção de duplicatas: {df.count()}")
                
                # 2. Remove rows with null values
                df = df.na.drop()
                print(f"Após remoção de valores nulos: {df.count()}")
                
                # 3. Convert all string columns to uppercase
                for column in df.columns:
                    if df.schema[column].dataType == StringType():
                        df = df.withColumn(column, upper(col(column)))
                
                # 4. Rename columns according to the mapping
                column_mapping = {
                    'CD': 'CODIGO',
                    'TP': 'TIPO', 
                    'DT': 'DATA',
                    'NM': 'NOME',
                    'DS': 'DESCRICAO'
                }
                
                # Apply column renaming
                for old_name, new_name in column_mapping.items():
                    if old_name in df.columns:
                        df = df.withColumnRenamed(old_name, new_name)
                        print(f"Coluna renomeada: {old_name} para {new_name}")
                
                # Add metadata columns
                df = df.withColumn("_silver_ingestion_timestamp", current_timestamp())
                df = df.withColumn("_source_table", col("_ingestion_timestamp"))  # Keep original ingestion timestamp
                
                print(f"Final schema for {table_name}:")
                df.printSchema()
                print(f"Contagem de linhas: {df.count()}")
                
                # Write to silver layer in Delta format
                silver_table_path = f"{silver_path}{table_name}"
                df.write.format("delta").mode("overwrite").save(silver_table_path)
                
                print(f"Tabela delta - {table_name} - processada com sucesso para camada silver")

        except Exception as e:
            print(f"Erro ao processar dados: {str(e)}")
            raise e
        finally:
            spark.stop()

    process_bronze_to_silver()

dag = bronze_to_silver_dag() 