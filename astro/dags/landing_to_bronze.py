from airflow.decorators import dag, task
from datetime import datetime
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

@dag(
    dag_id="landing_to_bronze",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["adls", "pyspark", "elt", "azure", "bronze"],
)
def landing_to_bronze_dag():

    @task()
    def process_landing_to_bronze():
        # Carrega variáveis do .env
        dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
        load_dotenv(dotenv_path)

        account_name = os.getenv("ADLS_ACCOUNT_NAME")
        landing_container = os.getenv("ADLS_FILE_SYSTEM_NAME")
        bronze_container = os.getenv("ADLS_BRONZE_CONTAINER")
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
        landing_path = f"abfss://{landing_container}@{account_name}.dfs.core.windows.net/{subdir}/"
        bronze_path = f"abfss://{bronze_container}@{account_name}.dfs.core.windows.net/{subdir}/"

        try:
            # Lista arquivos CSV
            landing_uri = spark._jvm.java.net.URI(landing_path)
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(landing_uri, hadoop_conf)
            path = spark._jvm.org.apache.hadoop.fs.Path(landing_uri)

            statuses = fs.listStatus(path)

            table_names = [
                status.getPath().getName().replace(".csv", "")
                for status in statuses
                if status.getPath().getName().endswith(".csv")
            ]

            for table_name in table_names:
                df = spark.read.option("header", "true").csv(f"{landing_path}{table_name}.csv")
                df = df.withColumn("_ingestion_timestamp", current_timestamp())
                df.write.format("delta").mode("overwrite").save(f"{bronze_path}{table_name}")
                print(f"✅ Processado: {table_name}")

        except Exception as e:
            print(f"❌ Erro ao processar arquivos: {str(e)}")
            raise e
        finally:
            spark.stop()

    process_landing_to_bronze()

dag = landing_to_bronze_dag()
