import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def test_adls_connection():
    # Load environment variables from the .env file
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path)

    account_name = os.getenv("ADLS_ACCOUNT_NAME")
    landing_container = os.getenv("ADLS_FILE_SYSTEM_NAME")
    bronze_container = os.getenv("ADLS_BRONZE_CONTAINER") # Not strictly needed for connection test, but good to keep consistent

    # Service Principal Credentials
    client_id = os.getenv("ADLS_SP_CLIENT_ID")
    client_secret = os.getenv("ADLS_SP_CLIENT_SECRET")
    tenant_id = os.getenv("ADLS_SP_TENANT_ID")

    # Basic validation
    if not all([account_name, landing_container, client_id, client_secret, tenant_id]):
        print("❌ Error: One or more required environment variables are missing. Please check your .env file.")
        print(f"ADLS_ACCOUNT_NAME: {account_name}")
        print(f"ADLS_FILE_SYSTEM_NAME: {landing_container}")
        print(f"ADLS_SP_CLIENT_ID: {client_id}")
        print(f"ADLS_SP_CLIENT_SECRET: {'*' * len(client_secret) if client_secret else None}") # Mask secret for printing
        print(f"ADLS_SP_TENANT_ID: {tenant_id}")
        return

    spark = None # Initialize spark to None for finally block
    try:
        # Build Spark Session with ADLS Gen2 (ABFS) configurations
        print("Attempting to create SparkSession and connect to ADLS Gen2...")
        spark = (
            SparkSession.builder
            .appName("ADLSGen2ConnectionTest")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-azure:3.3.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

            # OAuth 2.0 (Service Principal) configuration for ABFS
            .config(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "OAuth")
            .config(f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            .config(f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net", client_id)
            .config(f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net", client_secret)
            .config(f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

            # Driver for abfss://
            .config("spark.hadoop.fs.azurebfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem")
            # Set log level to INFO for more detailed Spark output
            .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/path/to/log4j.properties") # Optional: point to a custom log4j.properties for more detailed logging
            .getOrCreate()
        )
        print("✅ SparkSession created successfully.")
        print(f"Spark Version: {spark.version}")

        # Try to list contents of the landing container
        landing_path = f"abfss://{landing_container}@{account_name}.dfs.core.windows.net/"
        print(f"Attempting to list contents of: {landing_path}")

        # This will trigger the actual connection and authentication with ADLS Gen2
        files = spark.read.text(f"{landing_path}*").limit(1).collect()
        # Alternatively, if you just want to test listing:
        # fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(landing_path), spark._jsc.hadoopConfiguration())
        # statuses = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(landing_path))
        # print(f"Found {len(statuses)} items in the landing container.")
        # for status in statuses:
        #     print(f"  - {status.getPath().getName()}")

        print(f"✅ Successfully accessed data in {landing_path}. First line of a file (if any): {files[0].value if files else 'N/A'}")
        print("Connection to ADLS Gen2 successful!")

    except Exception as e:
        print(f"❌ An error occurred during ADLS Gen2 connection test: {e}")
        # Print specific details if it's a Py4JJavaError
        if "Py4JJavaError" in str(e):
            print("This is likely a Py4JJavaError, which means a Java exception occurred.")
            print("Check the full stack trace above for details from Hadoop/Azure libraries.")
            print("Common causes: Incorrect Service Principal credentials, insufficient RBAC permissions.")
    finally:
        if spark:
            spark.stop()
            print("SparkSession stopped.")

if __name__ == "__main__":
    test_adls_connection()