import json

# Read configuration from JSON file
with open('config.json') as config_file:
    config = json.load(config_file)

# Extract configuration values
oracle_host = config['oracle']['host']
oracle_port = config['oracle']['port']
oracle_service_name = config['oracle']['service_name']
oracle_username = config['oracle']['username']
oracle_password = config['oracle']['password']

sql_server_host = config['sql_server']['host']
sql_server_database = config['sql_server']['database']
sql_server_username = config['sql_server']['username']
sql_server_password = config['sql_server']['password']

table_name = config['table_name']

# Set up SparkSession
spark = SparkSession.builder \
    .appName("Oracle to SQL Server") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .getOrCreate()

# Connect to Oracle
oracle_conn = cx_Oracle.connect(f"{oracle_username}/{oracle_password}@{oracle_host}:{oracle_port}/{oracle_service_name}")

# Read Oracle table data using Spark
oracle_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:oracle:thin:@//{oracle_host}:{oracle_port}/{oracle_service_name}") \
    .option("dbtable", table_name) \
    .option("user", oracle_username) \
    .option("password", oracle_password) \
    .load()

# Connect to SQL Server
sql_server_conn = pyodbc.connect(
    f"DRIVER={{SQL Server}};"
    f"SERVER={sql_server_host};"
    f"DATABASE={sql_server_database};"
    f"UID={sql_server_username};"
    f"PWD={sql_server_password}"
)

# Write data to SQL Server table using Spark
oracle_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{sql_server_host};databaseName={sql_server_database}") \
    .option("dbtable", table_name) \
    .option("user", sql_server_username) \
    .option("password", sql_server_password) \
    .mode("overwrite") \
    .save()

# Close connections and stop SparkSession
oracle_conn.close()
sql_server_conn.close()
spark.stop()
