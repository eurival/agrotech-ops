from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# 1. Configuração da Sessão Spark
spark = SparkSession.builder \
    .appName("AgrotechTelemetryProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definição do Schema (O formato que seu Firmware envia)
# JSON: {"mac": "...", "temp": 20.5, "status": "OPERACIONAL", "cliente": "MercadoX", "local": "Frios"}
schema = StructType([
    StructField("mac", StringType(), True),
    StructField("temp", FloatType(), True),
    StructField("status", StringType(), True),
    StructField("cliente", StringType(), True),
    StructField("local", StringType(), True)
])

# 3. Leitura do Kafka (Stream)
# Lembre-se: O Kafka Connect jogou tudo no 'telemetria-raw'
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "agro-cluster-kafka-bootstrap:9092") \
    .option("subscribe", "telemetria-raw") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transformação (ETL)
# O Kafka entrega 'key' e 'value' em binário. Precisamos converter para String e depois JSON.
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumnRenamed("temp", "temperatura") \
    .withColumnRenamed("cliente", "cliente_id") \
    .withColumnRenamed("local", "local_id") \
    .withColumn("data_processamento", current_timestamp())

# Filtra apenas dados válidos (com temperatura)
df_final = df_parsed.filter(col("temperatura").isNotNull())

# 5. Escrita no PostgreSQL (Via JDBC)
def write_to_postgres(batch_df, batch_id):
    print(f"Processando Batch ID: {batch_id}")
    
    # URL JDBC: jdbc:postgresql://<NOME-DO-SERVICE>:<PORTA>/<NOME-DO-DB>
    # Service: postgis-svc (definido no seu YAML)
    # DB: agrotech_db
    jdbc_url = "jdbc:postgresql://postgis-svc:5432/agrotech_db"
    
    properties = {
        "user": "agro_admin",              # <-- Do seu YAML: POSTGRES_USER
        "password": "%%%%%3Filhos32023@@", # <-- Do seu YAML: POSTGRES_PASSWORD
        "driver": "org.postgresql.Driver"
    }

    try:
        batch_df.write \
            .jdbc(url=jdbc_url, table="telemetria_historico", mode="append", properties=properties)
        print("Batch salvo com sucesso!")
    except Exception as e:
        print(f"Erro ao salvar no Postgres: {str(e)}")

# Inicia o Stream
query = df_final.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()