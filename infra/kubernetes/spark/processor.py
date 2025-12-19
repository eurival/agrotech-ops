from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# 1. Configuração da Sessão
spark = SparkSession.builder \
    .appName("AgrotechTelemetryProcessor") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- CONFIGURAÇÃO DO BANCO ---
db_url = "jdbc:postgresql://postgis-svc:5432/agrotech_db"
db_props = {
    "user": "agro_admin",
    "password": "%%%%%3Filhos32023@@",
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

# 2. CARREGAR TABELA DE DISPOSITIVOS (LOOKUP)
df_dispositivos = spark.read.jdbc(url=db_url, table="dispositivo", properties=db_props) \
    .select(col("identificador_mac").alias("mac_lookup"), col("id").alias("device_id_found"))

df_dispositivos.cache()

# 3. Schemas (ATUALIZADO PARA MODELO EMPRESA -> UNIDADE)
dados_adicionais_schema = StructType([
    StructField("ip", StringType(), True),
    StructField("rssi", IntegerType(), True),
    StructField("status", StringType(), True),
    # --- MUDANÇA AQUI ---
    # Antes: cliente, local
    # Agora: empresa, unidade
    StructField("empresa", StringType(), True), 
    StructField("unidade", StringType(), True)
    # --------------------
])

schema = StructType([
    StructField("mac", StringType(), True), 
    StructField("temperatura", FloatType(), True),
    StructField("umidade", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("dadosAdicionais", dados_adicionais_schema, True)
])

# 4. Leitura Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "agro-cluster-kafka-bootstrap:9092") \
    .option("subscribe", "telemetria-raw") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Transformação
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") 

# 6. ENRIQUECIMENTO
df_enriched = df_parsed.join(df_dispositivos, df_parsed.mac == df_dispositivos.mac_lookup, "left")

# 7. Preparação Final
df_final = df_enriched.select(
    col("temperatura"),
    col("umidade"),
    col("latitude"),
    col("longitude"),
    col("device_id_found").alias("dispositivo_id"), 
    current_timestamp().alias("data_hora"),
    
    # Grava o JSONB no banco
    to_json(struct(
        col("mac"), 
        col("dadosAdicionais.*") # O '*' pega automaticamente 'empresa' e 'unidade' do schema novo
    )).alias("dados_adicionais")
)

df_final = df_final.filter(col("temperatura").isNotNull())

# 8. Escrita no PostgreSQL
def write_to_postgres(batch_df, batch_id):
    try:
        batch_df.write \
            .jdbc(url=db_url, table="telemetria", mode="append", properties=db_props)
        print(f"Batch {batch_id}: {batch_df.count()} registros processados.")
    except Exception as e:
        print(f"ERRO Batch {batch_id}: {str(e)}")

query = df_final.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()