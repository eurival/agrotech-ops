from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_json, struct, broadcast, lit, explode
from pyspark.sql.types import MapType, StringType, StructType, FloatType, IntegerType

# 1. Configuração da Sessão
spark = SparkSession.builder \
    .appName("Agrotech_ETL_Production") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- CONEXÃO BANCO ---
db_url = "jdbc:postgresql://postgis-svc:5432/agrotech_db"
db_props = {
    "user": "agro_admin",
    "password": "%%%%%3Filhos32023@@", 
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

print(">>> AGROTECH ETL V3 INICIADO <<<")

# ==============================================================================
# 1. CARREGAR CACHE: MAC -> MAPA DE PORTAS
# ==============================================================================
# Schema do JSON no Banco: { "portas": { "v1": 1, "v2": 2 } }
schema_cfg = StructType().add("portas", MapType(StringType(), IntegerType()))

# Lemos a tabela dispositivo
df_disp_raw = spark.read.jdbc(url=db_url, table="dispositivo", properties=db_props) \
    .select(
        col("identificador_mac").alias("mac_lookup"), 
        col("id").alias("device_id"),
        col("configuracao_adicional").cast("string").alias("json_cfg")
    )

# Extraímos o mapa {"v1": 1, "v2": 2}
df_dispositivos = df_disp_raw.withColumn("mapa_portas", from_json(col("json_cfg"), schema_cfg).getItem("portas"))
# Filtramos apenas dispositivos que têm configuração de portas válida
df_dispositivos = df_dispositivos.filter(col("mapa_portas").isNotNull())
df_dispositivos.cache()

print(f"Dispositivos configurados em cache: {df_dispositivos.count()}")

# ==============================================================================
# 2. CARREGAR IDs VÁLIDOS DE GRANDEZAS (Segurança)
# ==============================================================================
df_grandezas_ids = spark.read.jdbc(url=db_url, table="grandeza", properties=db_props) \
    .select(col("id").alias("grandeza_id_valid"))
df_grandezas_ids.cache()

# ==============================================================================
# 3. LEITURA KAFKA
# ==============================================================================
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "agro-cluster-kafka-bootstrap:9092") \
    .option("subscribe", "telemetria-raw") \
    .option("startingOffsets", "latest") \
    .load()

# ==============================================================================
# 4. PARSE DO JSON (Baseado no seu Log Real)
# ==============================================================================
# Log: {"mac": "...", "leituras": {"v1": 26.13, "v2": 52.00}, "dadosAdicionais": {...}}

schema_in = StructType() \
    .add("mac", StringType()) \
    .add("leituras", MapType(StringType(), FloatType())) \
    .add("dadosAdicionais", StringType()) # Pegamos como string para salvar direto no JSONB

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_in).alias("data")) \
    .select("data.*") \
    .filter(col("mac").isNotNull()) # Proteção contra JSON vazio

# ==============================================================================
# 5. ENRIQUECIMENTO E TRADUÇÃO
# ==============================================================================

# A. Join com Dispositivo (Pega ID e Mapa de Portas)
df_enriched = df_parsed.join(
    broadcast(df_dispositivos),
    df_parsed.mac == df_dispositivos.mac_lookup,
    "inner" # Só processa se o MAC estiver cadastrado no banco
)

# B. Explode (Transforma v1 e v2 em linhas)
df_exploded = df_enriched.select(
    col("device_id"),
    col("dadosAdicionais").alias("dados_adicionais"), # JSON context (ip, rssi, uptime)
    current_timestamp().alias("data_hora"),
    col("mapa_portas"),
    explode(col("leituras")).alias("canal_lido", "valor_lido") 
)

# C. Tradução (v1 -> 1)
df_mapped = df_exploded.withColumn(
    "grandeza_id_target",
    col("mapa_portas").getItem(col("canal_lido")) # Busca o ID inteiro no mapa
)

# D. Filtros de Integridade
# 1. O canal tem mapeamento?
df_mapped = df_mapped.filter(col("grandeza_id_target").isNotNull())

# 2. O ID da grandeza existe na tabela grandeza?
df_final = df_mapped.join(
    broadcast(df_grandezas_ids),
    df_mapped.grandeza_id_target == df_grandezas_ids.grandeza_id_valid,
    "inner"
).select(
    col("data_hora"),
    col("valor_lido").cast("double").alias("valor"),
    col("dados_adicionais"), # O driver Postgres trata String JSON -> JSONB
    col("device_id").alias("dispositivo_id"),
    col("grandeza_id_target").alias("grandeza_id"),
    lit(None).cast("string").alias("valor_texto")
)

# ==============================================================================
# 6. GRAVAÇÃO
# ==============================================================================
def write_to_postgres(batch_df, batch_id):
    # Cache para evitar reprocessamento no count e no write
    batch_df.persist() 
    count = batch_df.count()
    
    if count > 0:
        try:
            print(f"--- Batch {batch_id}: Gravando {count} registros... ---")
            # Debug: Mostrar o que está sendo gravado nas primeiras vezes
            # batch_df.show(5, truncate=False) 
            
            batch_df.write \
                .jdbc(url=db_url, table="telemetria", mode="append", properties=db_props)
            print(">>> Sucesso Banco!")
        except Exception as e:
            print(f"!!! ERRO Batch {batch_id}: {str(e)}")
    else:
        print(f".", end="", flush=True) # Heartbeat no log
        
    batch_df.unpersist()

query = df_final.writeStream \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()