from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# 1. Configuração da Sessão Spark
spark = SparkSession.builder \
    .appName("AgrotechTelemetryProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definição do Schema (Atualizado para o Firmware Profissional)
# O campo 'dadosAdicionais' agora é um Objeto (Struct) e não mais string solta.
dados_adicionais_schema = StructType([
    StructField("ip", StringType(), True),
    StructField("rssi", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("cliente", StringType(), True),
    StructField("local", StringType(), True)
])

# Schema Principal
schema = StructType([
    StructField("temperatura", FloatType(), True),
    StructField("umidade", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    # Aqui definimos a estrutura aninhada
    StructField("dadosAdicionais", dados_adicionais_schema, True)
])

# 3. Leitura do Kafka (Stream)
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "agro-cluster-kafka-bootstrap:9092") \
    .option("subscribe", "telemetria-raw") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Transformação (ETL)
# Parse do JSON que vem do Firmware
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data"))

# Seleção e Preparação para o Banco
# ATENÇÃO: O PostgreSQL espera que a coluna 'dados_adicionais' (jsonb) receba uma STRING.
# Por isso usamos to_json(col("data.dadosAdicionais"))
df_final = df_parsed.select(
    col("data.temperatura"),
    col("data.umidade"),
    col("data.latitude"),
    col("data.longitude"),
    to_json(col("data.dadosAdicionais")).alias("dados_adicionais"), # Converte Struct -> JSON String
    current_timestamp().alias("data_hora") # O Firmware não manda hora, geramos aqui
)

# Filtra apenas dados válidos (garantia simples)
df_final = df_final.filter(col("temperatura").isNotNull())

# 5. Escrita no PostgreSQL (Via JDBC)
def write_to_postgres(batch_df, batch_id):
    print(f"Processando Batch ID: {batch_id}")
    
    # Atualizei o nome do banco e tabela baseado no seu Entity JPA
    jdbc_url = "jdbc:postgresql://postgis-svc:5432/agrotech" 
    
    properties = {
        "user": "agro_admin",
        "password": "%%%%%3Filhos32023@@",
        "driver": "org.postgresql.Driver",
        # stringtype=unspecified ajuda o driver a converter String para JSONB automaticamente
        "stringtype": "unspecified" 
    }

    try:
        # Tabela atualizada para 'telemetria' (conforme sua entidade Java)
        batch_df.write \
            .jdbc(url=jdbc_url, table="telemetria", mode="append", properties=properties)
        print(f"Batch {batch_id} salvo com sucesso! Registros: {batch_df.count()}")
    except Exception as e:
        print(f"Erro ao salvar no Postgres: {str(e)}")

# Inicia o Stream
query = df_final.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()