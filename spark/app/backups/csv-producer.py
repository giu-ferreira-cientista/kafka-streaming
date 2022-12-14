# import libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType 	


# Spark session & context
spark = (SparkSession
         .builder
         .master('local')
         .appName('csv-producer')
         # Add kafka package
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())
sc = spark.sparkContext

mySchema = StructType([
 StructField("id", IntegerType()),
 StructField("nome", StringType()),
 StructField("idade", IntegerType()),
 StructField("sexo", IntegerType()),
 StructField("peso", DoubleType()),
 StructField("bpm", DoubleType()),
 StructField("pressao", DoubleType()),
 StructField("respiracao", DoubleType()),
 StructField("temperatura", DoubleType()),
 StructField("glicemia", DoubleType()),
 StructField("saturacao_oxigenio", DoubleType()),
 StructField("estado_atividade", IntegerType()),
 StructField("dia_de_semana", IntegerType()),
 StructField("periodo_do_dia", IntegerType()),
 StructField("semana_do_mes", IntegerType()),
 StructField("estacao_do_ano", IntegerType()),
 StructField("passos", IntegerType()),
 StructField("calorias", DoubleType()),
 StructField("distancia", DoubleType()),
 StructField("tempo", DoubleType()),
 StructField("total_sleep_last_24", DoubleType()),
 StructField("deep_sleep_last_24", DoubleType()),
 StructField("light_sleep_last_24", DoubleType()),
 StructField("awake_last_24", DoubleType()),
 StructField("timestampstr", StringType())
 
])

csv_path = "/home/jovyan/work/csv"
csv_topic = "national-health-indicators"
kafka_server = "kafka-server:29092"

streamingDataFrame = spark.readStream.schema(mySchema).csv(csv_path)

streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
  .writeStream \
  .format("kafka") \
  .option("topic", csv_topic) \
  .option("kafka.bootstrap.servers", kafka_server) \
  .option("checkpointLocation", csv_path) \
  .start()
