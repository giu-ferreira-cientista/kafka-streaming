#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import libraries
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType


# Spark session & context
spark = (SparkSession
         .builder
         .master('local')
         .appName('json-producer')
         # Add kafka package
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())
sc = spark.sparkContext


# In[2]:


mySchema = StructType([
 StructField("id", IntegerType()),
 StructField("nome", StringType()),
 StructField("idade", IntegerType()),
 StructField("sexo", IntegerType()),
 StructField("peso", DoubleType()),
 StructField("altura", IntegerType()),
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
 StructField("fumante", IntegerType()),
 StructField("genetica", IntegerType()),
 StructField("gestante", IntegerType()),
 StructField("frutas", IntegerType()),
 StructField("vegetais", IntegerType()),
 StructField("alcool", IntegerType()),
 StructField("doenca_coracao", IntegerType()),     
 StructField("avc", IntegerType()),
 StructField("colesterol_alto", IntegerType()), 
 StructField("exercicio", IntegerType()), 
 StructField("timestampstr", StringType()),
 StructField("timestamp_epoch", StringType())    
])


# In[3]:


#import time

#import os

#timestr = time.strftime("%Y%m%d-%H%M%S")

#json_name = "patient-data-" + timestr + '.json' 

#os.system('curl "https://api.mockaroo.com/api/e172bfb0?count=10&key=42e8f800" > ' + json_name)

#os.system('mv ' + json_name + ' ../json')


# In[7]:


import requests
import os
import time

timestr = time.strftime("%Y%m%d-%H%M%S")

json_name = "patient-data-" + timestr + '.json' 

with open('/home/jovyan/work/json/sample.json', 'rb') as file:
    files = {'f': ('sample.json', file)}
    response = requests.post("https://api.mockaroo.com/api/e172bfb0?count=10&key=42e8f800",files=files)  #https://638d3c7e4190defdb74041ac.mockapi.io/patients

response.raise_for_status() # ensure we notice bad responses

with open(json_name, "wb") as file:
    file.write(response.content)
    
os.system('mv ' + json_name + ' /home/jovyan/work/json')


# In[5]:



json_path = "/home/jovyan/work/json"
json_topic = "patient-data"
kafka_server = "kafka:29092"

streamingDataFrame = spark.readStream.schema(mySchema).json(json_path)


# In[6]:



streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")   .writeStream   .format("kafka")   .option("topic", json_topic)   .option("kafka.bootstrap.servers", kafka_server)   .option("checkpointLocation", json_path)   .start()


# In[ ]:





# In[ ]:





# In[ ]:




