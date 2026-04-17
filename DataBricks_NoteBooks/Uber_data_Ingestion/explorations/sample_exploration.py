# Databricks notebook source
# DBTITLE 1,Cell 1
# from pyspark import pipelines as dp
import pyspark.sql.types as T
from pyspark.sql.functions import *

# Event Hubs configuration
EH_NAMESPACE                    =  "uber-eventhub-pro"
EH_NAME                         =  "ubertopic"

 
# EH_CONN_STR                     =  spark.config.get("connection_string") 
EH_CONN_STR                     =  "Endpoint=sb://uber-eventhub-pro.servicebus.windows.net/;SharedAccessKeyName=ListenPolicy;SharedAccessKey=wNtFBWIktRGoeXmsbdqU69PjuHEL9XNDE+AEhE/3Jqw=;EntityPath=ubertopic"

# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : True,
  "startingOffsets"          : "earliest"

}


df=spark.readStream.format("kafka")\
    .options(**KAFKA_OPTIONS)\
    .load()

display(df,checkpointLocation="/Volumes/uber_project/bronze_layer/uber_volume/Uber_Checkpoint/")

# COMMAND ----------


