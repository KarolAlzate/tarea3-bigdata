# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD) - Consumidor Spark Structured Streaming (Anexo 3 adaptado)
Lee el topic 'trm_operaciones' y, en tiempo real:
  1. Calcula por ventana de 1 minuto y tipo de operación: número de operaciones,
     precio promedio, mínimo, máximo y monto negociado.
  2. Emite alertas cuando una operación se aleja más de 1 % del precio de referencia.

Ejecución (Spark 3.5.8 usa Scala 2.12):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

PRECIO_REFERENCIA = 3312.84   # TRM vigente usada como base del productor
UMBRAL_ALERTA = 0.01          # 1 %

spark = SparkSession.builder.appName("Tarea3_Streaming_TRM").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

esquema = StructType([
    StructField("operacion_id", IntegerType()),
    StructField("entidad", StringType()),
    StructField("tipo", StringType()),
    StructField("precio", DoubleType()),
    StructField("monto_usd", DoubleType()),
    StructField("timestamp", LongType()),      # segundos epoch enviados por el productor
])

# Lectura del topic de Kafka
crudo = (spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "trm_operaciones")
         .option("startingOffsets", "latest")
         .load())

# Parseo del JSON y conversión del epoch a timestamp
operaciones = (crudo
               .select(F.from_json(F.col("value").cast("string"), esquema).alias("d"))
               .select("d.*")
               .withColumn("hora", F.col("timestamp").cast("timestamp"))
               .withWatermark("hora", "2 minutes"))

# 1. Estadísticas por ventana de 1 minuto y tipo de operación
estadisticas = (operaciones
                .groupBy(F.window("hora", "1 minute").alias("ventana"), "tipo")
                .agg(F.count("*").alias("operaciones"),
                     F.round(F.avg("precio"), 2).alias("precio_prom"),
                     F.min("precio").alias("precio_min"),
                     F.max("precio").alias("precio_max"),
                     F.round(F.sum("monto_usd") / 1e6, 2).alias("monto_millones_usd"))
                .select(F.date_format("ventana.start", "HH:mm:ss").alias("inicio"),
                        F.date_format("ventana.end", "HH:mm:ss").alias("fin"),
                        "tipo", "operaciones", "precio_prom", "precio_min",
                        "precio_max", "monto_millones_usd")
                .orderBy("inicio", "tipo"))

consulta_stats = (estadisticas.writeStream
                  .outputMode("complete")
                  .format("console")
                  .option("truncate", "false")
                  .trigger(processingTime="10 seconds")
                  .queryName("estadisticas_trm")
                  .start())

# 2. Alertas de precio fuera de rango
alertas = (operaciones
           .withColumn("desviacion_pct",
                       F.round((F.col("precio") - PRECIO_REFERENCIA) / PRECIO_REFERENCIA * 100, 2))
           .filter(F.abs(F.col("desviacion_pct")) > UMBRAL_ALERTA * 100)
           .select(F.date_format("hora", "HH:mm:ss").alias("hora"), "operacion_id",
                   "entidad", "tipo", "precio", "desviacion_pct"))

consulta_alertas = (alertas.writeStream
                    .outputMode("append")
                    .format("console")
                    .option("truncate", "false")
                    .trigger(processingTime="10 seconds")
                    .queryName("alertas_trm")
                    .start())

spark.streams.awaitAnyTermination()
