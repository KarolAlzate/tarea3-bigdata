# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD 202016911) - Procesamiento en batch con Apache Spark
Problema: comportamiento y volatilidad de la Tasa Representativa del Mercado (TRM)
          del dólar en Colombia, 1992-2026.
Fuente:   Superintendencia Financiera de Colombia - Datos Abiertos Colombia
          https://www.datos.gov.co/api/views/mcec-87by/rows.csv

Ejecución (usuario hadoop, con HDFS encendido):
    spark-submit tarea3_batch.py
    spark-submit tarea3_batch.py hdfs://localhost:9000/Tarea3/trm.csv
"""
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RUTA_HDFS = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/Tarea3/trm.csv"
RUTA_SALIDA_HDFS = "hdfs://localhost:9000/Tarea3/resultados"
CARPETA_GRAFICAS = os.path.expanduser("~/tarea3/graficas")


def titulo(texto):
    print("\n" + "=" * 70)
    print(" " + texto)
    print("=" * 70)


def barras_ascii(filas, etiqueta, valor, ancho=45):
    """Gráfico de barras en consola (se ve directamente en PuTTY / VirtualBox)."""
    maximo = max(f[valor] for f in filas)
    for f in filas:
        n = int(round(f[valor] / maximo * ancho))
        print(f"{str(f[etiqueta]):>6} | {'#' * n} {f[valor]:,.2f}")


spark = SparkSession.builder.appName("Tarea3_Batch_TRM").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. CARGA
# ---------------------------------------------------------------------------
titulo("1. CARGA DEL CONJUNTO DE DATOS DESDE HDFS")
crudo = (spark.read
         .option("header", "true")
         .option("inferSchema", "false")   # todo como texto; se tipa en la limpieza
         .csv(RUTA_HDFS))
print(f"Ruta: {RUTA_HDFS}")
crudo.printSchema()
crudo.show(5, truncate=False)
total_crudo = crudo.count()
print(f"Registros leídos: {total_crudo}")

# ---------------------------------------------------------------------------
# 2. LIMPIEZA
# ---------------------------------------------------------------------------
titulo("2. LIMPIEZA")
# Nulos por columna antes de limpiar
crudo.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in crudo.columns]).show()

limpio = (crudo
          .select(
              F.regexp_replace(F.trim("VALOR"), ",", "").cast("double").alias("valor"),
              F.upper(F.trim("UNIDAD")).alias("unidad"),
              F.to_date(F.trim("VIGENCIADESDE"), "dd/MM/yyyy").alias("desde"),
              F.to_date(F.trim("VIGENCIAHASTA"), "dd/MM/yyyy").alias("hasta"))
          .dropna(subset=["valor", "desde", "hasta"])     # filas incompletas o con fecha inválida
          .filter(F.col("valor") > 0)                       # valores imposibles
          .filter(F.col("hasta") >= F.col("desde"))         # rangos de vigencia incoherentes
          .dropDuplicates(["desde"]))                       # una sola TRM por fecha de inicio

total_limpio = limpio.count()
print(f"Registros antes: {total_crudo} | después: {total_limpio} | "
      f"descartados: {total_crudo - total_limpio}")

# ---------------------------------------------------------------------------
# 3. TRANSFORMACIÓN
# ---------------------------------------------------------------------------
titulo("3. TRANSFORMACIÓN (nuevas columnas)")
orden = Window.orderBy("desde")
trm = (limpio
       .withColumn("anio", F.year("desde"))
       .withColumn("mes", F.month("desde"))
       .withColumn("decada", (F.floor(F.col("anio") / 10) * 10).cast("int"))
       .withColumn("dias_vigencia", F.datediff("hasta", "desde") + 1)
       .withColumn("valor_anterior", F.lag("valor").over(orden))
       .withColumn("variacion_pct",
                   F.round((F.col("valor") - F.col("valor_anterior"))
                           / F.col("valor_anterior") * 100, 4))
       .cache())
trm.select("desde", "valor", "anio", "mes", "dias_vigencia", "variacion_pct") \
   .orderBy(F.desc("desde")).show(10)

# ---------------------------------------------------------------------------
# 4. ANÁLISIS EXPLORATORIO (EDA)
# ---------------------------------------------------------------------------
titulo("4.1 ESTADÍSTICA DESCRIPTIVA DE LA TRM (COP por USD)")
trm.select("valor", "variacion_pct").summary(
    "count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()
rango = trm.agg(F.min("desde").alias("inicio"), F.max("desde").alias("fin")).first()
print(f"Periodo cubierto: {rango['inicio']} a {rango['fin']}")

titulo("4.2 ESTADÍSTICAS POR AÑO (DataFrame + groupBy)")
anual = (trm.groupBy("anio")
         .agg(F.count("*").alias("registros"),
              F.round(F.avg("valor"), 2).alias("promedio"),
              F.round(F.min("valor"), 2).alias("minimo"),
              F.round(F.max("valor"), 2).alias("maximo"),
              F.round(F.stddev("variacion_pct"), 4).alias("volatilidad_pct"))
         .orderBy("anio"))
anual.show(50)

titulo("4.3 OPERACIONES CON RDD (map, filter, reduceByKey, reduce)")
rdd = trm.select("anio", "valor").rdd
# Promedio anual con transformaciones de RDD: (anio, (suma, conteo))
promedio_rdd = (rdd.map(lambda r: (r["anio"], (r["valor"], 1)))
                   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                   .map(lambda kv: (kv[0], round(kv[1][0] / kv[1][1], 2)))
                   .sortByKey())
print("Promedio anual (primeros 5 con take):", promedio_rdd.take(5))
dias_4000 = rdd.filter(lambda r: r["valor"] > 4000).count()
print(f"Registros con TRM > 4.000 COP (filter + count): {dias_4000}")
maximo = rdd.map(lambda r: r["valor"]).reduce(lambda a, b: a if a > b else b)
print(f"TRM máxima histórica (reduce): {maximo:,.2f} COP")
print(f"Particiones del RDD: {rdd.getNumPartitions()}")

titulo("4.4 MAYORES ALZAS Y CAÍDAS DIARIAS")
movs = trm.filter(F.col("variacion_pct").isNotNull()) \
          .select("desde", "valor_anterior", "valor", "variacion_pct")
print("Top 5 alzas:")
movs.orderBy(F.desc("variacion_pct")).show(5)
print("Top 5 caídas:")
movs.orderBy("variacion_pct").show(5)

titulo("4.5 PROMEDIO POR DÉCADA Y ESTACIONALIDAD POR MES (Spark SQL)")
trm.createOrReplaceTempView("trm")
spark.sql("""
    SELECT decada, ROUND(AVG(valor), 2) AS promedio, ROUND(MAX(valor), 2) AS maximo
    FROM trm GROUP BY decada ORDER BY decada
""").show()
spark.sql("""
    SELECT mes, ROUND(AVG(variacion_pct), 4) AS variacion_media_pct,
           ROUND(STDDEV(variacion_pct), 4) AS volatilidad_pct
    FROM trm WHERE variacion_pct IS NOT NULL
    GROUP BY mes ORDER BY mes
""").show(12)

titulo("4.6 LOS 5 AÑOS MÁS VOLÁTILES")
anual.orderBy(F.desc("volatilidad_pct")).show(5)

# ---------------------------------------------------------------------------
# 5. VISUALIZACIÓN
# ---------------------------------------------------------------------------
filas_anuales = anual.collect()   # pocas filas (una por año): seguro traerlas al driver

titulo("5.1 PROMEDIO ANUAL DE LA TRM (gráfico en consola)")
barras_ascii(filas_anuales, "anio", "promedio")

titulo("5.2 GRÁFICAS PNG (matplotlib)")
try:
    import matplotlib
    matplotlib.use("Agg")          # servidor sin entorno gráfico
    import matplotlib.pyplot as plt

    os.makedirs(CARPETA_GRAFICAS, exist_ok=True)
    serie = trm.select("desde", "valor").orderBy("desde").toPandas()
    variaciones = trm.select("variacion_pct").dropna().toPandas()
    anios = [f["anio"] for f in filas_anuales]

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(serie["desde"], serie["valor"], color="#1f5fa8", linewidth=1)
    ax.set_title("TRM histórica en Colombia (COP por USD)")
    ax.set_xlabel("Fecha"); ax.set_ylabel("COP"); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(f"{CARPETA_GRAFICAS}/1_serie_historica.png", dpi=120)

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(anios, [f["promedio"] for f in filas_anuales], color="#1f5fa8")
    ax.set_title("Promedio anual de la TRM"); ax.set_xlabel("Año"); ax.set_ylabel("COP")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout(); fig.savefig(f"{CARPETA_GRAFICAS}/2_promedio_anual.png", dpi=120)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(variaciones["variacion_pct"], bins=80, color="#1f5fa8")
    ax.set_title("Distribución de la variación entre registros consecutivos (%)")
    ax.set_xlabel("Variación %"); ax.set_ylabel("Frecuencia")
    fig.tight_layout(); fig.savefig(f"{CARPETA_GRAFICAS}/3_histograma_variacion.png", dpi=120)

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(anios, [f["volatilidad_pct"] or 0 for f in filas_anuales],
            marker="o", color="#d9822b")
    ax.set_title("Volatilidad anual de la TRM (desviación estándar de la variación %)")
    ax.set_xlabel("Año"); ax.set_ylabel("Desv. estándar %"); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(f"{CARPETA_GRAFICAS}/4_volatilidad_anual.png", dpi=120)

    plt.close("all")
    print(f"Gráficas guardadas en {CARPETA_GRAFICAS}")
except ImportError:
    print("matplotlib no está instalado: pip3 install matplotlib pandas")

# ---------------------------------------------------------------------------
# 6. GUARDAR RESULTADOS EN HDFS
# ---------------------------------------------------------------------------
titulo("6. RESULTADOS GUARDADOS EN HDFS")
anual.coalesce(1).write.mode("overwrite").option("header", "true") \
     .csv(f"{RUTA_SALIDA_HDFS}/trm_anual")
print(f"Resumen anual escrito en {RUTA_SALIDA_HDFS}/trm_anual")

input("\nPresiona Enter para terminar (la interfaz de Spark sigue en el puerto 4040)...")
spark.stop()
