# Tarea 3 – Procesamiento de datos con Apache Spark (TRM Colombia)

Curso Big Data (202016911) – UNAD · Grupo 22 · Karol Yanith Alzate Gómez

## Problema

¿Cómo se ha comportado el precio del dólar en Colombia desde 1992 y en qué periodos fue más volátil? Se analiza la **Tasa Representativa del Mercado (TRM)** con Apache Spark en batch y se simula el mercado cambiario en tiempo real con Kafka y Spark Structured Streaming.

## Conjunto de datos

|Campo|Descripción|
|-|-|
|VALOR|TRM en pesos colombianos por dólar|
|UNIDAD|Moneda (COP)|
|VIGENCIADESDE / VIGENCIAHASTA|Fechas de vigencia (dd/MM/yyyy)|

Fuente: Superintendencia Financiera de Colombia, Datos Abiertos Colombia – [https://www.datos.gov.co/api/views/mcec-87by/rows.csv](https://www.datos.gov.co/api/views/mcec-87by/rows.csv)

## Archivos

|Archivo|Qué hace|
|-|-|
|`tarea3\_batch.py`|Carga desde HDFS, limpieza, transformación, EDA con DataFrames, RDD y Spark SQL, gráficas y guardado de resultados en HDFS|
|`kafka\_producer.py`|Genera operaciones simuladas de compra/venta de dólares y las envía al topic `trm\_operaciones`|
|`spark\_streaming\_consumer.py`|Consume el topic, calcula estadísticas por ventana de 1 minuto y emite alertas de precio|

## Entorno

Ubuntu Server 22.04.5 · Hadoop 3.3.6 · Spark 3.5.8 (Scala 2.12) · OpenJDK 11 · Kafka 3.9.2 · Python 3.10

\---

## Parte 1 – Procesamiento en batch

Todo se ejecuta con el usuario `hadoop`.

```bash
# 1. Encender HDFS y YARN
start-all.sh
jps

# 2. Descargar el dataset desde la fuente original y subirlo a HDFS
mkdir -p \~/tarea3 \&\& cd \~/tarea3
wget -O trm.csv https://www.datos.gov.co/api/views/mcec-87by/rows.csv
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put -f trm.csv /Tarea3/
hdfs dfs -ls /Tarea3

# 3. Librerías para las gráficas (una sola vez)
sudo apt install -y python3-pip     # solo si pip3 no existe
pip3 install matplotlib pandas

# 4. Ejecutar el análisis
spark-submit tarea3\_batch.py
```

Mientras corre, abre [http://localhost:4040](http://localhost:4040) en el navegador para ver Jobs y Stages.

**Resultados:**

* En consola: limpieza, estadísticas, promedio por año, operaciones RDD, alzas y caídas, década, mes y gráfico de barras en texto.
* Gráficas PNG en `\~/tarea3/graficas/`. Para copiarlas a Windows (PowerShell):

```powershell
  scp -P 2222 hadoop@localhost:\~/tarea3/graficas/\*.png .
  ```

* Resumen anual en HDFS: `hdfs dfs -cat /Tarea3/resultados/trm\_anual/\*.csv`

\---

## Parte 2 – Tiempo real con Kafka y Spark Streaming (Anexo 3)

Kafka se instala en la carpeta del usuario `hadoop`, así no hace falta `sudo` y `spark-submit` queda disponible en la misma sesión.

```bash
# 1. Librería de Python para Kafka
pip3 install kafka-python

# 2. Descargar Kafka 3.9.2 (versión con ZooKeeper; Kafka 4.x ya no lo usa)
cd \~
wget https://downloads.apache.org/kafka/3.9.2/kafka\_2.12-3.9.2.tgz
#   si da error 404, usar el archivo histórico:
#   wget https://archive.apache.org/dist/kafka/3.9.2/kafka\_2.12-3.9.2.tgz
tar -xzf kafka\_2.12-3.9.2.tgz
mv kafka\_2.12-3.9.2 \~/kafka

# 3. Limitar memoria (la VM tiene 4 GB)
export KAFKA\_HEAP\_OPTS="-Xmx512M -Xms512M"

# 4. Iniciar ZooKeeper y Kafka en segundo plano
\~/kafka/bin/zookeeper-server-start.sh -daemon \~/kafka/config/zookeeper.properties
sleep 5
\~/kafka/bin/kafka-server-start.sh -daemon \~/kafka/config/server.properties
sleep 10

# 5. Crear el topic
\~/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \\
  --replication-factor 1 --partitions 1 --topic trm\_operaciones
\~/kafka/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic trm\_operaciones
```

Para ahorrar memoria, puedes apagar YARN durante esta parte: `stop-yarn.sh`.

**Ejecución en dos terminales** (en la consola de VirtualBox usa Alt+F2 para abrir una segunda sesión, o conéctate por SSH al puerto 2222):

```bash
# Terminal 1 – productor
cd \~/tarea3
python3 kafka\_producer.py

# Terminal 2 – consumidor
cd \~/tarea3
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.8 spark\_streaming\_consumer.py
```

Cada 10 segundos el consumidor imprime dos tablas: `estadisticas\_trm` (operaciones, precio promedio, mínimo, máximo y monto por minuto) y `alertas\_trm` (operaciones que se alejan más de 1 % del precio de referencia). La pestaña **Structured Streaming** en [http://localhost:4040](http://localhost:4040) muestra la tasa de entrada y el tiempo de procesamiento.

Detener: Ctrl+C en ambas terminales, y luego:

```bash
\~/kafka/bin/kafka-server-stop.sh
\~/kafka/bin/zookeeper-server-stop.sh
```

## Problemas comunes

|Error|Solución|
|-|-|
|`Failed to find data source: kafka`|Falta `--packages` o no hay internet en la VM|
|`NoSuchMethodError` o `ClassNotFound ... scala`|El paquete debe terminar en `\_2.12:3.5.8`, no `\_2.13`|
|`NoBrokersAvailable` en el productor|Kafka no está encendido; revisar con `jps` que aparezcan `Kafka` y `QuorumPeerMain`|
|El consumidor no muestra filas|Arrancar primero el productor; el consumidor lee desde los mensajes nuevos|
|Kafka se cae por memoria|Exportar `KAFKA\_HEAP\_OPTS` y apagar YARN|



