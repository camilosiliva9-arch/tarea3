from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

from kafka import KafkaProducer
import json
import threading
import time
import random

"""Se crean 2 dunciones, una para generar los datos como se solicita en la guia y otra para iniciar el streaming de datos y cliente pueda recibirlos"""

# ========= 1. GENERADOR DE DATOS  =====================
def iniciar_productor():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    productos = ["Laptop", "Mouse", "Teclado", "Monitor"]
    categorias = ["Tecnologia", "Oficina"]

    while True:
        data = {
            "producto": random.choice(productos),
            "categoria": random.choice(categorias),
            "cantidad": random.randint(1, 5),
            "precio": round(random.uniform(10, 500), 2)
        }

        producer.send("ventas-tiempo-real", value=data)
        print("Enviado:", data)

        time.sleep(2)



# ============================= 2. STREAMING ==============================
def iniciar_streaming():
    spark = SparkSession.builder \
        .appName("StreamingVentas") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType() \
        .add("producto", StringType()) \
        .add("categoria", StringType()) \
        .add("cantidad", IntegerType()) \
        .add("precio", DoubleType())

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ventas-tiempo-real") \
        .load()

    df = df_kafka.selectExpr("CAST(value AS STRING)")

    df_json = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    df_json = df_json.withColumn("total", col("cantidad") * col("precio"))

    ventas_categoria = df_json.groupBy("categoria") \
        .agg(sum("total").alias("ventas_totales"))

    query = ventas_categoria.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()



# ////////////////////////////////////////////////////////////////////////////////////////
if __name__ == "__main__":
    # Ejecutar productor en paralelo
    hilo_productor = threading.Thread(target=iniciar_productor)
    hilo_productor.daemon = True
    hilo_productor.start()

    # Ejecutar streaming
    iniciar_streaming()
