
# Se crea una sesión de Spark y se cargan los datos desde un archivo CSV 
# utilizando DataFrames, lo que permite trabajar de manera estructurada con los datos.

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AnalisisVentas") \
    .getOrCreate()

df = spark.read.csv("Superstore.csv", header=True, inferSchema=True)

df.show(5)

# Se eliminaron valores nulos y registros duplicados para garantizar la calidad de los datos.

df = df.dropna()

df = df.dropDuplicates()

# Se creó una nueva columna llamada "Ventas_Totales"
# para facilitar el análisis del ingreso generado por cada registro.

from pyspark.sql.functions import col, sum, to_date, regexp_replace, round

df = df.replace('', None)
df = df.replace("", None)


df = df.withColumn(
    "Sales",
    regexp_replace(col("Sales"), "[^0-9.]", "")
)

df = df.withColumn(
    "Quantity",
    regexp_replace(col("Quantity"), "[^0-9.]", "")
)

df = df.withColumn(
    "Quantity",
    round(
        regexp_replace(col("Quantity"), ",", ".").try_cast("double")
    ).try_cast("int")
)

df = df.withColumn(
    "Sales",
    round(
        regexp_replace(col("Sales"), ",", ".").try_cast("double")
    )
)

df = df.na.drop(subset=["Quantity", "Sales"])
df = df.replace("", None)
df = df.na.drop()

df = df.withColumn("Sales_total", col("Sales") * col("Quantity"))


# Productos más vendidos
print("+++++++++++++++++++PRODUCTOS MAS VENDIDOS++++__++++++++++++++++++++++++++ ")
df.groupBy("Product Name") \
  .agg(sum("Quantity").alias("total_quantity")) \
  .orderBy("total_quantity", ascending=False) \
  .show(10)

# Categorías más vendidas
print("+++++++++++++++++++CATEGORIAS MAS VENDIDOS++_++++++++++++++++++++++++++++ ")
df.groupBy("Category") \
  .agg(sum("Sales_total").alias("total_sales")) \
  .orderBy("total_sales", ascending=False) \
  .show()

# Ventas por fecha
print("+++++++++++++++++++VENTAS POR FECHAS++++++++++++++++++++++++++++++++++++++ ")
df.groupBy("Order Date") \
  .agg(sum("Sales_total").alias("total_sales")) \
  .orderBy("Order Date") \
  .show()



# Se utilizó matplotlib para visualizar los resultados,
# facilitando la interpretación de los datos analizado

import matplotlib.pyplot as plt

pdf = df.groupBy("Category").sum("Sales_total").toPandas()

pdf.plot(x="Category", y="sum(Sales_total)", kind="bar")
plt.show()
