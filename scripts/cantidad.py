from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, lit, sum
import sys
import time

start_time = time.time()

# Inicializar la sesión de Spark
spark = SparkSession.builder.appName('Year Ratings').getOrCreate()

# Leer el archivo CSV de libros
df_libros = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
df_libros = df_libros.withColumnRenamed("Title", "Libro")

df_libros = df_libros.withColumn("Cuantos", lit(1))

# Leer el archivo CSV de años
df_years = spark.read.csv(sys.argv[2], header=True, inferSchema=True)
df_years = df_years.withColumnRenamed("Title", "Libro").withColumnRenamed("publishedDate", "Fecha")

# Crear la columna 'Anyo' extrayendo el año de la fecha
df_years = df_years.withColumn(
    "Anyo",
    when(col("Fecha").isNull() | (col("Fecha") == ""), 0)
    .otherwise(year(col("Fecha")))
)

# Unir ambos DataFrames por la columna "Libro"
df_unido = df_libros.join(df_years, on="Libro", how="inner")

# Agrupar por "Anyo" y sumar la columna "Cuantos"
average_ratings_by_year = df_unido.groupBy("Anyo").agg(
    sum("Cuantos").cast("int").alias("Cantidad")  # Sumar los valores de "Cuantos"
)

# Ordenar los resultados por "Anyo"
average_ratings_by_year = average_ratings_by_year.orderBy("Anyo")

# Calcular la suma total de la columna "Cantidad"
total_count = average_ratings_by_year.agg(sum("Cantidad").cast("int").alias("Total")).collect()[0]["Total"]

# Crear la columna "Años" según el rango de años
resultado_agrupado = average_ratings_by_year.withColumn(
    "Años",
    when(col("Anyo") >= 2010, "2010 en adelante")
    .when((col("Anyo") >= 2000) & (col("Anyo") < 2010), "2000 a 2010")
    .when((col("Anyo") > 0) & (col("Anyo") < 2000), "Antes del 2000")
    .otherwise("Año desconocido")
)

# Calcular el porcentaje
resultado_agrupado = resultado_agrupado.withColumn(
    "Porcentaje",
    (col("Cantidad") / total_count) * 100
)

# Agrupar por la columna "Años" y sumar los resultados, y también calcular el porcentaje
resultado_agrupado = resultado_agrupado.groupBy("Años").agg(
    sum("Cantidad").cast("int").alias("Cantidad"),
    sum("Porcentaje").cast("double").alias("Porcentaje")
)

# Escribir el resultado a un archivo CSV
resultado_agrupado.write.option("header", "true").mode("overwrite").csv(sys.argv[3])


end_time = time.time()

execution_time = end_time - start_time
print(f"Tiempo de ejecución: {execution_time:.2f} segundos")

