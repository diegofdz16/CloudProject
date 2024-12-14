from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, year
import sys
import time

start_time = time.time()

spark = SparkSession.builder.appName('Year Ratings').getOrCreate()

df_libros = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
df_libros = df_libros.withColumnRenamed("Title", "Libro").withColumnRenamed("review/score", "rating")
df_libros = df_libros.filter(~df_libros['rating'].rlike('(?i)rating')).withColumn("rating", col("rating").cast("float"))
df_libros = df_libros.filter((col("rating") >= 0) & (col("rating") <= 5))

average_ratings = df_libros.groupBy("Libro").agg(
    avg("rating").alias("media"))

df_years = spark.read.csv(sys.argv[2], header=True, inferSchema=True)
df_years = df_years.withColumnRenamed("Title", "Libro").withColumnRenamed("publishedDate", "Fecha")

df_years = df_years.withColumn(
    "Anyo",
    when(col("Fecha").isNull() | (col("Fecha") == ""), 0)
    .otherwise(year(col("Fecha")))
)


df_unido = df_libros.join(df_years, on="Libro", how="inner")

average_ratings_by_year = df_unido.groupBy("Anyo").agg(
    avg("rating").alias("media_rating")
).orderBy("Anyo")

resultado_agrupado = average_ratings_by_year.withColumn(
    "Años",
    when(col("Anyo") >= 2010, "2010 en adelante")
    .when((col("Anyo") >= 2000) & (col("Anyo") < 2010), "2000 a 2010")
    .when((col("Anyo") > 0) & (col("Anyo") < 2000), "Antes del 2000")
    .otherwise("Año desconocido")
)

resultado_agrupado = resultado_agrupado.groupBy("Años").agg(
    avg("media_rating").alias("media_rating")
)

resultado_agrupado.write.option("header", "true").mode("overwrite").csv(sys.argv[3])

end_time = time.time()

execution_time = end_time - start_time
print(f"Tiempo de ejecución: {execution_time:.2f} segundos")
