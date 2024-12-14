from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count,when,col,lit
import sys
import os
import matplotlib.pyplot as plt
import time

start_time = time.time()

# Crear sesión de Spark
spark = SparkSession.builder.appName("AmazonBookReviews").getOrCreate()

# Obtener las rutas del archivo de entrada y salida desde los argumentos
file_path = sys.argv[1]
output_path = sys.argv[2]

# Define el esquema del CSV
schema = StructType([
    StructField("book_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("helpfulness", StringType(), True),
    StructField("review_score", DoubleType(), True),
    StructField("review_time", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("text", StringType(), True)
])

# Cargar el archivo CSV
df = spark.read.csv(file_path, schema=schema, header=True)

# Añadir una columna para identificar reseñas "sin puntuar"
df = df.withColumn(
    "review_score_category",
    when(col("review_score").isNull(), lit("sin puntuar")).otherwise(col("review_score"))
)

df = df.filter(
    ((col("review_score_category") == "sin puntuar") | col("review_score_category").between(1, 5)) 
)

# Agrupar por categoría de puntaje y contar cuántas veces aparece cada una
rating_counts = df.groupBy("review_score_category").agg(count("*").alias("num_reviews"))

# Calcular el total de reseñas
total_reviews = df.count()

# Añadir una columna con el porcentaje respecto al total
rating_counts = rating_counts.withColumn(
    "percentage",
    (col("num_reviews") / lit(total_reviews) * 100).cast("int")
).orderBy("review_score_category")
"""
# Obtener los resultados en un formato que pueda usarse para el gráfico
rating_counts_data = rating_counts.collect()


# Preparar los datos para el gráfico circular
ratings = [row["review_score"] for row in rating_counts_data]
num_reviews = [row["num_reviews"] for row in rating_counts_data]

# Crear el gráfico circular
plt.figure(figsize=(8, 6))
plt.pie(num_reviews, labels=ratings, autopct='%1.1f%%', startangle=90)
plt.title("Distribución de Puntuaciones de Reviews")
plt.axis("equal")  # Asegura que el gráfico sea circular

# Guardar el gráfico como una imagen
plt.savefig(image_output_path, format="png")  # Guardar como archivo PNG (puedes cambiar el formato si lo prefieres)

# Cerrar la figura para liberar recursos
plt.close()
"""
# Guardar los resultados en un archivo CSV
rating_counts.write.option("header", "true").csv(output_path)

end_time = time.time()

execution_time = end_time - start_time
print(f"Tiempo de ejecución: {execution_time:.2f} segundos")