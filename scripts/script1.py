from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import count
from pyspark.sql.functions import avg
import sys

# Crear sesión de Spark
spark = SparkSession.builder.appName("AmazonBookReviews").getOrCreate()

file_path = sys.argv[1]
output_path = sys.argv[2]
output_path2 = sys.argv[3]

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

# Agrupar por book_id y contar reviews
reviews_per_book = df.groupBy("book_id").agg(count("*").alias("num_reviews"))

# Calcular la media de rating por libro (agrupado por book_id)
avg_rating_per_book = df.groupBy("book_id").agg(avg("review_score").alias("avg_rating"))


# Agrupar por user_id y calcular la media de review_score
avg_rating_per_user = df.groupBy("user_id").agg(avg("review_score").alias("avg_rating"))

# Agrupar por user_id y contar el número de reviews
reviews_per_user = df.groupBy("user_id").agg(count("*").alias("num_reviews"))

# Unir las dos agregaciones de book_id (número de reviews y media de rating)
book_summary = reviews_per_book.join(avg_rating_per_book, "book_id")

# Unir también el título del libro desde el DataFrame original (sin duplicar las filas)
book_summary_with_title = book_summary.join(df.select("book_id", "title").distinct(), "book_id")


# Unir las dos agregaciones de user_id (número de reviews y media de rating)
user_summary = reviews_per_user.join(avg_rating_per_user, "user_id")

                                             # Unir también el nombre del usuario desde el DataFrame original (sin duplicar las filas)
user_summary_with_name = user_summary.join(df.select("user_id", "user_name").distinct(), "user_id")


book_summary_with_title.write.csv(output_path,header = True)
user_summary_with_name.write.csv(output_path2,header = True)
