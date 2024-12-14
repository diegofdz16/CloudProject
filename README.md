# CloudProject
Proyecto CloudyBigData
## Miembros
* Diego Fernández Albert
* Gonzalo Sanchez Alonso
### 1 Descripcion del problema 

### 2 necesidad de Cloud y BigData

El Big Data es necesario por la gran cantidad de datos que manejamos, actualmente +85k películas, con posible expansión de datos a partir de otras plataformas. Al estar estructurados la búsqueda y análisis de datos se procesarán a mayor velocidad. Haciendo uso de estos datos y de métodos estadísticos se pueden hacer predicciones, a mayor cantidad de datos mayor fiabilidad del resultado. Estas predicciones hablan de los gustos y necesidades de los espectadores y de cómo van evolucionando. La evolución tiene que ver con los avances tecnológicos y culturales, lo que vemos reflejado en los datos.

### 3 Descripicion de los datos

Nuestros datos han sido obtenidos de dos datasets sobre reseñas de libros en amazon , que tiene información relevante de las reseñas, de los libros y de los usuarios como los ids de usuario y libro, titulo, precio,nombre del usuario de la reseña, la puntuacion del usuario y el texto de la reseña . Con todos estos datos, lo que hemos hecho es ir organizando y filtrando distintas condiciones para poder hacer distintos enfoques.

Los ficheros son los siguientes:

Books_rating.csv
books_data.csv

### 4 Descripcion de la aplicacion

* Para gestionar todos los ficheros que forman nuestro proyecto, hemos utilizado GitHub.
* Como lenguaje de programación para todos nuestros scripts, hemos utilizado Python.
* Para procesar dichos scripts hemos utilizado Apache Spark, ya que nos permite hacer uso de una programación funcional paralela.
* Para llevar a cabo las pruebas de nuestros scripts, hemos utilizado Google Cloud como plataforma para la ejecución de dichos scripts.

### 5 Software

Los scripts son los siguientes:
* 
*
*
### 6 Uso
* Uso instancia local*
Para ello debemos iniciar una instancia de VM , instalar PySpark e imporat el script que nos interese.
Lo siguiente sera introducir el siguiente comando:
```spark-submit movies_by_country.py                                                         ```


### 7 Evaluacion de rendimiento

### 8 Caracteristicas avanzadas

* Se ha explorado la API de los DataFrames de Spark para realizar operaciones como unión de Dataframes

### 9 Conclusiones y resultados



### 10 Referencias 
* Datset : https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews

