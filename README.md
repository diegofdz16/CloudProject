# CloudProject
Proyecto CloudyBigData
## Miembros
* Diego Fernández Albert
* Gonzalo Sanchez Alonso
### 1 Descripcion del problema 
La idea del proyecto es analizar las valoraciones de los usuarios que realizan personas sobre libros en amazon.Podemos sacar a partir de estas popularidad de libros, actividad de usuarios, gustos entre otros
### 2 necesidad de Cloud y BigData

El Big Data es esencial debido a la enorme cantidad de datos que gestionamos, como las más de 3 millones de reseñas de más de 200,000 libros diferentes, con la posibilidad de ampliar estos datos mediante la integración de otras plataformas. Al estar organizados, los datos permiten una búsqueda y un análisis mucho más rápido. Mediante el uso de estos datos y técnicas estadísticas, es posible conocer los gustos de los usuarios y sus opiniones. Estas valoraciones nos permiten conocer  las obras literarias mas reconocidas por usuarios. Esta evolución está relacionada con los avances literarios y culturales, que se reflejan en los datos que recopilamos.

### 3 Descripicion de los datos

Nuestros datos han sido obtenidos de dos datasets sobre reseñas de libros en amazon , que tiene información relevante de las reseñas, de los libros y de los usuarios como los ids de usuario y libro, titulo, precio,nombre del usuario de la reseña, la puntuacion del usuario y el texto de la reseña . Con todos estos datos, lo que hemos hecho es ir organizando y filtrando distintas condiciones para poder hacer distintos enfoques.

Los ficheros son los siguientes:

Books_rating.csv
books_data.csv

### 4 Descripcion de la aplicacion

* Para gestionar todos los ficheros que forman nuestro proyecto, hemos utilizado GitHub.
* Como lenguaje de programación para todos nuestros scripts, hemos utilizado Python.
* Para procesar dichos scripts hemos utilizado Apache Spark, ya que nos permite realizar tareas de análisis de grandes volúmenes de datos con procesamiento en tiempo real como por lotes.
* Para llevar a cabo las pruebas de nuestros scripts, hemos utilizado Google Cloud como plataforma para la ejecución de dichos scripts.

### 5 Software

Los scripts son los siguientes:
* 
*
*
### 6 Uso
#### Uso instancia local
Para ello debemos iniciar una instancia de VM , instalar PySpark e imporat el script que nos interese.
Lo siguiente sera introducir el siguiente comando:<br><br>
```
spark-submit script.py input.txt output
```
Espera a que finalice y mira el resultado con:   
```
ls output cat output/*
```  

#### Uso de cluster
Para ello debemos crearlo como aprendido en los laboratorios usando la Cloud Shell:
```

gcloud dataproc clusters create mycluster --region=europe-southwest1 \
--master-machine-type=e2-standard-4 --master-boot-disk-size=50 \
--worker-machine-type=e2-standard-4 --worker-boot-disk-size=50 \
--enable-component-gateway
```  
Y atraves del master node:  
```
BUCKET=gs://BUCKET_NAME
spark-submit script.py $BUCKET/input $BUCKET/output5

```
### 7 Evaluacion de rendimiento

### 8 Caracteristicas avanzadas

* Se ha explorado la API de los DataFrames de Spark para realizar operaciones como unión de Dataframes
* Se ha consultado la biblioteca matplotlib de python para la generacion de graficos, el codigo esta añadido en algunos scripts sin embargo no se ejecuta ya que solo funciona local, por lo que está comentado.

### 9 Conclusiones y resultados

Hemos realizado un pequeño estudio sobre los libros mejor valorados para distintos propósitos.

Mejorar las valoraciones medias utilizando el número de reseñas y de los libros para una valoración más precisa.

Hemos aprendido a manejar grandes cantidades de datos, a desenvolvernos con el uso de spark y también a utilizar los servicios básicos de Google cloud.

### 10 Referencias 
* Datasets : https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews

