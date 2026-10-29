

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, sum as suma, avg, count,
    regexp_replace, lower
)
import unicodedata

# Crear la sesion de Spark
spark = SparkSession.builder \
    .appName("Tarea3_Batch_Produccion_Agricola_EVA") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Rutas de entrada y salida en HDFS
ruta_entrada = "hdfs://localhost:9000/Tarea3/datos/EVA.csv"
ruta_salida = "hdfs://localhost:9000/Tarea3/resultados"

print("========================================")
print(" ANALISIS AGRICOLA EVA CON APACHE SPARK")
print("========================================")

# Leer el archivo CSV desde HDFS
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .option("sep", ",") \
    .option("quote", '"') \
    .option("multiLine", "true") \
    .csv(ruta_entrada)

print("\nColumnas originales:")
print(df.columns)

cantidad_inicial = df.count()
print("\nCantidad inicial de registros:", cantidad_inicial)


# Normalizar nombres de columnas para identificarlas
def normalizar(texto):
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(
        caracter for caracter in texto
        if not unicodedata.combining(caracter)
    )
    texto = texto.replace("\n", " ")
    return "".join(
        caracter.lower()
        for caracter in texto
        if caracter.isalnum()
    )


columnas = {
    normalizar(nombre): nombre
    for nombre in df.columns
}


def buscar_columna(nombre_normalizado):
    for clave, original in columnas.items():
        if clave == nombre_normalizado:
            return original

    for clave, original in columnas.items():
        if nombre_normalizado in clave:
            return original

    return None


departamento = buscar_columna("departamento")
municipio = buscar_columna("municipio")
cultivo = buscar_columna("cultivo")
anio = buscar_columna("ano")
produccion = buscar_columna("produccion")
area_cosechada = buscar_columna("areacosechada")
rendimiento = buscar_columna("rendimiento")

print("\nColumnas identificadas:")
print("Departamento:", departamento)
print("Municipio:", municipio)
print("Cultivo:", cultivo)
print("Año:", anio)
print("Producción:", produccion)
print("Área cosechada:", area_cosechada)
print("Rendimiento:", rendimiento)

# Comprobar que existan las columnas necesarias
if not all([
    departamento, municipio, cultivo, anio, produccion
]):
    spark.stop()
    raise ValueError(
        "No se encontraron las columnas obligatorias del archivo CSV."
    )

# Preparar los datos y seleccionar las columnas necesarias
datos = df.select(
    col(departamento).alias("departamento"),
    col(municipio).alias("municipio"),
    col(cultivo).alias("cultivo"),
    col(anio).alias("anio_original"),
    col(produccion).alias("produccion_original"),
    (
        col(area_cosechada)
        if area_cosechada
        else col(produccion).cast("double") * 0
    ).alias("area_original"),
    (
        col(rendimiento)
        if rendimiento
        else col(produccion).cast("double") * 0
    ).alias("rendimiento_original")
)

# Limpiar textos y convertir los campos numericos
datos = datos \
    .withColumn(
        "departamento",
        trim(col("departamento").cast("string"))
    ) \
    .withColumn(
        "municipio",
        trim(col("municipio").cast("string"))
    ) \
    .withColumn(
        "cultivo",
        trim(col("cultivo").cast("string"))
    ) \
    .withColumn(
        "anio",
        regexp_replace(
            col("anio_original").cast("string"), ",", "."
        ).cast("double")
    ) \
    .withColumn(
        "produccion",
        regexp_replace(
            col("produccion_original").cast("string"), ",", "."
        ).cast("double")
    ) \
    .withColumn(
        "area_cosechada",
        regexp_replace(
            col("area_original").cast("string"), ",", "."
        ).cast("double")
    ) \
    .withColumn(
        "rendimiento",
        regexp_replace(
            col("rendimiento_original").cast("string"), ",", "."
        ).cast("double")
    )

# Filtrar registros con datos obligatorios validos
datos_limpios = datos.filter(
    col("departamento").isNotNull() &
    (col("departamento") != "") &
    col("municipio").isNotNull() &
    (col("municipio") != "") &
    col("cultivo").isNotNull() &
    (col("cultivo") != "") &
    col("anio").isNotNull() &
    col("produccion").isNotNull() &
    (col("produccion") >= 0)
)

cantidad_limpia = datos_limpios.count()

print("\nRegistros después de la limpieza:", cantidad_limpia)
print("Registros descartados:", cantidad_inicial - cantidad_limpia)

print("\nMUESTRA DE DATOS LIMPIOS")
datos_limpios.select(
    "departamento",
    "municipio",
    "cultivo",
    "anio",
    "produccion",
    "area_cosechada"
).show(10, truncate=False)

# Resumen por departamento, cultivo y año
resumen = datos_limpios.groupBy(
    "departamento", "cultivo", "anio"
).agg(
    suma("produccion").alias("produccion_total"),
    suma("area_cosechada").alias("area_cosechada_total"),
    avg("rendimiento").alias("rendimiento_promedio"),
    count("*").alias("cantidad_registros")
)

print("\nTOP 20: PRODUCCION POR DEPARTAMENTO Y CULTIVO")
resumen.orderBy(
    col("produccion_total").desc()
).show(20, truncate=False)

# Guardar resumen nacional en HDFS
resumen.write.mode("overwrite") \
    .option("header", "true") \
    .csv(ruta_salida + "/resumen_nacional")

# Analisis del departamento del Cauca
cauca = resumen.filter(
    lower(col("departamento")) == "cauca"
)

print("\nPRODUCCION AGRICOLA DEL CAUCA")
cauca.orderBy(
    col("produccion_total").desc()
).show(20, truncate=False)

cauca.write.mode("overwrite") \
    .option("header", "true") \
    .csv(ruta_salida + "/resumen_cauca")

# Analisis del municipio de Almaguer
almaguer = datos_limpios.filter(
    (lower(col("departamento")) == "cauca") &
    (lower(col("municipio")) == "almaguer")
)

print("\nREGISTROS DE ALMAGUER")
print("Cantidad de registros:", almaguer.count())

almaguer_resumen = almaguer.groupBy(
    "anio", "cultivo"
).agg(
    suma("produccion").alias("produccion_total"),
    suma("area_cosechada").alias("area_cosechada_total"),
    avg("rendimiento").alias("rendimiento_promedio"),
    count("*").alias("cantidad_registros")
)

almaguer_resumen.orderBy(
    "anio", "cultivo"
).show(50, truncate=False)

almaguer_resumen.write.mode("overwrite") \
    .option("header", "true") \
    .csv(ruta_salida + "/resumen_almaguer")

print("\n========================================")
print(" PROCESAMIENTO BATCH FINALIZADO")
print(" Resultados guardados en HDFS")
print("========================================")

spark.stop()
