# bigdata-tarea3
Procesamiento de datos agrícolas con Hadoop, Apache Spark, Kafka y Spark Streaming

# Análisis de datos agrícolas con Hadoop, Apache Spark y Kafka

## 1. Descripción del proyecto

Este proyecto desarrolla un ejercicio de procesamiento de datos utilizando tecnologías de Big Data. Para el procesamiento por lotes se utiliza el conjunto de datos EVA.csv, relacionado con la actividad agrícola colombiana. Se emplean Hadoop para el almacenamiento distribuido y Apache Spark para el procesamiento y análisis de los datos.

Como complemento, se implementa una simulación de procesamiento en tiempo real mediante Apache Kafka y Spark Streaming, utilizando datos simulados de sensores.

## 2. Objetivos

### Objetivo general

Aplicar herramientas de Big Data para almacenar, procesar y analizar información agrícola mediante Hadoop y Apache Spark, complementando el ejercicio con una simulación de procesamiento en tiempo real mediante Kafka y Spark Streaming.

### Objetivos específicos

- Utilizar Hadoop HDFS para almacenar el conjunto de datos.
- Procesar información agrícola mediante Apache Spark.
- Obtener resúmenes de los datos a escala nacional, departamental y municipal.
- Implementar un productor de datos simulados con Apache Kafka.
- Procesar los eventos recibidos mediante Spark Streaming.

## 3. Tecnologías utilizadas

- **Python:** lenguaje utilizado para los programas.
- **Apache Hadoop HDFS:** almacenamiento de los datos.
- **Apache Spark:** procesamiento distribuido de información.
- **Apache Kafka:** transmisión de eventos.
- **Spark Structured Streaming:** procesamiento continuo de datos.
- **EVA.csv:** conjunto de datos utilizado para el análisis agrícola.

## 4. Estructura del repositorio

```text
bigdata-Tarea3/
├── README.md
├── tarea3_batch.py
├── kafka_producer.py
└── spark_streaming_consumer.py
```

## 5. Procesamiento por lotes

El archivo `tarea3_batch.py` contiene el programa de procesamiento por lotes del conjunto de datos EVA.csv.

El archivo de entrada se almacena en Hadoop HDFS en la siguiente ubicación:

```text
/Tarea3/datos/EVA.csv
```

### Preparación del entorno

Se requiere tener Hadoop y Apache Spark instalados y configurados, además de disponer del conjunto de datos EVA.csv.

Para comprobar los procesos de Hadoop, ejecutar:

```bash
jps
```

Si HDFS está detenido, iniciarlo con:

```bash
start-dfs.sh
```

Comprobar que el archivo de datos esté disponible:

```bash
hdfs dfs -ls /Tarea3/datos
```

### Ejecución

Desde la carpeta donde se encuentre el programa, ejecutar:

```bash
/opt/spark/bin/spark-submit tarea3_batch.py
```

El comando debe ejecutarse desde la ubicación del archivo o utilizando su ruta completa.

### Resultados

El procesamiento genera resultados en Hadoop HDFS en las siguientes ubicaciones:

```text
/Tarea3/resultados/resumen_nacional
/Tarea3/resultados/resumen_cauca
/Tarea3/resultados/resumen_almaguer
```

Para consultar los archivos generados:

```bash
hdfs dfs -ls -R /Tarea3/resultados
```

Los resultados permiten consultar resúmenes de la información agrícola nacional, del departamento del Cauca y del municipio de Almaguer.

**Nota:** si las carpetas de resultados ya existen, se debe comprobar el comportamiento del programa antes de ejecutarlo nuevamente, ya que Spark puede rechazar una ruta de salida existente.

## 6. Procesamiento en tiempo real con Kafka y Spark Streaming

Esta parte del proyecto utiliza datos simulados para demostrar el envío y procesamiento continuo de eventos.

### Archivos utilizados

- `kafka_producer.py`: genera y envía eventos simulados a Kafka.
- `spark_streaming_consumer.py`: consume los eventos y los procesa mediante Spark Structured Streaming.

Los eventos simulados contienen información como identificador del sensor, temperatura, humedad y marca de tiempo. Estos datos son de prueba y no corresponden a mediciones reales del conjunto EVA.csv.

### Preparación de Kafka

Con Kafka instalado y configurado en `/opt/Kafka`, iniciar ZooKeeper:

```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties
```

En otra terminal, iniciar el servidor Kafka:

```bash
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties
```

Si ambos servicios ya están ejecutándose, no es necesario iniciarlos nuevamente.

### Crear el tópico

En otra terminal, ejecutar:

```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
```

El tópico `sensor_data` recibe los eventos generados por el productor.

### Ejecutar el consumidor de Spark Streaming

Con Kafka en funcionamiento, ejecutar el consumidor desde la carpeta que contiene el archivo:

```bash
/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
```

La versión del conector de Kafka debe corresponder con la versión instalada de Spark.

### Ejecutar el productor

En otra terminal, ejecutar:

```bash
python3 kafka_producer.py
```

El productor enviará eventos simulados al tópico `sensor_data`, que serán recibidos por el consumidor de Spark Streaming.

## 7. Consideraciones

- El archivo EVA.csv debe estar disponible en HDFS antes de ejecutar el procesamiento por lotes.
- Las rutas de Hadoop y Spark corresponden al entorno utilizado durante el desarrollo y pueden requerir ajustes en otras instalaciones.
- Los servicios de Hadoop, Kafka y Spark deben estar configurados correctamente.
- El procesamiento en tiempo real utiliza datos simulados y constituye un ejercicio complementario al análisis agrícola.
- Los resultados dependen del conjunto de datos disponible y de la correcta ejecución de los programas.

## 8. Autoría y propósito académico

Repositorio desarrollado como evidencia práctica de la actividad académica de Big Data, orientada al almacenamiento, procesamiento distribuido y análisis de información mediante Hadoop, Apache Spark y Kafka.
