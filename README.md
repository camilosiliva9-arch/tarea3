# tarea3
## Estudiantes
* Camilo Silva (@unadvirual.edu.co)

## Paso realizado en la tarea 3:

Implementación en Spark: Desarrollar una aplicación
implementado Spark (utilizando Python) que realicen las
siguientes tareas:
• Procesamiento en batch:
  o Cargar el conjunto de datos seleccionado desde la fuente original.
  o Realizar operaciones de limpieza, transformación y análisis exploratorio de datos (EDA) utilizando RDDs o DataFrames.
  o Visualizar los resultados del procesamiento en batch.
• Procesamiento en tiempo real (Spark Streaming & Kafka):
  o Configurar un topic en Kafka para simular la llegada de datos en tiempo real (usar un generador de datos).
o Implementar una aplicación Spark Streaming que consuma datos del topic de Kafka
o Realizar algún tipo de procesamiento o análisis sobre los datos en tiempo real (contar eventos, calcular estadísticas, etc.).
o Visualizar los resultados del procesamiento en tiempo real.


## Paso a paso para la ejecucion de los comandos.
### Ejecucion del batch
1. Para ejecutar el archivo del batch.py en consola se pasa el siguiente comando:
    ```Bash
     python3 batch.py
    ```
3. Esto ejecutar el comando [batch.py](./batch.py) y mostara los resultados.

### Ejecucion del spark.py
1. Para ejecutar primero en la terminal se debe iniciar el servicio de Kafka. Para esto se recomeinda ejecutar los siguientes comandos en orden en el terminal.
   ```Bash
     KAFKA_CLUSTER_ID=$(/opt/Kafka/bin/kafka-storage.sh random-uuid)
   ```
2. Luego se ejecutara el sigueinte comando que va a preparar y formatear el directorio de almacenamiento de un nodo de Apache Kafka cuando se utiliza el modo KRaft
   ```Bash
     /opt/Kafka/bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c /opt/Kafka/config/server.properties
   ```
3. A continaucion se iniciara el servidor de Kafka
   ```Bash
     /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
   ```
4. Una reocmendacion es tener un archivo .sh que automatice esto:
   ```Bash
   # Comandos para ejecutar Kafka
   KAFKA_CLUSTER_ID=$(/opt/Kafka/bin/kafka-storage.sh random-uuid)
   /opt/Kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c /opt/Kafka/config/server.properties
   /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
   ```
5. Acontinuacion se ejecutara el programa [spark.py](./spark.py)
   ```Bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark.py
   ```
6. Para confirmar Kafka en otra terminal ejecuta
   ```Bash
     bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ventas-tiempo-real --from-beginning
   ```

### IMPORTANTE ⚠⚠
* Comprender que esto solo esta  probado para las sigueintes versiones de KAFKA y Python:  
  - *Kafka:* version 4.1.1
  - *Python:* Python 3.12.3

   
