Actividad práctica con Docker + Kafka + Spark

A. Se levantan los servicios y se crea topic con las siguientes instrucciones:

	docker-compose down -v
	docker-compose pull
	docker-compose up -d
	docker exec -it kafka bash
	kafka-topics --bootstrap-server kafka:9092 --list
	kafka-topics \--create \--topic actividad-topic \--bootstrap-server kafka:9092 \--partitions 1 \--replication-factor 1
	Y desde la terminal en Jupiter se ejecuta pip install kafka-python

	 

B. Descripción de la solución construida con procesamiento en tiempo real 

Objetivo: implementar un sistema de procesamiento de datos IoT en tiempo real utilizando Kafka y Spark Structured Streaming. Un Producer generará continuamente mediciones de temperatura, humedad y presión de diferentes sensores y las publicará en un topic de Kafka. Spark consumirá los eventos, interpretará los mensajes JSON, transformará los datos y calculará indicadores agregados por sensor. Además, identificará mediciones que superen determinados umbrales y almacenará los resultados procesados en formato Parquet.

Consideraciones

- El producer genera continuamente datos simulados entre 1 a tres segundos de temperatura, humedad y presión para diferentes sensores.
-	El consumer lee los eventos, convierte los datos, detecta valores fuera de un rango establecido. La decisión de clasificar las alertas en NORMAL/ALERTA/CRÍTICO la hará Spark en el Consumer

- Para el ejercicio se tendrán 5 máquinas
  	M001 → Sensor S001
	M002 → Sensor S002
  	M003 → Sensor S003
  	M004 → Sensor S004
  	M005 → Sensor S005

-	Cada sensor tendrá un umbral
Máquina	Sensor	Temp. máxima	Humedad máxima	Presión mínima
M001	S001	30 °C	70 %	1000 hPa
M002	S002	35 °C	75 %	1000 hPa
M003	S003	28 °C	65 %	1005 hPa
M004	S004	32 °C	80 %	995 hPa
M005	S005	30 °C	70 %	1005 hPa

-	La ejecución del producer3 en la terminal es de la siguiente forma:
 

-	En el consumer se ejecutan en un notebook los siguientes pasos:
  	Importar librerías
	Crear SparkSession
  	Definir el esquema de los sensores
  	Conectar con Kafka
  	Convertir el JSON recibido
  	Definir los umbrales. 
  	Cruzar sensores con sus umbrales
  	Calcular las alertas
  	Determinar el estado de las alertas: NORMAL / ALERTA / CRITICO
  	Seleccionar el resultado final
  	Preparar las rutas de salida
  	iniciar el streaming. 
  	Mostrar las notificaciones por pantalla
  	Guardar los resultados en Parquet
  	verificar que los streams están activos
  	Detener el consumer. 
  	Leer el Parquet.  
  	Mostrar las últimas 10 ejecuciones.  
  	Resumen final. 
 
