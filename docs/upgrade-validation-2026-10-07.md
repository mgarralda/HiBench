# Validacion de la rama upgrade: 2026-10-07

Referencia: Spark 3.5.9, Scala 2.12.18, OpenJDK 11.0.32.1, Python remoto 3.10.12 y Hadoop 3.3.6. Spark usa sus propios clientes Hadoop 3.3.4. Maven 3.9.9 y Java 11 para compilar.

La compilacion limpia de los 11 modulos batch paso. Despues se recompilo autogen con la correccion del total exacto de KMeans: dos pruebas Java y ocho del controlador Python/Streamlit pasaron. CI ejecuta ambos grupos. Los logs locales completos estan en report/; la evidencia compacta, configuraciones, estados YARN, metricas y hashes finales estan en [el JSON de validacion](upgrade-validation-2026-10-07.json).

## Jobs reales en YARN

| Workload | Application ID | Tiempo submit + ejecucion (s) | Resultado |
|---|---|---:|---|
| ScalaSparkWordcount | application_1791359074600_0015 | 26.751 | succeeded |
| ScalaSparkSort | application_1791359074600_0017 | 29.914 | succeeded |
| ScalaRepartition | application_1791359074600_0019 | 25.303 | succeeded |
| ScalaSparkKmeans | application_1791359074600_0021 | 35.012 | succeeded |
| ScalaSparkScan | application_1791359074600_0024 | 33.399 | succeeded |
| ScalaSparkKmeans | application_1791359074600_0027 | 39.707 | succeeded |

WordCount y Sort: generacion RandomTextWriter MapReduce con TextOutputFormat explicito. Repartition: TeraGen MapReduce, 100 registros de 100 bytes, caching activado y salida activada. SQL Scan: generacion HiBench.DataGen con 100 paginas y 100 visitas; tablas aisladas en una base por run. KMeans: generacion Mahout/MapReduce y entrenamiento Spark ML.

La primera prueba KMeans pidio 100 muestras y detecto 250: el generador rellenaba ficheros completos y redondeaba entre clusters. La correccion reparte el resto y escribe el ultimo fichero parcial. La repeticion final pidio 101, y DenseKMeans confirmo `numExamples = 101`. No presentar el primer entrenamiento como una prueba sobre 100 filas.

## Comprobaciones independientes

- WordCount: coincidencia con Counter de Python sobre todos los tokens del input; 3130 tokens.
- Sort: conserva las 49 filas y verifica orden dentro de cada fichero y entre rangos de particiones.
- Repartition: igualdad exacta de todos los registros binarios, clave y valor; 100 registros, 10000 bytes.
- SQL Scan: igualdad de las 100 visitas y sus nueve campos, leidos directamente de los SequenceFiles de entrada y salida.
- Cancelacion: Sleep de 60 segundos, app application_1791359074600_0025, quedo KILLED en YARN y cancelled en el controlador.
- Streamlit: carga y parametrizacion; dos pulsaciones Run crean un unico envio. Solicitudes concurrentes se resuelven con un solo ID.
- SQLite: 30 repeticiones de inicializacion concurrente pasaron tras corregir el cierre de conexiones y los reintentos de bloqueo. La interfaz reiniciada muestra la ejecucion final KMeans completada, sus logs y metricas.

Los verificadores reutilizables estan en tests/integration. No ejecutarlos sobre datasets grandes: recogen datos para comprobaciones exhaustivas pequenas.

## Limites del resultado

Esto verifica cinco workloads con datos reducidos y YARN client, no toda la suite ni grandes escalas ni yarn-cluster. El inventario automatizado actualizado detecta referencias a SparkContext/RDD/API clasica en 42 ficheros activos y 3 opcionales o inactivos; no equivale a 42 algoritmos RDD. Revisar [el inventario por fichero y linea](rdd-inventory-upgrade.json).

Siguen pendientes la migracion completa de generadores a DataFrames/Parquet, eliminar Mahout y ObjectFile/SequenceFile donde proceda, validar los otros 19 workloads, adaptar la ejecucion a cloud y construir una variante Spark Connect para Databricks serverless. XGBoost queda fuera del build normal. El monitor SSH historico no se ha validado funcionalmente; permanece desactivado.

El helper Windows bin/build-java11.ps1 usa el flujo Docker/Linux verificado; su sintaxis PowerShell se ha comprobado, pero el wrapper completo no se ha vuelto a ejecutar tras crearlo. Ante una caida abrupta del worker puede quedar un lease pendiente de recuperacion manual. No hay limpieza automatica de HDFS ni del metastore.
