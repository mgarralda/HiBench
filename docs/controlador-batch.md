# Controlador batch

La referencia es `configs/docker-yarn.yaml`. `hibench list` muestra los IDs y parámetros derivados de las configuraciones batch. `mode` admite `prepare`, `run` y `prepare_and_run`; `repetitions` repite el benchmark sobre un input; `timeout_s` limita cada fase y la espera del destino. `resources` fija executors, memoria, cores y particiones. `spark_conf` admite propiedades adicionales sin sobrescribir los recursos administrados.

La UI importa/exporta YAML y permite workloads, parámetros, comprobar destino, Run, cancelación, logs y resultados. El worker vive fuera de Streamlit: recargar no vuelve a enviar trabajos. Para repetir, pulsar Nueva ejecución antes de Run. El historial recupera ejecuciones después de perder la sesión.

SQLite y snapshots viven en `report/control` o `HIBENCH_STATE_DIR`. Cada ID tiene configuración, logs y eventos JSONL. CLI: `hibench status ID`, `hibench logs ID`, `hibench cancel ID`. No borrar este directorio mientras haya trabajos activos.

Inputs nuevos: `/HiBench-Control/datasets`, con huella de parámetros, configuración y JARs. El manifiesto se publica tras preparar y comprobar existencia en HDFS. `run` exige un manifiesto coincidente. Outputs: `/HiBench-Control/runs/ID`. No hay limpieza automática de datos.

Los experimentos se serializan por destino. Cancelar termina aplicaciones YARN identificadas y el grupo remoto. Una caída abrupta del controlador puede dejar un lease sin recuperación automática: comprobar los procesos y YARN antes de intervenir. Esa recuperación queda pendiente.

Java 11 es la referencia de compilación/runtime. El JAR Spark evita empaquetar Spark/Hadoop/Scala del runtime y conserva Hadoop examples para TeraSort. Quedan fronteras RDD de formatos heredados. Migrar generadores a Spark SQL/Parquet y añadir adaptadores cloud requiere fases siguientes.

La compilación no certifica los 24 algoritmos. Publicar siempre runtime, configuración y resultados. No comparar con cifras antiguas sin revisar formato, escala, caching y acciones. WordCount/Sort generan texto explícito; repartition fuerza el shuffle al desactivar output. El ejemplo desactiva adaptive SQL y dynamic allocation.

Las tablas SQL pertenecen a una base `hibench_<run_id>`; el runner no usa tablas compartidas de `default`. No hay borrado automático del metastore de otras ejecuciones. Las métricas SQL cuentan el tamaño del directorio de input preparado, que puede contener tablas no consumidas por una consulta concreta.

`doctor` comprueba versiones, JARs y hashes de scripts/configuraciones contra el checkout. Los cambios de bin/conf requieren reinstalación, incluso cuando no cambia el JAR. El helper PowerShell de build usa un directorio temporal Linux para evitar problemas de rendimiento de Docker sobre el filesystem Windows. Por defecto ejecuta las pruebas Java; `-SkipTests` las omite.

La distribución del generador KMeans ahora respeta el total exacto de muestras, incluyendo restos entre clusters y ficheros parciales. Puede cambiar el dataset respecto a versiones anteriores que redondeaban al alza; no comparar cifras antiguas suponiendo el mismo número de filas.

La ejecución utiliza ahora una [interfaz de adaptadores](adaptadores.md). `hibench adapters` lista las implementaciones disponibles y los proveedores pendientes.

El formulario empieza por el adaptador y los datos del destino. Comprobar conexión no requiere seleccionar workloads. El segundo bloque configura el experimento y los recursos solicitados; antes de Run se muestra un resumen. La configuración guardada sigue incluyendo ambos bloques en un único YAML/JSON. Los perfiles independientes de destino todavía no están implementados.
