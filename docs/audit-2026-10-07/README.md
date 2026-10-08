# Evidencia de la auditoría de 7 de octubre de 2026

Consultar [el informe](../compatibilidad-spark-3.5.md) para las conclusiones y límites.

- `rdd-inventory.json`: inventario estático de referencias RDD/contexto/APIs relacionadas tras quitar comentarios. Incluye imports, generadores y módulos no activos. No constituye un análisis exhaustivo de llamadas transitivas.
- `runtime-results.json`: resultados obtenidos en Spark 3.5.9; los errores esperados demuestran problemas de las adaptaciones existentes. Las comprobaciones de WordCount incluyen el contenido de los Parquet escritos por el código Scala del repositorio.
- `scala-smoke.sh`: comandos usados para ejecutar los JAR finos micro/common compilados con overrides Spark 3.5.9, Scala 2.12.18 y Hadoop 3.3.4. Requiere preparar esos JAR como `/tmp/hibench-audit-20261007/sparkbench-{micro,common}.jar` en el maestro. El script crea datos nuevos y no se diseñó para relanzarse sobre un namespace HDFS ya existente. Para repetir, cambiar la ruta de auditoría en ambos scripts; no borrar datos anteriores automáticamente.
- `spark35_runtime_probe.py`: ejecutar con `spark-submit --master 'local[2]' --conf spark.eventLog.enabled=false --conf spark.sql.shuffle.partitions=2`. Las tres comprobaciones finales requieren haber ejecutado primero el smoke Scala y conservar sus resultados.

La aplicación YARN fue `application_1791359074600_0001`, con estado final SUCCEEDED. Los logs completos quedan localmente en `report/compatibility-audit-2026-10-07` y no se añaden a Git porque `report/` está ignorado.

No se ejecutó la suite original mediante `run_all.sh`, ni el assembly completo, ni la batería ML; consultar la matriz de validación del informe antes de presentar estos resultados como soporte de toda la suite.
