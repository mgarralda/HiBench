# Auditoría de HiBench: Spark 3.5.9 y modernización batch

Fecha: 7 de octubre de 2026. Repositorio analizado: `E:\projects\HiBench`, HEAD `3dd2c97`. Clúster: `E:\projects\hadoop-spark-cluster`. El árbol de HiBench no tenía cambios locales al comenzar.

Complemento: [generación MapReduce, compatibilidad cloud y propuesta de interfaz Streamlit](plataformas-y-control-streamlit.md).

## Dictamen

**El proyecto es recuperable y parte del código actual ya funciona en el clúster nuevo. La suite completa, con sus scripts y configuración actuales, todavía no es ejecutable de forma fiable.**

Se han compilado los módulos base y micro para Spark 3.5.9, ejecutado sus JAR Scala de WordCount y Sort en modo local y ejecutado WordCount en YARN. La aplicación YARN `application_1791359074600_0001` terminó con `Final-State: SUCCEEDED`. Estas pruebas usan entradas pequeñas y configuración explícita, evitando el runner antiguo. No equivalen a validar `run_all.sh`, todos los generadores ni todos los workloads.

La compilación completa falla en dos generadores ML por una dependencia BLAS que cambió. Además hay errores propios de las adaptaciones a DataFrames, rutas antiguas, peticiones de memoria incompatibles con el clúster, Python 2 y problemas de integridad de resultados. Algunos de estos errores pueden producir un resultado aparentemente correcto sin medir el trabajo previsto.

La prioridad debe ser: **hacer reproducible y correcto el perfil batch de Spark clásico 3.5.9; después completar la migración de datos y ML; finalmente crear un perfil serverless separado.** Cambiar RDD por DataFrame no hace por sí solo que el runner, almacenamiento, dependencias y medición sean compatibles con Databricks serverless.

## 1. Entorno real y compatibilidad de versiones

| Componente | HiBench actual | Clúster verificado | Evaluación |
|---|---|---|---|
| Spark | Maven 3.3.1; rutas de ejecución 3.3.2 | 3.5.9 | Actualizar build y rutas; no reutilizar el ensamblado antiguo como validado |
| Scala | 2.12.15, sufijo `_2.12` | 2.12.18 | La familia binaria coincide; fijar 2.12.18 en la compilación objetivo |
| Java | Bytecode Java 8; `run_all.sh` impone Java 8 | OpenJDK 11.0.32.1 | El bytecode 8 no es el bloqueo; la ruta impuesta no existe en el contenedor |
| Hadoop servidor | POM 3.2.2; rutas 3.3.2 | 3.3.6 | Actualizar rutas y generadores MapReduce |
| Hadoop cliente Spark | Dependencias Hadoop empaquetables en HiBench | JAR `hadoop-client-api-3.3.4` de Spark | Distinguir servidor y cliente; no meter JAR servidor 3.3.6 indiscriminadamente en Spark |
| Python controlador HiBench | Shebangs Python 2 | `python` 2.7.18 | La imagen mantiene compatibilidad transitoria explícitamente |
| Python Spark | No hay workloads PySpark en la suite principal examinada | Python 3.10.12, `/opt/pyspark/bin/python` | Independiente del Python 2 del controlador |
| YARN | Ejecutores 5 GB; driver 4 GB; 4 cores por ejecutor | 3072 MB máximo por contenedor, 6 vcores por nodo, 3 nodos activos | 5 GB de heap más overhead no cabe; 4 GB driver también bloquea modo cluster |

Spark 3.5.9 admite Java 8/11/17 y Scala 2.12/2.13. Por tanto Java 11 y Scala 2.12 son una combinación soportada. La compilación debería realizarse con JDK 11 para reproducir el entorno; las pruebas Maven de esta auditoría usaron el JDK disponible del host Windows, Java 8u161, y las ejecuciones usaron Java 11 del clúster. Esta diferencia se registra como límite de la validación. [Documentación oficial](https://spark.apache.org/docs/3.5.9/).

El volumen `/home/sparker/HiBench` del maestro estaba vacío. Compose usa un volumen nombrado `hibench-data`, no un bind mount automático de este checkout Windows. Incluso arreglando el código, falta un procedimiento explícito de instalación/copiar artefactos en ese volumen. El maestro tampoco tiene Maven disponible. `MIGRATION.md` del clúster excluye expresamente HiBench de su validación.

Configuración inicial razonable para los smoke tests: Spark `/usr/local/spark`, Hadoop `/home/sparker/hadoop-3.3.6`, HDFS `hdfs://spark-cluster-master:9000`, un ejecutor de 1 GB y un core, driver 1 GB, dos particiones shuffle. Para ampliar a tres ejecutores hay que presupuestar heap, overhead y ApplicationMaster; no usar esta configuración pequeña como perfil de rendimiento general.

## 2. Evidencia de compilación y ejecución

La compilación usó overrides, sin cambiar los POM:

```powershell
mvn -B '-DskipTests' '-Dspark.version=3.5.9' '-Dspark.bin.version=3.5' '-Dscala.version=2.12.18' '-Dhadoop.mr2.version=3.3.4' package
```

El override Hadoop 3.3.4 permite contrastar la aplicación con los clientes incluidos en Spark. No decide todavía la versión final del módulo generador MapReduce.

| Prueba | Resultado | Qué demuestra |
|---|---|---|
| Reactor completo | Falla en `sparkbench-ml` | Hay un bloqueo real de compilación, no solo un riesgo teórico |
| `common`, `autogen`, `sparkbench/common`, `micro` | Compilan y empaquetan | Base compatible con las dependencias seleccionadas |
| `websearch`, `graph`, `sql`, con `-pl ... -am` | Compilan y empaquetan | No se detectaron errores de compilación en esos módulos; falta validación funcional |
| JAR Scala WordCount, `local[2]` | Exit 0 | Lectura Text, agregación DF y escritura Parquet funcionan |
| JAR Scala Sort, `local[2]` | Exit 0 | Lectura Text, ordenación DF y escritura Parquet funcionan |
| JAR Scala WordCount, YARN client | Exit 0 y YARN SUCCEEDED | Driver, distribución del JAR auxiliar, ejecutor e I/O HDFS funcionan |
| Lectura de las tres salidas Scala | Contenido correcto | WordCount local/YARN: a=2, b=2, c=1; Sort conserva las dos líneas de entrada |
| InputFormat/OutputFormat Hadoop usados como datasource SQL | Fallos reproducidos | Las adaptaciones SequenceFile necesitan reparación |
| Columnas y función binaria de Repartition | Fallos reproducidos | Su rama de escritura está rota |
| Repartition sin acción | Cero jobs enviados | La transformación sola no mide un shuffle |
| Sintaxis Python 3.11 del host, 8 archivos | 5 errores de sintaxis | Migrar Python 2 exige más que cambiar shebang |

Fallos de compilación: `LinearRegressionDataGenerator.scala:21,69` y `SVMDataGenerator.scala:24,60`, importación `com.github.fommil.netlib.BLAS`. Spark 3.5 usa `dev.ludovic.netlib`; la documentación oficial confirma ese backend. Opciones: usar la API pública de esa dependencia de forma explícita, o implementar un producto escalar sencillo en el generador. Para generación de datos, esta última opción evita introducir JNI y una dependencia antigua para resolver solo `ddot`. [Dependencias MLlib](https://spark.apache.org/docs/3.5.9/ml-guide.html).

No se ha ejecutado la batería completa ni se han modificado los workloads para hacerlos pasar. Los datos de prueba se crearon en `/tmp/hibench-audit-20261007`, tanto local como HDFS, separados de `/HiBench`. No se borraron datasets del usuario.

Los resultados estructurados de las pruebas se conservan en [runtime-results.json](audit-2026-10-07/runtime-results.json).

## 3. Qué queda de RDD y de la migración a DataFrames

La búsqueda con eliminación de comentarios localiza **45 archivos Scala con referencias a RDD, SparkContext o APIs relacionadas**, incluyendo generadores, utilidades y DAL inactivo. No significa que haya 45 workloads, ni que todos esos archivos tengan un algoritmo RDD: WordCount y Sort tienen algoritmo DF pero aún construyen un `SparkContext`. Inventario detallado en [audit-2026-10-07/rdd-inventory.json](audit-2026-10-07/rdd-inventory.json).

Hay tres tipos de trabajo pendiente:

1. **Algoritmo ya DF, bootstrap antiguo:** WordCount y Sort crean `SparkContext`; `IOCommon` exige ese contexto. Cambiar a `SparkSession` como dependencia y eliminar acceso al contexto en el perfil portable.
2. **Algoritmo DF, datos RDD:** KMeans y GMM leen `sequenceFile[LongWritable, VectorWritable]`; ALS, PCA, Linear y Correlation leen `objectFile` y después `toDF`. La generación de esos datos sigue usando RDD. Migrar conjuntamente productor y consumidor a esquemas Parquet.
3. **Algoritmo todavía RDD:** TeraSort, Sleep, InMemRepartition, PageRank websearch, GraphX/NWeight y los ML LogisticRegression, GBT, RF, LDA, SVM, SVD y Summarizer. Son migraciones reales de algoritmo o decisiones de alcance.

NaiveBayes consume Parquet y usa `spark.ml`, pero `BayesDataGen` conserva `sequenceFile`, `parallelize` y broadcast. Preparar externamente el dataset puede servir como transición; no convierte al generador en portable.

`BaseRangePartitioner`, `ConfigurableOrderedRDDFunctions` y `MemoryDataRDD` añaden dependencias de internals (`ShuffledRDD`, `PartitionPruningRDD`, utilidades Spark) y clases dentro de `org.apache.spark`. Conviene aislarlas en un módulo clásico; no arrastrarlas al núcleo DataFrame/serverless.

## 4. Matriz de workloads y propuesta de alcance

`conf/benchmarks.lst` activa 24 workloads: 5 micro, 3 SQL, 1 websearch, 14 ML y NWeight. GraphX PageRank y XGBoost tienen código/scripts pero no están seleccionados allí. DAL tiene directorio y scripts, pero no participa en el reactor por defecto.

En la tabla, «clásico» significa Spark local/YARN/standalone; las entradas no marcadas como ejecutadas requieren smoke test después de reparar build y configuración.

| Workload | Estado actual | Acción recomendada |
|---|---|---|
| WordCount | Algoritmo DF; contexto explícito; probado local y YARN | Conservar; refactorizar IO/arranque; validar tokens y counts |
| Sort | Algoritmo DF; probado local | Conservar; comprobar orden global por particiones, no asumir orden al releer Parquet |
| TeraSort | RDD, formato Tera y particionador propio | Conservar como clásico; una variante DF requiere contrato de bytes/orden e identidad distinta |
| Repartition HDFS | DF sobre lectura RDD; ejecución lazy incompleta y escritura rota | Reparar antes de publicar resultados |
| InMemRepartition | RDD; fuerza acción incluso sin salida | Mantener clásico o crear variante DF con payload y acción equivalentes |
| Sleep | `parallelize` + `Thread.sleep` | Opcional clásico: prueba del scheduler, no benchmark portable de negocio |
| SQL Scan/Join/Aggregation | SparkSession con Hive; tablas SerDe SequenceFile | Conservar operaciones; versión moderna CSV/Parquet con esquema explícito |
| Websearch PageRank | RDD iterativo | Conservar clásico opcional; variante DF separada si se desea |
| GraphX PageRank | GraphX; no seleccionado por defecto | Opcional clásico; aclarar diferencia con PageRank websearch |
| NWeight | GraphX/Pregel; generación RDD | Opcional clásico, fuera del perfil sin RDD |
| Bayes | `spark.ml` + Parquet; generador RDD | Conservar; modernizar generación y contrato de datos |
| KMeans/GMM | `spark.ml`; SequenceFile/Mahout → RDD → DF | Conservar; migrar datasets y eliminar Mahout de esta ruta |
| ALS | `spark.ml`; objeto serializado `ALS.Rating` leído por RDD | Conservar; Parquet `user,item,rating`; semilla explícita del modelo |
| Logistic Regression | `LogisticRegressionWithLBFGS`, `spark.mllib` | Migrar a `spark.ml.classification.LogisticRegression`; fijar equivalencia de parámetros |
| GBT/RF | `spark.mllib.tree` y `LabeledPoint` antiguo | Migrar a clasificadores DF; conservar límites y significado del problema |
| LDA | `spark.mllib.clustering.LDA`, RDD de documentos | Migrar a `spark.ml.clustering.LDA`; registrar optimizer y representación |
| SVM | `SVMWithSGD`; generador bloquea build | Reparar generador; variante `LinearSVC` cambia optimizer y necesita nueva versión del workload |
| SVD | `RowMatrix.computeSVD` RDD | Conservar opcional clásico; PCA no es sustitución directa equivalente |
| PCA | `spark.ml.PCA`, entrada objectFile | Conservar; Parquet; definir si se mide solo fit o también transform |
| Linear Regression | `spark.ml`, entrada objectFile; generador BLAS roto | Conservar; reparar generador, datos y parámetros ignorados |
| Correlation | `spark.ml.stat.Correlation`; entrada RDD y cache | Conservar; Parquet; describir correctamente correlación entre features |
| Summarizer | `spark.mllib.stat.Statistics` | Migrar a `spark.ml.stat.Summarizer` y vectors ML |
| XGBoost | 1.0.0, entrada RDD, biblioteca nativa; no seleccionado | Plugin/módulo opcional con versión validada; no dependencia obligatoria del core |
| DAL/DAAL KMeans | RDD y biblioteca Intel antigua; módulo inactivo | Retirar del mantenimiento principal; conservar en historial/tag si hace falta reproducibilidad |
| DFSIOE/Nutch auxiliares | Código presente, fuera de suite batch Spark seleccionada | Evaluar extracción de generadores útiles y retirar restos sin consumidores |

MLlib **no está obsoleta en su conjunto**. `spark.ml` es la API principal; `spark.mllib` está en mantenimiento, con correcciones pero sin nuevas funcionalidades. No hay que retirar KMeans, ALS o PCA por pertenecer a MLlib; hay que distinguir su API y contrato de datos. [Estado oficial de MLlib](https://spark.apache.org/docs/3.5.9/ml-guide.html).

La migración SVMWithSGD → LinearSVC y otras conversiones deben conservar una referencia reproducible y declarar diferencias de solver, entrenamiento, evaluación y coste. [Algoritmos Spark ML 3.5.9](https://spark.apache.org/docs/3.5.9/ml-classification-regression.html).

## 5. Defectos funcionales y de medición prioritarios

### P0: bloqueos y benchmarks potencialmente inválidos

- **Build ML bloqueado:** imports BLAS anteriores. Actualizar Spark/Scala en POM no basta.
- **Entorno impuesto:** `bin/run_all.sh:20-22` exporta Java 8 y Hadoop 3.3.2. La ruta Java no existe en el runtime nuevo. `conf/spark.conf`, `conf/hadoop.conf` y template Docker conservan rutas antiguas. Usar entorno/defaults verificables y rutas estables.
- **Memoria incompatible:** ejecutores 5 GB superan el máximo YARN 3072 MB incluso antes del overhead. Separar perfiles Docker pequeños y perfiles de rendimiento.
- **Repartition sin shuffle efectivo:** la configuración activa `fromhdfs=true`, `cacheinmemory=true`, `disableoutput=true`. `ScalaRepartition` materializa el input con `count`, crea `df.repartition(...)` y termina sin una acción sobre `shuffled`. Puede medir lectura/cache, pero no el shuffle. Con cache false podría no ejecutar ninguna acción sobre los datos. Una simple acción `count()` sobre ciertas proyecciones tampoco asegura el procesamiento de todo el payload: comprobar el plan ejecutado y métricas de shuffle.
- **Repartition con salida rota:** su DataFrame solo tiene `value`, pero construye `concat(key,value)`. Después usa `slice` con BINARY, aunque esa función exige ARRAY; debe usarse una operación binaria adecuada. Finalmente usa `SequenceFileOutputFormat` como datasource SQL, lo que falla. Son tres defectos independientes, reproducidos en 3.5.9.
- **SequenceFile SQL inválido:** `IOCommon.loadDF` usa `.format("org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat")`. Un InputFormat Hadoop no implementa el contrato de datasource SQL. El modo Text sí funciona; Sequence no. Mantener lector RDD temporal para clásico o convertir datasets a Parquet.
- **Fallo de workload ocultado:** en `run_all.sh`, después de `run.sh` se ejecuta `echo` antes de guardar `$?`; se comprueba el éxito de `echo`. Guardar inmediatamente el exit code. `run_hadoop_job` tampoco propaga de forma fiable un fallo: tiene comentado el `exit` y continúa.
- **IDs y reporting dependientes del terminal:** `execute_withlog` solo invoca `execute_with_log.py` si stdout es TTY. En CI/pipes ejecuta directamente y no captura el ID. El wrapper exige `APPLICATION_ID` al generar el reporte. Además el clúster configura Spark logging a ERROR, suprimiendo normalmente los mensajes INFO de submission que usa la regex. La prueba YARN exitosa no imprimió ID. Capturarlo de forma independiente del formato/nivel de logs o emitir un resultado estructurado desde la aplicación.

### P1: contratos y reproducibilidad

- `IOCommon.saveDF` siempre escribe Parquet Snappy y overwrite, aunque `sparkbench.outputformat` anuncie Text/Sequence/Null. La configuración publicada no describe el comportamiento real. Implementar opciones soportadas o rechazar las que ya no se aplican.
- `gen_report` divide por duración sin proteger el cero y calcula throughput por un listado estático de hosts, no por recursos efectivos. El throughput por nodo no es comparable en serverless ni con asignación variable.
- `dir_size` imprime todos los tokens numéricos de `hadoop fs -du -s`; esa salida incluye tamaño lógico y consumo replicado. Puede devolver dos números cuando el caller necesita uno. Seleccionar explícitamente la métrica y soportar HDFS/object storage.
- Repartition reporta tamaño `0` aunque haya obtenido `SIZE`; SQL Scan sustituye tamaño de entrada por tamaño de salida antes del reporte. Hay que separar `input_bytes`, `output_bytes`, `processed_records` y `shuffle_bytes`.
- La adquisición del ID usa `/home/sparker/application_id.txt` global: puede quedar obsoleto, cruzarse entre runs y no admite IDs `app-...` de standalone. Usar un directorio por ejecución y permitir un `run_id` independiente del ID del proveedor.
- `report_gen_plot.py` conserva el parser del formato antiguo: toma posiciones que ahora corresponden a ID/workload/fecha/hora. Migrar a CSV/JSON con nombres y versión del esquema.
- `--skip-if-exists` deriva rutas y capitalización a mano, fuera del resolver de configuración. Bayes produce además `.parquet`: existencia del input base no garantiza el dataset realmente consumido. Usar manifest, esquema, filas y marcador de generación completada.
- PCA hace `fit`, pero su `transform(...).select(...)` no tiene acción. Esto puede ser intencionado si el benchmark es solo entrenamiento; no afirmar que se mide transformación. SQL ejecuta DDL e INSERT, que sí son comandos: no extrapolar que todo SQL aquí es lazy por la ausencia de `collect`.
- Linear acepta `--tol` pero no llama `.setTol(params.tol)`; el parámetro de benchmark no se aplica. KMeans fija `.setTol(0)` para forzar iteraciones: debe registrarse como decisión experimental.
- El generador Linear inicializa el RNG de cada partición con la misma semilla y genera pesos con un RNG sin semilla fija: puede repetir secuencias entre particiones y variar entre ejecuciones. GBT/RF/XGBoost usan splits sin semilla explícita. Determinismo por seed y row id/partition es parte del contrato.
- Correlation imprime que correlaciona label con features, pero calcula la matriz entre features. Corregir descripción o cómputo.
- Los scripts SQL comparten nombres de tablas Hive y hacen DROP/CREATE: ejecuciones simultáneas pueden interferir. Usar namespace por run o vistas temporales con lectura datasource.
- `ScalaSparkSQLBench` importa `SparkFiles` pero resuelve el script con `Source.fromFile(sqlFile)`; el uso de basename distribuido depende del directorio de trabajo en cluster mode. Usar `SparkFiles.get` con tratamiento explícito de client/local y cerrar el Source. El splitting por `;` tampoco soporta SQL arbitrario, aunque puede bastar para templates controlados.
- Las estrategias de particionado de TeraSort y Repartition deducen reducers desde executors × cores, mientras otros workloads usan `hibench.default.shuffle.parallelism`. Unificar política y registrar el valor efectivo.

No cambiar retrospectivamente la interpretación de mediciones antiguas. Nuevos formatos, acciones o algoritmos requieren una versión de workload y una nueva línea base experimental.

## 6. Python 2 y ejecución

Archivos con error de sintaxis Python 3: `load_config.py`, `monitor.py`, `monitor_replot.py`, `terminalsize.py`, `report_gen_plot.py`. `execute_with_log.py`, el mapping y el test se parsean, pero eso no demuestra ejecución Python 3.

Cambios necesarios: print functions; `urllib.request`; vistas `dict.items()` no concatenables; texto/bytes en subprocess y streams; `sys.stdout.write` sin bytes; sustituir `mock` externo por `unittest.mock`; revisar scripts Python embebidos enviados por SSH en `monitor.py`; y división entera donde cambie la semántica. Usar una variable/configuración `HIBENCH_PYTHON` explícita, separada de `PYSPARK_PYTHON`.

Propuesta de runner: CLI Python 3 con comandos `doctor`, `list`, `prepare`, `run`, `suite` y `report`, conservando inicialmente wrappers Bash compatibles. Usar argv de subprocess, logs siempre capturados, timeouts y cancelación, codes de error consistentes y run directories únicos. Mantener Linux como entorno soportado del runner; que Maven compile en Windows no hace portables `fcntl`, `/proc`, SSH y herramientas GNU.

El monitor SSH debe ser opcional. Para Spark clásico, priorizar event logs y métricas del contenedor/JVM; para serverless, recoger las métricas realmente expuestas por el proveedor. No hacer fallar el benchmark si el monitor opcional no arranca. Reemplazar `eval` del log de monitor por JSON si se conserva ese monitor.

Solo después de verificar el controlador Python 3 y los scripts remotos se podrá retirar `python2` y el enlace `/usr/bin/python` de la imagen del clúster.

## 7. Dependencias, módulos y residuos a retirar

- **Streaming:** retirar `common/.../streaming`, `autogen/.../datagen/streaming`, helpers Storm/Gearpump/Flink, mappings/configs streaming y dependencias Kafka una vez verificados consumidores batch. Que no haya workloads streaming seleccionados no impide que esos módulos se compilen y empaqueten.
- **Kafka antiguo:** `common/pom.xml` requiere `kafka_2.11:0.8.2.1`; `autogen` usa kafka-clients 0.8.2.2. La compilación ya advierte de Scala 2.11, incluyendo `scala-xml_2.11`. Eliminar esta ruta evita mezclar familias binarias por funcionalidad fuera de alcance.
- **Mahout 0.9:** persiste en generadores Java, vectores SequenceFile y ML. No eliminarlo sin migrar esos datasets. Objetivo: generadores DataFrame/Parquet; módulo legacy solo si se requiere reproducción histórica.
- **XGBoost 1.0.0:** sacarlo del módulo ML obligatorio. Su actualización exige selección y validación específica de versión Spark/Scala, API, workers y librerías nativas, no sustituir ciegamente por la última versión. [Guía oficial XGBoost4J-Spark](https://xgboost.readthedocs.io/en/stable/jvm/xgboost4j_spark_tutorial.html).
- **Packaging:** `spark-sql` no tiene scope provided en `sparkbench/common` y `micro`; varios clientes Hadoop tampoco. El assembly desempaqueta dependencias transitivas. Esto puede introducir duplicados Spark/Hadoop/Scala y conflictos de servicios/logging. Auditar dependency tree y contenido real del JAR; Spark y runtime libraries provided; incluir solo dependencias de aplicación y fusionar servicios si se usa shade.
- **Build:** plugins Scala 3.2.0, compiler 3.2 y assembly 2.5.3 son una base antigua. Han compilado varios módulos en la prueba, así que no atribuirles el fallo BLAS, pero modernizar con Maven Wrapper, JDK 11, UTF-8 real y versiones fijadas. La propiedad `<encoding>` actual no evita los avisos de encoding Cp1252.
- **DAL y restos de Hadoopbench:** hay helpers de Hive/Mahout/Nutch que buscan rutas `hadoopbench/...` ausentes del reactor. Retirar referencias sin uso y preservar generadores MapReduce que aún alimenten batch.
- **Documentación:** README describe Spark 3.3 y algunos workloads como mllib ya migrados; conserva contenidos heredados. Publicar catálogo generado, matriz de soporte probado, instalación real en Docker, notas de breaking changes y alcance batch.
- **Comunidad:** preservar LICENSE/NOTICE y atribución Intel, documentar cómo contribuir, CI, releases y datasets. Distinguir licencia Apache de la petición académica de cita; no añadir requisitos legales nuevos para uso comunitario.

## 8. Databricks serverless es un segundo destino

La documentación actual admite tareas JAR serverless, aunque Scala y librerías JAR siguen sin admitirse en notebooks serverless. Por tanto no es necesario descartar automáticamente Scala. Las tareas JAR deben usar Spark Connect, APIs públicas y versiones Scala/JDK del entorno; el ejemplo oficial environment 4 usa Scala 2.13, JDK 17 y Databricks Connect 17.3. No se puede transportar sin más este JAR Spark 3.5 `_2.12`. [JAR serverless](https://docs.databricks.com/aws/en/jobs/how-to/use-jars-in-workflows).

El perfil serverless necesita quitar SparkContext/RDD, internals, JNI y datasource personalizados no soportados. Las APIs cache/persist/checkpoint de DataFrame no están soportadas, así que cambiar el algoritmo a DF tampoco basta. El almacenamiento debe integrarse con Unity Catalog; Hive SerDe/SequenceFile del perfil SQL actual no sirve. La disponibilidad de API ML y bibliotecas debe validarse contra el entorno elegido. [Límites serverless](https://docs.databricks.com/aws/en/compute/serverless/limitations).

Mantener tres capacidades explícitas: `classic`, `dataframe` y `serverless`, con requisitos por workload. El perfil DataFrame puede seguir utilizando operaciones que el proveedor limite. GraphX, SVD/RowMatrix, TeraSort original y Sleep pueden seguir siendo útiles para comunidad Spark clásica sin bloquear un subconjunto serverless.

Medición serverless: coste y duración end-to-end, perfil de query cuando esté disponible, bytes/filas lógicos y configuración permitida. No prometer event logs ni métricas CPU por nodo como las de clásico. Documentar diferencias de AQE, ANSI, timezone y Photon; comparaciones solo con metodologías y métricas compatibles.

## 9. Hoja de ruta con criterios de aceptación

### Fase A: recuperar el batch clásico 3.5.9

1. Fijar Spark 3.5.9, Scala 2.12.18 y JDK 11; separar versiones Hadoop de generador/cliente y reparar BLAS.
2. Resolver packaging y sacar streaming/Kafka del core; mantener XGBoost opcional.
3. Actualizar rutas y recursos; `doctor` verifica runtime, JARs, HDFS y capacidad YARN antes de lanzar jobs.
4. Corregir exit codes, ID/reporting, `dir_size` y Repartition; migrar infraestructura Python 3.
5. Instalar artefactos explícitamente en volumen Docker. Validar todos los workloads seleccionados a escala tiny en YARN client; después YARN cluster y standalone donde se declaren soportados.

Aceptación: reactor limpio en JDK 11, Python 3 operativo sin Python 2, fallos nunca reportados como éxito, todos los seleccionados con estado y resultados correctos, reports con run_id y tamaños válidos. Registrar excepciones temporales, especialmente clásicos RDD.

### Fase B: núcleo batch moderno

1. Definir esquemas Parquet: texto, pares Tera si se crea variante, `label/features`, ratings y edges.
2. Migrar generadores y lectores conjuntamente; converter explícito para datasets legacy, sin reutilización silenciosa.
3. Migrar LR/GBT/RF/LDA/SVM/Summarizer; eliminar Mahout del core cuando ningún consumidor lo necesite.
4. Versionar semántica del workload; fijar seeds y separar generación, lectura, entrenamiento, evaluación y escritura en métricas.
5. CI con checks de corrección y una suite Docker tiny; runs de rendimiento separados, con repeticiones, warmup, mediana/dispersión y planes/configuración registrados.

Aceptación: núcleo sin RDD/SparkContext en fuentes y generadores, sin Kafka/Mahout innecesarios; esquemas y manifest verificables; resultados reproducibles. El perfil clásico puede conservar módulos opcionales RDD documentados.

### Fase C: perfil serverless y ampliación comunitaria

Build específico Databricks Connect/environment, storage Unity Catalog, runner/adaptador de jobs y métricas compatibles. Validar primero WordCount, Sort, SQL y Bayes; ampliar ML solo con prueba funcional del entorno. Un catálogo debe rechazar workloads que requieran capacidades ausentes.

Después de estabilizar la base, considerar workloads batch modernos de formatos columnares, joins/skew, funciones ventana y pipelines ML. Incorporarlos con propósito y contrato explícitos, sin convertir la modernización inicial en una ampliación indiscriminada del benchmark.

## 10. Artefactos y límites de esta auditoría

Los scripts de diagnóstico y el inventario quedan en `docs/audit-2026-10-07`. Los logs completos de build/runtime, estado YARN y resultados estructurados quedan en `report/compatibility-audit-2026-10-07` (carpeta report ignorada por Git). Las pruebas Scala usaron JARs finos de micro/common, no el assembly completo, porque ML impide construirlo.

No están validados todavía: todos los generadores sobre YARN, todo ML, grafos en ejecución, datasets grandes, standalone, YARN cluster, Azure/HDInsight y Databricks serverless. Compilar graph/SQL no demuestra equivalencia de resultados ni performance. La auditoría aporta una base real para priorizar, no una declaración de soporte completo.
