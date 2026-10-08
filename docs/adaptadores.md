# Adaptadores de ejecución

El controlador usa `ExecutionAdapter` en `hibench/adapters/base.py`. Docker y local Linux están implementados sobre el backend clásico; SSH, Livy, EMR, Dataproc, Synapse, Fabric y Databricks figuran en el registro como pendientes. `hibench adapters` muestra su estado. Elegir un destino pendiente devuelve un error antes de crear una ejecución. Streamlit solo permite seleccionar implementaciones disponibles y muestra las pendientes.

## Contrato

- `capabilities`, `validate_target` y `validate`: describir capacidades y comprobar conexión/configuración antes del envío.
- `doctor` e `install`: verificar el destino y preparar su runtime cuando corresponda.
- `dataset_key`, `dataset_root` y `dataset_available`: identificar, ubicar y comprobar los datos preparados sin imponer HDFS al controlador.
- `stage(PhaseJob)`: distribuir configuración y artefactos requeridos para una fase.
- `submit(staged)`: devolver un `JobReference` con identificador nativo y metadatos serializables.
- `status`, `logs(reference, cursor)` y `cancel`: consultar estado, leer logs incrementalmente y cancelar en el destino.
- `wait`: recoger logs y métricas, atender cancelación/timeout y verificar el resultado autoritativo. Recibe callbacks; no depende de SQLite ni de Streamlit.

Cada PhaseJob representa preparación o benchmark y conserva el run ID, workload e índice de repetición. Los eventos guardan referencias del trabajo y actualizan los IDs de aplicaciones detectadas. Los handles de procesos no se serializan. Guardar referencias no implementa recuperación automática tras caída del worker; esa capacidad sigue pendiente.

El adaptador clásico concentra rutas Linux, configuración HiBench, subprocess, HDFS y validación/cancelación YARN. `backend.py` conserva sus utilidades de transporte y propiedades. El worker coordina datasets, fases, repeticiones y estado durable sin construir comandos Bash ni consultar YARN directamente. La interfaz de usuario y CLI usan la misma factoría.

## Preparación y capacidades

El catálogo declara requisitos de preparación a partir de los scripts mantenidos: `mapreduce` para run_hadoop_job, `spark_batch` y `classic_spark` para run_spark_job. Sleep no genera datos. Bayes utiliza ambos mecanismos. La ejecución actual de los benchmarks requiere Spark clásico. En modo `run` no se exige capacidad de generación, pero debe existir un dataset preparado compatible.

Esto permite que un futuro adaptador Livy implemente envío batch sin prometer soporte de los generadores MapReduce. El soporte de almacenamiento cloud, aprovisionamiento de datos externos y selección de adaptadores distintos por fase no está implementado todavía. Tampoco basta añadir el adaptador Databricks para hacer los algoritmos compatibles con serverless.

## Añadir un proveedor

Implementar ExecutionAdapter, registrar la clase en create_adapter y activar su entrada en REGISTRY solo después de validar envío, estado final, logs, cancelación, timeout y distribución de artefactos. Mover las reglas específicas de conexión al adaptador; no introducir sus rutas o credenciales en el worker. El esquema común conserva recursos, parámetros y fases. Las credenciales deben obtenerse del entorno o mecanismo del proveedor, evitando guardarlas en snapshots de experimentos.

Las pruebas en tests/test_adapters.py cubren destinos pendientes, capacidades por fase, recolección de métricas/logs, fallo autoritativo y cancelación/timeout mediante procesos reales locales y respuestas YARN simuladas. Las pruebas de Streamlit verifican el flujo y la idempotencia.

## Validación de esta extracción

Las 13 pruebas Python pasan, incluyendo Streamlit y un adaptador simulado sin comandos Bash ni almacenamiento HDFS. Docker/YARN completó Sleep (run ad7f8e4d-164b-4155-8782-951e47e47273) y las dos fases de WordCount con datos reducidos (run 66334371-2235-4e3d-858b-8efca6fcd5e7). Estados autoritativos, configuraciones y métricas: [evidencia JSON](adapter-validation-2026-10-07.json). La cancelación y timeout de la interfaz se verificaron con procesos locales; la cancelación real YARN documentada en la validación anterior precede esta extracción.
