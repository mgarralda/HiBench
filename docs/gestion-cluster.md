# Navegación y gestión del laboratorio

La entrada `hibench/web.py` gestiona la navegación lateral. Las páginas viven en `hibench/ui/`: Experimentos, Ejecuciones (seguimiento e historial), Clústeres y Ayuda. La lógica de instalación y ciclo de vida vive en `hibench/services/clusters.py`, sin dependencia de Streamlit ni FastAPI.

Docker existente es un adaptador de envío: ejecuta en un contenedor ya levantado. La página Clústeres gestiona el laboratorio por separado, utilizando el repositorio https://github.com/mgarralda/hadoop-spark-cluster.git. SSH sigue registrado como pendiente y será una alternativa de transporte hacia el maestro; esta entrega no lo implementa.

El perfil guarda directorio local, nombre del proyecto Compose y rama/etiqueta de descarga. El proyecto de referencia utiliza `spark-cluster`. Descargar requiere un directorio nuevo y no sobrescribe el checkout existente. Las imágenes se construyen en el orden documentado en el laboratorio: base, master, slave y Jupyter. Arrancar ejecuta Compose con `--wait` y healthchecks. Consultar estado muestra los contenedores del proyecto. Detener utiliza `stop`; retirar contenedores utiliza `down` sin `-v`. No existe operación para borrar volúmenes.

Todas las acciones de escritura necesitan pulsación explícita. Detener o retirar pide confirmación en la página. Si hay benchmarks pendientes/en curso, se rechaza el arranque, construcción, parada o retirada. Este bloqueo es conservador; no constituye un bloqueo transaccional compartido entre el worker de benchmarks y Compose. No iniciar benchmarks mientras se modifica el clúster.

Las operaciones largas se ejecutan en un proceso independiente, con logs y estado en `report/control/clusters`. Se permite una operación a la vez. El navegador puede cerrarse sin detenerla. Una caída abrupta del worker puede dejar `operation.lock`; la recuperación es manual después de confirmar que no sigue activo. No hay cancelación ni recuperación automática de builds. No se han ejecutado instalación, rebuild, stop ni down sobre el laboratorio existente durante esta entrega: sus comandos se verificaron con pruebas simuladas. La consulta Compose real sí se comprobó y reconoció el proyecto.

La web necesita los permisos del sistema para Docker, Git y el directorio elegido. Usarla en localhost; no se añade autenticación ni API remota. Los perfiles independientes de destinos de ejecución y la selección del perfil del laboratorio en Experimentos siguen pendientes: el perfil actual gestiona únicamente la instalación/ciclo de vida.

Validación: 18 pruebas Python, incluyendo navegación por las cuatro páginas, ausencia de acciones automáticas, perfiles, construcción de comandos, conservación de volúmenes, operación independiente y bloqueo de operaciones duplicadas. Log local: report/navigation-tests.log.
