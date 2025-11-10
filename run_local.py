import logging
from pyspark.sql import SparkSession

# modulos propios
from src import utils
from src import orchestrator

# Configuracion logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


CONFIG_PATH = "config/metadata.json"

def main():
    """
    Punto de entrada para la ejecución local.
    Simula el job de Databricks:
    1. Carga la config.
    2. Obtiene una sesión Spark local.
    3. Ejecuta CADA dataflow, uno por uno.
    """
    log.info(f"--- Iniciando Ejecución Local ---")
    log.info(f"Cargando metadatos desde: {CONFIG_PATH}")
    
    config = utils.load_config(CONFIG_PATH)
    if not config or 'dataflows' not in config:
        log.error("No se pudo cargar la configuración o 'dataflows' no encontrado.")
        return

    spark = None
    try:
        # crear la sesión local con Delta
        spark = utils.get_spark_session()
        log.info(f"SparkSession local creada (Versión {spark.version})")
        
        dataflows_list = config['dataflows']
        log.info(f"Se encontraron {len(dataflows_list)} dataflows para procesar.")
        
        # --- Simulación del "For-Each" de Databricks ---
        # En local, los ejecutamos en serie (uno después de otro)
        for i, dataflow_cfg in enumerate(dataflows_list, 1):
            log.info(f"--- Procesando Dataflow {i}/{len(dataflows_list)}: {dataflow_cfg['name']} ---")
            
            # Llamamos a nuestro orquestador con la config de UN solo dataflow
            orchestrator.run_single_dataflow(spark, dataflow_cfg)
        
        log.info("--- Ejecución Local Completada Exitosamente ---")

    except Exception as e:
        log.error(f"Error fatal durante la ejecución local: {e}", exc_info=True)
    finally:
        if spark:
            log.info("Deteniendo SparkSession local.")
            spark.stop()

if __name__ == "__main__":
    main() 