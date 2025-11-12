# job_tasks/task_02_process_dataflow.py
import json
import logging
import sys
import os

# --- Añadir raíz del proyecto al path ---
# (Usamos os.getcwd() como corregimos para los Jobs)
project_root = os.getcwd()
if project_root not in sys.path:
    sys.path.append(project_root)
# ----------------------------------------

# ¡Importamos el orchestrator, que ya tiene la lógica de Bronce+Plata!
from src import utils, orchestrator 
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def main():
    log.info("Tarea 2  Process single Dataflow iniciada.")

    try:
        # --- 1. Leer el argumento (el dataflow config) 
        # sirve para iterar sobre el parametro input: {{tasks.task_01_setup.values.dataflows_config_list}}
        # definido en la tarea 1, recibe dataflow1 , luego 2 etc
        dataflow_json_string = sys.argv[1]
        
        dataflow_config = json.loads(dataflow_json_string)
        
        # raiseo a la task en databricks para que falle si no hay dataflows en el metadatos
        if not dataflow_config or 'name' not in dataflow_config:
            raise ValueError("Configuración de dataflow vacía o inválida recibida.")
            
        dataflow_name = dataflow_config.get("name")
        log.info(f"Procesando dataflow bronze mas plata oro?¿: {dataflow_name}")

        # --- 2 Obtener Spark ---
        spark_session = utils.get_spark_session()
        
        # --- 3 Llamar al Orquestador para cada elemento del array que recibe

        orchestrator.run_single_dataflow(spark_session, dataflow_config)
        
        log.info(f"Dataflow {dataflow_name}  completado.")

    except Exception as e:
        log.error(f"Error fatal durante la ejecución del dataflow {dataflow_name}: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()