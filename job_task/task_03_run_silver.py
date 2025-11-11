# job_tasks/task_03_run_silver.py
import json, logging, sys, os
from pyspark.dbutils import DBUtils # type: ignore
from pyspark.sql import SparkSession

# --- Añadir raíz del proyecto al path ---
project_root = os.getcwd() 
if project_root not in sys.path:
    sys.path.append(project_root)
# ----------------------------------------
from src import utils, silver # ¡Importa el módulo silver!

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def main():
    dataflow_json_string = sys.argv[1]
    log.info("Tarea 3 (Run Silver) iniciada.")

    try:
        dataflow_config = json.loads(dataflow_json_string)
        if not dataflow_config:
            raise ValueError("Configuración de dataflow vacía.")
            
        dataflow_name = dataflow_config.get("name")
        log.info(f"Procesando SILVER para dataflow: {dataflow_name}")
        
        spark_session = utils.get_spark_session()
        
        # --- 4. ¡Llamar solo a Silver! ---
        # No pasamos 'initial_state', forzando que lea de Delta
        silver.run_silver_processing(spark_session, dataflow_config) 
        
        log.info(f"Dataflow {dataflow_name} (SILVER) completado.")

    except Exception as e:
        log.error(f"Error fatal durante la ejecución de SILVER: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()