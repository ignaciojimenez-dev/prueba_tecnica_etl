# job_tasks/task_02_run_bronze.py
import json, logging, sys, os
from pyspark.dbutils import DBUtils # type: ignore
from pyspark.sql import SparkSession

# --- Añadir raíz del proyecto al path ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.append(project_root)
# ----------------------------------------
from src import utils, bronze # ¡Importa el módulo bronze!

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def main():
    dbutils.widgets.text("dataflow_config_json", "{}", "Configuración del Dataflow (JSON String)")
    dataflow_json_string = dbutils.widgets.get("dataflow_config_json")
    log.info("Tarea 2 (Run Bronze) iniciada.")

    try:
        dataflow_config = json.loads(dataflow_json_string)
        if not dataflow_config:
            raise ValueError("Configuración de dataflow vacía.")
            
        dataflow_name = dataflow_config.get("name")
        log.info(f"Procesando BRONCE para dataflow: {dataflow_name}")
        
        spark_session = utils.get_spark_session()
        
        # --- 4. ¡Llamar solo a Bronce! ---
        bronze.run_bronze_ingestion(spark_session, dataflow_config)
        
        log.info(f"Dataflow {dataflow_name} (BRONCE) completado.")

    except Exception as e:
        log.error(f"Error fatal durante la ejecución de BRONCE: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()