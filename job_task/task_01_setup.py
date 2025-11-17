# job_tasks/task_01_setup.py
import json
import logging
import sys
import os

#  Añadir raíz del proyecto al path - no leia el modulo src
project_root = os.getcwd() 
if project_root not in sys.path:
    sys.path.append(project_root)

from src import utils
from pyspark.dbutils import DBUtils # type: ignore
from pyspark.sql import SparkSession

# Obtener dbutils solo funciona que solo funciona en databricks # type: ignore
try:
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
except ImportError:
    print("DBUtils no encontrado. Asumiendo ejecución fuera de databricks.")
    dbutils = None

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def main():
    if not dbutils:
        log.error("DBUtils no está disponible. Este script debe ejecutarse como un Job de Databricks.")
        return

    # leemos el parametro de inputs , ruta del volumen en UNIT catalog donde esta el metadatos
    config_path = sys.argv[1]
    log.info(f"Tarea 1: Iniciando. Leyendo metadatos desde {config_path}")

    # Cargar la configuracion del json de metadatos
    config = utils.load_config(config_path)

    # comprobamos que el fichero no esta vacio y hay dataflow

    ################################ añadir mas validaciones al metadatos ################################

    if config and 'dataflows' in config:
        dataflows_list = config['dataflows']
        log.info(f"Metadatos cargados. {len(dataflows_list)} dataflows encontrados.")
        
        # 2. Pasar la lista de de dict's de dataflows a la Tarea 2
        # se convierte  la lista de diccionarios a un string JSON
        dbutils.jobs.taskValues.set(key="dataflows_config_list", value=dataflows_list)
        
        log.info(f"Lista de {len(dataflows_list)} dataflows pasada a la siguiente tarea.")
        
    else:
        log.error("No se pudieron cargar los metadatos o la clave 'dataflows' no existe.")
        raise ValueError("Error al cargar la configuración de dataflows.")

if __name__ == "__main__":
    main()