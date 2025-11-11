# job_tasks/task_01_setup.py
import json
import logging
import sys
import os

# --- Añadir raíz del proyecto al path ---
project_root = os.getcwd() 
if project_root not in sys.path:
    sys.path.append(project_root)
# ----------------------------------------

from src import utils
from pyspark.dbutils import DBUtils # type: ignore
from pyspark.sql import SparkSession

# Obtener dbutils solo funciona en Databricks
try:
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
except ImportError:
    print("DBUtils no encontrado. Asumiendo ejecución fuera de Databricks (no funcionará).")
    dbutils = None

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def main():
    if not dbutils:
        log.error("DBUtils no está disponible. Este script debe ejecutarse como un Job de Databricks.")
        return

    # --- Widgets ---
    dbutils.widgets.text("config_path", "/Volumes/workspace/prueba_tecnica/config/metadata.json", "Ruta al metadata.json")
    config_path = dbutils.widgets.get("config_path")

    log.info(f"Tarea 1: Iniciando. Leyendo metadatos desde {config_path}")

    # 1. Cargar la configuración
    config = utils.load_config(config_path)

    if config and 'dataflows' in config:
        dataflows_list = config['dataflows']
        log.info(f"Metadatos cargados. {len(dataflows_list)} dataflows encontrados.")
        
        # 2. Pasar la lista de dataflows a la Tarea 2 (ForEach)
        # Convertimos la lista de diccionarios a un string JSON
        # NOTA: Databricks Jobs prefiere pasar listas de valores simples.
        # Es mejor pasar la lista de diccionarios directamente.
        dbutils.jobs.taskValues.set(key="dataflows_config_list", value=dataflows_list)
        
        log.info(f"Lista de {len(dataflows_list)} dataflows pasada a la siguiente tarea.")
        
    else:
        log.error("No se pudieron cargar los metadatos o la clave 'dataflows' no existe.")
        raise ValueError("Error al cargar la configuración de dataflows.")

if __name__ == "__main__":
    main()