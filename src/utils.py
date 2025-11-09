# src/utils.py

import os
import json
import logging
from pyspark.sql import SparkSession
from typing import Union

log = logging.getLogger(__name__)

def get_spark_session() -> SparkSession:
    """
    Crea una SparkSession, detectando si está en local o Databricks.
    """
    # Si detecta que está en Databricks
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        log.info("Entorno Databricks detectado. Obteniendo sesión 'spark' existente.")
        # La sesión que Databricks ya ha creado.
        return SparkSession.builder.getOrCreate()
    
    # Si no, está en Local 
    log.info("Entorno Local detectado. Creando nueva SparkSession local con Delta.")
    
    return (
        SparkSession.builder.appName("PruebaTecnicaLocal")
        .master("local[*]")  
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def load_config(path: str) -> Union[dict, None]:
    """
    Carga un fichero de configuración JSON desde la ruta especificada.
    
    :param path: Ruta al fichero config.json
    :return: Un diccionario con la configuración o None si falla.
    """
    log.info(f"Cargando configuración desde: {path}")
    try:
        
        with open(path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        log.info("Configuración cargada exitosamente.")
        return config
    except Exception as e:
        log.error(f"Error inesperado al cargar {path}: {e}")
        return None
    
def get_project_root() -> str:
    """
    Obtiene la ruta raíz absoluta del proyecto.
    Asume que este fichero está en /ruta/proyecto/src/utils.py
    """
    # __file__ es la ruta a este mismo fichero (utils.py)
    # ej: /home/materials/PruebaTecnica/src/utils.py
    utils_py_path = os.path.abspath(__file__)
    
    # os.path.dirname() quita un nivel
    # ej: /home/materials/PruebaTecnica/src
    src_dir = os.path.dirname(utils_py_path)
    
    # os.path.dirname() quita otro nivel
    # ej: /home/materials/PruebaTecnica
    project_root = os.path.dirname(src_dir)
    
    return project_root

def correct_path_for_local(path: str) -> str:
    """
    Corrige un path local para que sea absoluto desde la raíz del proyecto.
    No hace nada en Databricks.
    """
    # 1. Si estamos en Databricks, el path tal cual.
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return path
        

    #  Si es un path local (relativo o absoluto del proyecto)
    project_root = get_project_root()
    
    # lstrip('/') elimina la barra inicial de "/data/input"
    cleaned_path = path.lstrip('/')
    
    # Unimos la raíz del proyecto con el path limpio
    absolute_path = os.path.join(project_root, cleaned_path)
    
    log.debug(f"Ruta local absoluta calculada: {absolute_path}")
    return absolute_path