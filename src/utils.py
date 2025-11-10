# src/utils.py

import os
import json
import logging
from pyspark.sql import SparkSession
from typing import Union, Optional

log = logging.getLogger(__name__)


def get_project_root() -> str:
    """
    Obtiene la ruta raíz absoluta del proyecto, tanto en local 
    como en Databricks Repos.
    """
    # Si estamos en Databricks Repos, os.getcwd() da la raíz del repo
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        project_root = os.getcwd()
        log.debug(f"Raíz del proyecto (Databricks Repos): {project_root}")
        return project_root
    
    # Si estamos en Local, usamos __file__ para encontrar la raíz
    else:
        # __file__ -> /ruta/proyecto/src/utils.py
        utils_py_path = os.path.abspath(__file__)
        # dirname -> /ruta/proyecto/src
        src_dir = os.path.dirname(utils_py_path)
        # dirname -> /ruta/proyecto
        project_root = os.path.dirname(src_dir)
        log.debug(f"Raíz del proyecto (Local): {project_root}")
        return project_root

# Obtenemos la raíz  al importar el módulo
PROJECT_ROOT = get_project_root()

# --- 2. FUNCIÓN DE RUTA CORREGIDA (¡AQUÍ ESTÁ EL ARREGLO!) ---

def get_absolute_path(relative_path: str) -> str:
    """
    Convierte un path (relativo o "falsamente absoluto")
    en un path absoluto para el entorno (local o Databricks).
    """
    # 1. Si es un URI (s3a://, dbfs://, etc.), lo dejamos
    if ":" in relative_path:
        return relative_path
    
    
    # lstrip('/') elimina la barra inicial de "/data/input"
    # Y no hace nada si es "data/input"
    cleaned_path = relative_path.lstrip('/')
    
    # Unimos la raíz del proyecto (calculada en PROJECT_ROOT) 
    # con el path limpio.
    absolute_path = os.path.join(PROJECT_ROOT, cleaned_path)
    
    log.debug(f"Ruta absoluta calculada: {absolute_path}")
    return absolute_path

# --- 3. FUNCIONES ANTIGUAS (QUE USAN LA FUNCIÓN CORREGIDA) ---

def get_spark_session() -> SparkSession:
    """
    Crea u obtiene una SparkSession.
    """
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        log.info("Entorno Databricks detectado. Obteniendo sesión 'spark' existente.")
        return SparkSession.builder.getOrCreate()
    
    log.info("Entorno Local detectado. Creando nueva SparkSession local con Delta.")
    
    warehouse_path = get_absolute_path("spark-warehouse")
    log.info(f"Usando warehouse local en: {warehouse_path}")

    return (
        SparkSession.builder.appName("LocalETLFramework")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def load_config(path: str) -> Optional[dict]:
    """
    Carga un fichero de configuración JSON desde una ruta relativa.
    """
    config_path = get_absolute_path(path)
    
    log.info(f"Cargando configuración desde: {config_path}")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        log.info("Configuración cargada exitosamente.")
        return config
    except Exception as e:
        log.error(f"Error inesperado al cargar {config_path}: {e}")
        return None