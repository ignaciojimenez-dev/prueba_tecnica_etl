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

def get_absolute_path(path: str) -> str:
    """
    Convierte un path en un path absoluto.
    - Si es un path absoluto de Databricks (/Volumes/...), lo deja.
    - Si es un path relativo (config/metadata.json), lo une a la raíz del proyecto.
    """

    # . Si ya es un path absoluto REAL (para /Volumes/... o /dbfs/...)
    if os.path.isabs(path):
        # En Databricks, /Volumes/... o /dbfs/... es correcto.
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
             log.debug(f"Path ya es absoluto de Databricks: {path}")
             return path
        # En Local, /data/input es un error, lo tratamos como relativo
        else:
             pass # Pasa al paso 3 para ser tratado como relativo

    #  Si es relativo (ej: "data/input") O es "falsamente absoluto"
    #    en local (ej: "/data/input")
    cleaned_path = path.lstrip('/')
    absolute_path = os.path.join(PROJECT_ROOT, cleaned_path)
    log.debug(f"Ruta absoluta (relativa al proyecto) calculada: {absolute_path}")
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