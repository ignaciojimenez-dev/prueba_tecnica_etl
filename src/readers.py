# src/readers.py

import logging
from pyspark.sql import SparkSession, DataFrame
from . import utils
import os

log = logging.getLogger(__name__)

def read_source(spark: SparkSession, source_config: dict) -> DataFrame:
    """
    Lee datos desde una fuente especificada en la configuración.

    :param spark: La SparkSession activa.
    :param source_config: Un diccionario de configuración del source,
                        dado por el metadata.
    :return: Un Spark DataFrame.
    """
    try:
        source_format = source_config['format']
        original_path = source_config['path']

        source_path = utils.get_absolute_path(original_path)

        log.info(f"Leyendo fuente '{source_config['name']}' ({source_format}) desde: {source_path}")

        # Construimos el reader de Spark
        reader = spark.read.format(source_format)

        # para manejar carpetas vacias o faltantes
        reader = reader.option("ignoreMissingFiles", "true")

        if source_format.upper() == 'JSON':
            # mergeSchema es bueno para ingesta de JSON
            reader = reader.option("mergeSchema", "true")

        df = reader.load(source_path)
        
        log.info(f"Lectura de '{source_config['name']}' completada.")
        return df
    
    except Exception as e:
        log.error(f"Error inesperado leyendo la fuente '{source_config['name']}': {e}")
        raise