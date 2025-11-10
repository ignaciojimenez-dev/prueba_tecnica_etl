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

        # --- INICIO DE LA SOLUCIÓN ---
        
        # Separamos el path base del "glob" (*)
        # ej: "/ruta/polizas/*" -> "/ruta/polizas"
        base_dir = os.path.dirname(source_path)

        # 1. Comprobamos si el directorio base existe
        if not os.path.isdir(base_dir):
            # Si la carpeta NO existe, no podemos leer.
            log.warning(f"Directorio base no encontrado para el source '{source_config['name']}'. Saltando. Ruta: {base_dir}")
            # Devolvemos un DataFrame vacío
            return spark.createDataFrame([], df.schema if 'df' in locals() else None) # type: ignore

        # 2. Si la carpeta SÍ existe, intentamos leer
        #    (ignoreMissingFiles=true se encargará si está vacía)
        # --- FIN DE LA SOLUCIÓN ---




        log.info(f"Leyendo fuente '{source_config['name']}' ({source_format}) desde: {source_path}")

        # Construimos el reader de Spark
        reader = spark.read.format(source_format)

        reader = reader.option("ignoreMissingFiles", "true")

        if source_format.upper() == 'JSON':
            reader = reader.option("mergeSchema", "true")

        df = reader.load(source_path)
        
        log.info(f"Lectura de '{source_config['name']}' completada.")
        return df
    
    except Exception as e:
        log.error(f"Error inesperado leyendo la fuente '{source_config['name']}': {e}")
        raise