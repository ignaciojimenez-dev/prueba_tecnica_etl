# src/writers.py

import logging
import os
from pyspark.sql import DataFrame
from . import utils # Importamos el módulo utils completo

log = logging.getLogger(__name__)

def write_sink(df: DataFrame, sink_config: dict):
    """
    Escribe un DataFrame como una TABLA EXTERNA robusta (Unity Catalog compatible):
    1. Guarda los ficheros de datos con .save()
    2. Registra la tabla en el catálogo con CREATE TABLE IF NOT EXISTS
    """
    try:
        # 1. Extraer la configuración del diccionario
        sink_format = sink_config['format']
        sink_mode = sink_config.get('saveMode', 'overwrite')
        table_name = sink_config['name'] 
        original_path = sink_config.get('path') or sink_config.get('paths')[0]
        
        # Esta función devuelve la ruta /Volumes/... tal cual
        corrected_path = utils.get_absolute_path(original_path) 

        log.info(f"Escribiendo sink (Formato: {sink_format}, Modo: {sink_mode}) como TABLA: '{table_name}' en RUTA: {corrected_path}")

        # --- LÓGICA ROBUSTA ---

        # 2. Escribir los DATOS primero
        #    La API .save() entiende /Volumes/... sin problemas
        writer = df.write.format(sink_format).mode(sink_mode)
        
        if sink_format.upper() == 'DELTA':
            if sink_mode == 'overwrite':
                writer = writer.option("overwriteSchema", "true")
            if sink_mode == 'append':
                writer = writer.option("mergeSchema", "true")
            
        writer.save(corrected_path)
        
        # 3. Asegurar que la TABLA exista en el catálogo (Metastore)
        spark = df.sparkSession

        # --- ¡AQUÍ ESTÁ EL ARREGLO! ---
        # Unity Catalog quiere la ruta /Volumes/... tal cual, sin 'dbfs:'.
        # Así que simplemente usamos la ruta corregida directamente.
        location_path = corrected_path 
        
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING {sink_format.upper()}
            LOCATION '{location_path}'
        """)
        # --- FIN DEL ARREGLO ---
        
        log.info(f"Escritura de tabla '{table_name}' completada.")

    except Exception as e:
        log.error(f"Error inesperado escribiendo la tabla '{table_name}': {e}")
        raise