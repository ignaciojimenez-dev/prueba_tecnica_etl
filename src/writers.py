# src/writers.py

import logging
from pyspark.sql import DataFrame
from . import utils

log = logging.getLogger(__name__)

def write_sink(df: DataFrame, sink_config: dict):
    """
    Escribe un DataFrame como una TABLA EXTERNA robusta:
    1. Guarda los ficheros de datos con .save()
    2. Registra la tabla en el catálogo con CREATE TABLE IF NOT EXISTS
    """
    try:
        # 1. Extraer la configuración del diccionario
        sink_format = sink_config['format']
        sink_mode = sink_config.get('saveMode', 'overwrite')
        
        # El 'name' del sink es el nombre de la tabla
        table_name = sink_config['name'] 
        
        # El 'path' es la ubicación de los ficheros
        original_path = sink_config.get('path') or sink_config.get('paths')[0]
        
        # Corregir el path para que funcione en local
        corrected_path = utils.get_absolute_path(original_path)

        log.info(f"Escribiendo sink (Formato: {sink_format}, Modo: {sink_mode}) como TABLA: '{table_name}' en RUTA: {corrected_path}")

        # --- INICIO DE LA LÓGICA ROBUSTA Y ÚNICA ---

        # 2. Escribir los DATOS primero
        writer = df.write.format(sink_format).mode(sink_mode)
        
        # Añadir opciones de Schema Evolution SOLO si es Delta
        if sink_format.upper() == 'DELTA':
            if sink_mode == 'overwrite':
                writer = writer.option("overwriteSchema", "true")
            
            if sink_mode == 'append':
                writer = writer.option("mergeSchema", "true")
            
        writer.save(corrected_path)
        
        # 3. Asegurar que la TABLA exista en el catálogo (Metastore)
        #    Esto es "idempotente": si ya existe, no hace nada.
        spark = df.sparkSession
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING {sink_format.upper()}
            LOCATION '{corrected_path}'
        """)
        
        # --- FIN DE LA LÓGICA ROBUSTA Y ÚNICA ---
        
        log.info(f"Escritura de tabla '{table_name}' completada.")

    except Exception as e:
        log.error(f"Error inesperado escribiendo la tabla '{table_name}': {e}")
        raise