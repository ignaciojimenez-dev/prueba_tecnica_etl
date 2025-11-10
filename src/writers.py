# src/writers.py

import logging
from pyspark.sql import DataFrame
from . import utils # Importamos el módulo utils completo

log = logging.getLogger(__name__)

def write_sink(df: DataFrame, sink_config: dict):
    """
    Escribe un DataFrame como una TABLA EXTERNA usando saveAsTable.
    Este es el método idiomático de Spark y compatible con Unity Catalog.
    """
    try:
        # 1. Extraer la configuración
        sink_format = sink_config['format']
        sink_mode = sink_config.get('saveMode', 'overwrite')
        table_name = sink_config['name'] 
        original_path = sink_config.get('path') or sink_config.get('paths')[0]
        
        # Obtenemos la ruta /Volumes/...
        corrected_path = utils.get_absolute_path(original_path) 

        log.info(f"Escribiendo sink (Formato: {sink_format}, Modo: {sink_mode}) como TABLA: '{table_name}' en RUTA: {corrected_path}")

        # 2. Construir el writer
        writer = df.write.format(sink_format).mode(sink_mode)

        # 3. Añadir opciones de Schema Evolution (importante)
        if sink_format.upper() == 'DELTA':
            if sink_mode == 'overwrite':
                writer = writer.option("overwriteSchema", "true")
            if sink_mode == 'append':
                writer = writer.option("mergeSchema", "true")

        # 4. Ejecutar el guardado con saveAsTable
        #    Esta API (de una sola línea) escribe los datos en 'path' 
        #    Y registra la tabla en Unity Catalog.
        writer.option("path", corrected_path).saveAsTable(table_name)

        log.info(f"Escritura de tabla '{table_name}' completada.")

    except Exception as e:
        log.error(f"Error inesperado escribiendo la tabla '{table_name}': {e}")
        raise