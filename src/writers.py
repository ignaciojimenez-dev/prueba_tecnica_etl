# src/writers.py

import logging
from pyspark.sql import DataFrame

log = logging.getLogger(__name__)

def write_sink(df: DataFrame, sink_config: dict):
    """
    Escribe un DataFrame como una managed table usando saveAsTable.
    Esto es compatible con Serverless y Unity Catalog sin
    necesidad de configurar 'path'.
    """
    try:
        # 1. Extraer la configuración
        sink_format = sink_config['format']
        sink_mode = sink_config.get('saveMode', 'overwrite')
        table_name = sink_config['name'] 
        
        
        log.info(f"Escribiendo sink (Formato: {sink_format}, Modo: {sink_mode}) como TABLA GESTIONADA: '{table_name}'")

        # 2. Construir el writer
        writer = df.write.format(sink_format).mode(sink_mode)

        # 3. Añadir opciones de Schema Evolution 
        if sink_format.upper() == 'DELTA':
            if sink_mode == 'overwrite':
                writer = writer.option("overwriteSchema", "true")
            if sink_mode == 'append':
                writer = writer.option("mergeSchema", "true")

        # 4. Guardar como TABLA GESTIONADA
        # Simplemente llamamos a saveAsTable con el nombre.
        writer.saveAsTable(table_name)
        
        log.info(f"Escritura de tabla '{table_name}' completada.")

    except Exception as e:
        log.error(f"Error inesperado escribiendo la tabla '{table_name}': {e}")
        raise