# src/bronze.py
import logging
from pyspark.sql import SparkSession
from . import readers, writers

log = logging.getLogger(__name__)

def run_bronze_ingestion(spark: SparkSession, dataflow_config: dict) -> dict: # CAMBIO: Añadimos '-> dict'
    """
    Ejecuta la capa Bronce:
    1. Lee todas las 'sources'.
    2. Escribe todas las 'bronze_sinks' usando los DFs leídos.
    3. DEVUELVE los DataFrames leídos para el siguiente paso.
    """
    dataflow_name = dataflow_config.get('name', 'unnamed_dataflow')
    log.info(f"[{dataflow_name}] --- Iniciando Capa Bronze ---")
    
    dataframes_state = {}

    try:
        # --- 1. FASE DE LECTURA
        log.info(f"[{dataflow_name}] Leyendo Sources...")
        for source_config in dataflow_config.get('sources', []):
            source_name = source_config['name']
            log.info(f"[{dataflow_name}] Leyendo: {source_name}")
            df = readers.read_source(spark, source_config)
            dataframes_state[source_name] = df # Guardamos el DF en memoria

        # --- 2. FASE DE ESCRITURA 
        log.info(f"[{dataflow_name}] Escribiendo Bronze Sinks...")
        for sink_config in dataflow_config.get('bronze_sinks', []):
            input_df_name = sink_config['input']
            table_name = sink_config['name']
            
            log.info(f"[{dataflow_name}] Escribiendo: {input_df_name} -> {table_name}")

            if input_df_name not in dataframes_state:
                log.error(f"Error en {table_name}: ...")
                raise ValueError(...)

            df_to_write = dataframes_state[input_df_name]
            writers.write_sink(df_to_write, sink_config)
        
        log.info(f"[{dataflow_name}] --- OK Capa Bronze completada ---")
        
        # se deveulve el estado para que el orquestador pueda usarlo
        return dataframes_state

    except Exception as e:
        log.error(f"--- X ERROR FATAL en Capa Bronze {dataflow_name} ---: {e}", exc_info=True)
        raise