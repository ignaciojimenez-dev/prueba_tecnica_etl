# src/bronze.py
import logging
from pyspark.sql import SparkSession
from . import readers, writers

log = logging.getLogger(__name__)

def run_bronze_ingestion(spark: SparkSession, dataflow_config: dict):
    """
    Ejecuta la capa Bronce:
    1. Lee todas las 'sources'.
    2. Escribe todas las 'bronze_sinks' usando los DFs leídos.
    """
    dataflow_name = dataflow_config.get('name', 'unnamed_dataflow')
    log.info(f"[{dataflow_name}] --- Iniciando Capa Bronze ---")
    
    # Este diccionario guarda los DFs leídos (ej: 'person_inputs')
    dataframes_state = {}

    try:
        # --- 1. FASE DE LECTURA (Extract) ---
        log.info(f"[{dataflow_name}] Leyendo Sources...")
        for source_config in dataflow_config.get('sources', []):
            source_name = source_config['name']
            log.info(f"[{dataflow_name}] Leyendo: {source_name}")
            df = readers.read_source(spark, source_config)
            dataframes_state[source_name] = df # Guardamos el DF en memoria

        # --- 2. FASE DE ESCRITURA (Load Bronze) ---
        log.info(f"[{dataflow_name}] Escribiendo Bronze Sinks...")
        for sink_config in dataflow_config.get('bronze_sinks', []):
            input_df_name = sink_config['input'] # ej: "person_inputs"
            table_name = sink_config['name']     # ej: "workspace.etl_modular.bronze_person"
            
            log.info(f"[{dataflow_name}] Escribiendo: {input_df_name} -> {table_name}")

            # Buscamos el DF que leímos en la Fase 1
            if input_df_name not in dataframes_state:
                log.error(f"Error en {table_name}: El DataFrame de entrada '{input_df_name}' no existe.")
                raise ValueError(f"Input DataFrame '{input_df_name}' no encontrado para el sink '{table_name}'")

            df_to_write = dataframes_state[input_df_name]
            
            # Llamamos al escritor
            writers.write_sink(df_to_write, sink_config)
        
        log.info(f"[{dataflow_name}] --- OK Capa Bronze completada ---")

    except Exception as e:
        log.error(f"--- X ERROR FATAL en Capa Bronze {dataflow_name} ---: {e}", exc_info=True)
        raise