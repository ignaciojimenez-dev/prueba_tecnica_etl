# src/orchestrator.py
import logging
from pyspark.sql import SparkSession
# ¡Importamos los nuevos módulos!
from . import silver, utils, readers, writers # Asegúrate de importar readers y writers

log = logging.getLogger(__name__)

def run_single_dataflow(spark: SparkSession, dataflow_config: dict):
    """
    Ejecuta un dataflow COMPLETO.
    """
    dataflow_name = dataflow_config.get('name', 'unnamed_dataflow')
    log.info(f" Iniciando Dataflow (Modo Local Integrado): {dataflow_name} ")

    try:
        dataframes_state = {}
        
        # --- FASE 1: BRONCE (Lógica corregida) ---
        log.info(f"[{dataflow_name}] Fase 1: Leyendo Sources...")
        
        # 1.A. Leer todos los sources a memoria
        for source_config in dataflow_config.get('sources', []):
            source_name = source_config['name']
            df = readers.read_source(spark, source_config)
            dataframes_state[source_name] = df # ¡Guardamos en estado!
            
        log.info(f"[{dataflow_name}] Fase 1: ...y Guardando en Bronce (Modo Local)...")
        
        # 1.B. Escribir en Bronze Sinks (usando la metadata)
        # Esto replica la lógica de bronze.py
        for sink_config in dataflow_config.get('bronze_sinks', []):
            input_df_name = sink_config['input']
            table_name = sink_config['name']
            
            log.info(f"[{dataflow_name}] Escribiendo (Local): {input_df_name} -> {table_name}")
            
            if input_df_name in dataframes_state:
                df_to_write = dataframes_state[input_df_name]
                # Usamos el writer real
                writers.write_sink(df_to_write, sink_config)
            else:
                log.warning(f"No se encontró el DF {input_df_name} en memoria para el sink local {table_name}")


        # --- FASE 2: SILVER (Lógica de orchestrator.py) ---
        log.info(f"[{dataflow_name}] Fases 2 y 3: Transformando y Guardando en Silver...")
        
        # ¡Llamamos a silver pasándole el estado que acabamos de crear!
        # silver.py usará este estado en lugar de leer de las tablas Bronce.
        silver.run_silver_processing(spark, dataflow_config, 
                                     initial_state=dataframes_state)
            
        log.info(f"--- OK Dataflow {dataflow_name} (Modo Local) completado ---")

    except Exception as e:
        log.error(f"--- X ERROR FATAL en Dataflow {dataflow_name} ---: {e}", exc_info=True)
        raise