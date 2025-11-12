# src/orchestrator.py
import logging
from pyspark.sql import SparkSession
# ¡Importamos los módulos necesarios!
from . import bronze, silver, utils 

log = logging.getLogger(__name__)

def run_single_dataflow(spark: SparkSession, dataflow_config: dict):
    """
    Ejecuta un dataflow COMPLETO llamando a los módulos 
    de Bronce y Plata en secuencia.
    """
    dataflow_name = dataflow_config.get('name', 'default_unnamed_dataflow')
    log.info(f" Iniciando Dataflow (Modo Integrado): {dataflow_name} ")

    try:
        # --- FASE 1: BRONCE 
        # llamamos al módulo de Bronce
        # lee todas las 'sources', Escribe todas las 'bronze_sinks'
        # devuelve los DataFrames leídos para el siguiente paso
        log.info(f"[{dataflow_name}] Fase 1: Ejecutando 'bronze.run_bronze_ingestion'...")
        dataframes_state = bronze.run_bronze_ingestion(spark, dataflow_config)
        

        # --- FASE 2: SILVER ---
        log.info(f"[{dataflow_name}] Fases 2 y 3: Ejecutando 'silver.run_silver_processing'...")
        
        # llamada a silver con los dataframe en memoria
        silver.run_silver_processing(spark, dataflow_config, 
                                     initial_state=dataframes_state)
            
        log.info(f"--- OK Dataflow {dataflow_name} (Modo Integrado) completado ---")

    except Exception as e:
        log.error(f"--- X ERROR FATAL en el dataflow {dataflow_name} ---: {e}", exc_info=True)
        raise