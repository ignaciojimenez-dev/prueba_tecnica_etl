# src/orchestrator.py
import logging
from pyspark.sql import SparkSession
# ¡Importamos los módulos necesarios!
from . import bronze, silver, utils 

log = logging.getLogger(__name__)

def run_single_dataflow(spark: SparkSession, dataflow_config: dict):
    """
    Ejecuta un dataflow completo llamando a los módulos 
    de Bronce y Plata en secuencia. 
    Se lanza para cada valor del array for-each que se le pasa por parametro
    """
    dataflow_name = dataflow_config.get('name', 'default_unnamed_dataflow')
    log.info(f" Iniciando Dataflow (Modo Integrado): {dataflow_name} ")

    try:
        # --- FASE 1: BRONCE 
        # llamamos al módulo de Bronce
        # lee todas las 'sources', Escribe todas las 'bronze_sinks'
        # devuelve los DataFrames leídos para el siguiente paso
        #escribe en tantas sink bronze como se especifique
        # EL INPUT DEL SINK ES EL NOMBRE DE LA TABLA
        log.info(f"[{dataflow_name}] Fase 1: Ejecutando 'bronze.run_bronze_ingestion'...")
        dataframes_state = bronze.run_bronze_ingestion(spark, dataflow_config)
        

        # --- FASE 2: SILVER ---
        log.info(f"[{dataflow_name}] Fase 2 ejecutando 'silver.run_silver_processing'...")
        
        # llamada a silver con los dataframe en memoria
        # llamamos al módulo de Plata
        # recibe los DataFrames de Bronce en memoria, desde 'initial_state'.
        # si no los recibe, los lee de las tablas Bronce ej: 'bronze_person'.
        # aplica la lógica de 'transformations', primero valida y guarda dataframe por cada validacion , ok y ok.
        # se pueden aplicar tantas validaciones como se quiera en el metadata, esta hecho modular para que
        # cueste poco añadir mas validaciones a la misma tabla
        # llega por input una tabla , con todas la validaciones que se quiera , al final produce un ko y ok 
        # luego si hay la tranformacion add_field , busca su input (persona ok), y devuelve persona ok with date
        # escribe los DataFrames resultantesen las 'silver_sinks'.
        silver.run_silver_processing(spark, dataflow_config, 
                                     initial_state=dataframes_state)
            
        
        # Fase 3: Golden layer, añadir DataMasking
        
        log.info(f"--- OK Dataflow {dataflow_name} (Modo Integrado) completado ---")

    except Exception as e:
        log.error(f"--- X ERROR FATAL en el dataflow {dataflow_name} ---: {e}", exc_info=True)
        raise