# src/orchestrator.py

import logging
from pyspark.sql import SparkSession

# modulos propios
from . import readers
from . import writers
from . import transformers

log = logging.getLogger(__name__)

def run_single_dataflow(spark: SparkSession, dataflow_config: dict):
    """
    Ejecuta un único dataflow de principio a fin.
    Esta es la función principal que será llamada por la Tarea 2 de Databricks.
    
    1. Lee todas las fuentes (Sources)
    2. Aplica todas las transformaciones (Transformations)
    3. Escribe todos los destinos (Sinks)
    """
    
    dataflow_name = dataflow_config.get('name', 'unnamed_dataflow')
    log.info(f" Iniciando Dataflow: {dataflow_name} ")
    
    # Este diccionario guarda en memoria los DataFrames generados
    dataframes_state = {}

    try:
        # --- 1. FASE DE LECTURA (Extract) ---
        log.info(f"[{dataflow_name}] Fase 1: Leyendo Sources...")
        for source_config in dataflow_config.get('sources', []):
            source_name = source_config['name']
            
            log.info(f"[{dataflow_name}] Leyendo: {source_name}")
            df = readers.read_source(spark, source_config)
            
            # Guardamos el DataFrame en nuestro "estado"
            dataframes_state[source_name] = df


        # --- 1.B. Construir el config de Bronce (Lógica del Orquestador) ---
            bronze_table_name = f"workspace.prueba_tecnica.bronze_{source_name}"
            #bronze_path = f"data/bronze/{source_name}" # Ruta base de Bronce local
            bronze_path = f"/Volumes/workspace/prueba_tecnica/bronze/{source_name}"
            bronze_sink_config = {
                "name": bronze_table_name,
                "path": bronze_path,
                "format": "DELTA",
                "saveMode": "overwrite" 
            }
        # --- 1.C. Llamar al writer para guardar en Bronce ---
            log.info(f"[{dataflow_name}] Guardando {source_name} en tabla Bronce: {bronze_table_name}")
            writers.write_sink(df, bronze_sink_config)
        
        # --- 2. FASE DE TRANSFORMACIÓN (Transform) ---
        log.info(f"[{dataflow_name}] Fase 2: Aplicando Transformaciones...")
        for tx_config in dataflow_config.get('transformations', []):
            
            # Llamamos a nuestro "factory" de transformaciones
            # Esta función MODIFICA el 'dataframes_state' internamente
            # (añadiendo 'validation_ok', 'validation_ko', 'ok_with_date', etc.)
            transformers.apply_transform(spark, dataframes_state, tx_config)


        # --- 3. FASE DE ESCRITURA (Load) ---
        log.info(f"[{dataflow_name}] Fase 3: Escribiendo Sinks...")
        for sink_config in dataflow_config.get('sinks', []):
            sink_name = sink_config['name']
            
            # Obtenemos el nombre del DataFrame que este sink necesita
            input_df_name = sink_config['input']
            
            log.info(f"[{dataflow_name}] Escribiendo: {sink_name} (usa {input_df_name})")

            # Buscamos el DataFrame en nuestro "estado"
            if input_df_name not in dataframes_state:
                log.error(f"Error en {sink_name}: El DataFrame de entrada '{input_df_name}' no existe.")
                raise ValueError(f"Input DataFrame '{input_df_name}' no encontrado para el sink '{sink_name}'")

            df_to_write = dataframes_state[input_df_name]
            
            # Llamamos a nuestro escritor genérico
            writers.write_sink(df_to_write, sink_config)
            
        log.info(f"--- OK Dataflow {dataflow_name} completado exitosamente ---")

    except Exception as e:
        log.error(f"--- X ERROR FATAL en Dataflow {dataflow_name} ---: {e}", exc_info=True)
        raise