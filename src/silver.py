# src/silver.py
import logging
from pyspark.sql import SparkSession
from . import writers, transformers

log = logging.getLogger(__name__)

def _get_bronze_table_for_input(dataflow_config: dict, input_name: str) -> str:
    """
    Función helper: Busca en 'bronze_sinks' qué tabla 
    corresponde a un input (ej: 'person_inputs').
    """
    for sink in dataflow_config.get('bronze_sinks', []):
        if sink['input'] == input_name:
            return sink['name'] # ej: "workspace.etl_modular.bronze_person"
    
    
    return None 

def run_silver_processing(spark: SparkSession, dataflow_config: dict, 
                          initial_state: dict = None):
    """
    Ejecuta la parte de Silver y Sinks.
    
    Si 'initial_state' es None , lee desde las tablas Bronce.
    Si 'initial_state' se proporciona lo usa.
    """
    dataflow_name = dataflow_config.get('name', 'unnamed_dataflow')
    log.info(f"[{dataflow_name}] --- Iniciando Capa Silver ---")

    dataframes_state = {}
    if initial_state:
        log.info(f"[{dataflow_name}] Usando 'initial_state' (modo local).")
        dataframes_state = initial_state
    
    try:
        # --- 1. FASE DE TRANSFORMACIÓN ---
        
        log.info(f"[{dataflow_name}] Fase 2: Aplicando Transformaciones...")

        #itera por las transforamciones, recibe de inputs las tablas donde validar
        # dos transforamciones en este caso para una tabla , pero podria ser mas
        #primero las validaciones para una tabla,
        for tx_config in dataflow_config.get('transformations', []):
            params = tx_config['params']
            input_df_name = params['input'] # tabla y dataframe "person_inputs"
            
            # Si el DF de entrada (ej: 'person_inputs') NO está en memoria...
            if input_df_name not in dataframes_state:
                #  leer de la tabla Bronce.
                
                # 1. Buscamos qué tabla Bronce le corresponde
                bronze_table = _get_bronze_table_for_input(dataflow_config, input_df_name)
                
                if not bronze_table:
                     log.error(f"No se encontró ni DF en memoria ni tabla Bronce para el input '{input_df_name}'")
                     raise ValueError(f"Input '{input_df_name}' no resuelto.")
                
                # 2. Leemos la tabla Bronce
                log.info(f"[{dataflow_name}] Leyendo desde tabla Bronce: {bronze_table} (para el input '{input_df_name}')")
                df = spark.read.table(bronze_table)
                
                # 3. La guardamos en el estado para que 'apply_transform' la use
                dataframes_state[input_df_name] = df
            
            # Ahora el DF (ya sea de 'initial_state' o de la tabla Bronce) existe
            # y podemos aplicar la transformación.
            transformers.apply_transform(spark, dataframes_state, tx_config)


        # --- 2. FASE DE ESCRITURA  Silver ---
        log.info(f"[{dataflow_name}] Fase 3: Escribiendo Silver Sinks...")
        # lee 'silver_sinks'
        for sink_config in dataflow_config.get('silver_sinks', []):
            input_df_name = sink_config['input'] 
            table_name = sink_config['name']    
            
            log.info(f"[{dataflow_name}] Escribiendo: {input_df_name} -> {table_name}")

            if input_df_name not in dataframes_state:
                log.error(f"Error en {table_name}: El DataFrame de entrada '{input_df_name}' no existe.")
                raise ValueError(f"Input DataFrame '{input_df_name}' no encontrado para el sink '{table_name}'")

            df_to_write = dataframes_state[input_df_name]
            writers.write_sink(df_to_write, sink_config)
            
        log.info(f"[{dataflow_name}] --- OK Capa Silver completada ---")

    except Exception as e:
        log.error(f"--- ERROR FATAL en Capa Silver {dataflow_name} ---: {e}", exc_info=True)
        raise