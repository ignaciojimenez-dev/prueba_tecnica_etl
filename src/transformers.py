# src/transformers.py

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from . import validations

log = logging.getLogger(__name__)

# ---  Funciones de Transformación Modulares ---
# Cada función maneja un tipo específico de transformación.

def _apply_validate_fields(df: DataFrame, params: dict) -> dict:
    """
    Aplica la validación de campos y devuelve un diccionario 
    con los DataFrames 'ok' y 'ko'.
    """
    validation_rules = params['validations']
    
    # Llamamos a la función modular que creamos en validations.py
    df_ok, df_ko = validations.validate_fields(df, validation_rules)
    
    # Devolvemos un diccionario para que la función principal sepa qué guardar
    return {"ok": df_ok, "ko": df_ko}


def _apply_add_fields(df: DataFrame, params: dict) -> DataFrame:
    """
    Aplica la transformación 'add_fields' .
    """
    fields_to_add = params.get('addFields', [])
    
    temp_df = df
    for field in fields_to_add:
        col_name = field['name']
        col_function = field['function']

        log.info(f"Añadiendo columna '{col_name}' usando la función '{col_function}'")

        # --- Mapeo de funciones de Spark ---
        # Este 'if/elif' es un "dispatcher" simple para funciones de Spark
        
        if col_function == 'current_timestamp':
            temp_df = temp_df.withColumn(col_name, F.current_timestamp())
        
        # --- Aquí puedes añadir más funciones ---
        # elif col_function == 'current_date':
        #     temp_df = temp_df.withColumn(col_name, F.current_date())
        # elif col_function.startswith('lit('):
        #     # Para añadir literales, ej: "function": "lit(Hola)"
        #     value = col_function[4:-1]
        #     temp_df = temp_df.withColumn(col_name, F.lit(value))
            
        else:
            log.warning(f"Función '{col_function}' no reconocida para '{col_name}'. Columna no añadida.")

    return temp_df


#  Funcion Principal

def apply_transform(spark: SparkSession, dataframes_state: dict, transform_config: dict):
    """
    Función principal que lee una configuración de transformación,
    llama a la función modular correcta y actualiza el 'dataframes_state'.

    :param spark: La SparkSession activa.
    :param dataframes_state: El diccionario que contiene todos los DataFrames
    :param transform_config: La configuración para UNA transformación
    """
    
    transform_name = transform_config['name']
    transform_type = transform_config['type']
    params = transform_config['params']
    
    # Obtenemos el DataFrame de entrada desde el estado
    input_df_name = params['input']
    if input_df_name not in dataframes_state:
        log.error(f"Error en '{transform_name}': DataFrame de entrada '{input_df_name}' no encontrado.")
        raise ValueError(f"DataFrame de entrada no encontrado: {input_df_name}")
        
    input_df = dataframes_state[input_df_name]
    
    log.info(f"Aplicando transformación: '{transform_name}' (Tipo: {transform_type})")

    # --- El Despachador (Dispatcher) ---
    # Decide qué función llamar basado en el 'type'
    
    if transform_type == 'validate_fields':
        # Esta transformación es especial: devuelve dos DFs (ok, ko)
        result_dfs = _apply_validate_fields(input_df, params)
        
        # Guardamos los resultados en el estado
        # Los nombres se basan en tu metadata: "validation_ok", "validation_ko"
        dataframes_state[transform_name + '_ok'] = result_dfs['ok']
        dataframes_state[transform_name + '_ko'] = result_dfs['ko']
        
        log.info(f"Guardados DataFrames: {transform_name}_ok y {transform_name}_ko")

    elif transform_type == 'add_fields':
        # Esta transformación devuelve un solo DF
        result_df = _apply_add_fields(input_df, params)
        
        # Guardamos el resultado en el estado
        # El nombre se basa en tu metadata: "ok_with_date"
        dataframes_state[transform_name] = result_df
        log.info(f"Guardado DataFrame: {transform_name}")

    else:
        log.warning(f"Tipo de transformación '{transform_type}' no reconocido. Omitiendo.")