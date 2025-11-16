# src/dlt_helpers.py

# se generan el codigo sql para dlt en las valadaciones que pasa a expect
# para transformaciones recibe dataframe y devuelve otro aplicando transformaciones
# modular para añadir validaciones y transforamcions facil si llega en el metadata
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

# --- 1. Generador de Reglas de Validación ---

def _get_dlt_expression(validation_name: str, col_name: str) -> str:
    """
    Mapea un nombre de regla (ej: "notNull") a su expresión SQL de DLT.
    """
    if validation_name == 'notNull':
        return f"{col_name} IS NOT NULL"
    
    elif validation_name == 'notEmpty':
        return f"{col_name} IS NOT NULL AND {col_name} != ''"
    
    # --- Añadir mas reglas aquí ---
    # elif validation_name == 'isEmail':
    #     return f"regexp_like({col_name}, '^[a-zA-Z0-9._%+-]+...')"

    else:
        log.warning(f"Regla DLT desconocida '{validation_name}'. Ignorando.")
        return "1=1" # devuelve sql true para qu eno falle el dlt

def generate_validation_rules(metadata_rules: list) -> dict:
    """
    Convierte la lista de validaciones del metadata en el diccionario
    que DLT espera para @dp.expect_all().
    
    Input: [ {"field": "office", "validations": ["notEmpty"]}, 
             {"field": "age", "validations": ["notNull"]}     ]
    
    Output: {
      "expect_office_notEmpty": "office IS NOT NULL AND office != ''",
      "expect_age_notNull": "age IS NOT NULL"  }
    """
    dlt_rules = {}
    for rule_set in metadata_rules:
        field_name = rule_set['field']
        validations_to_apply = rule_set['validations']
        
        for val_name in validations_to_apply:
            rule_key = f"expect_{field_name}_{val_name}" 
            rule_expression = _get_dlt_expression(val_name, field_name)
            dlt_rules[rule_key] = rule_expression
            
    log.info(f"Generadas {len(dlt_rules)} reglas de DLT.")
    return dlt_rules

# --- 2. Aplicador de Transformaciones ---

def apply_transformations(df: DataFrame, transform_configs: list) -> DataFrame:
    """
    Aplica una lista de transformaciones (ej: add_fields, apply_masking) 
    a un DataFrame.
    """
    temp_df = df
    
    for tx_config in transform_configs:
        # get para generar try sino hay
        tx_type = tx_config.get('type')
        params = tx_config.get('params', {}) 
        
        if tx_type == 'add_fields':
            temp_df = _apply_add_fields(temp_df, params)
        
        # --- masking---
        elif tx_type == 'apply_masking':
            temp_df = _apply_data_masking(temp_df, params)
        # -------------------------
            
        else:
            log.warning(f"Tipo de transformación '{tx_type}' no reconocido. Omitiendo.")
            
    return temp_df

def _apply_add_fields(df: DataFrame, params: dict) -> DataFrame:
    """
    Aplica la transformación 'add_fields' (lógica portada de transformers.py).
    """
    fields_to_add = params.get('addFields', [])
    temp_df = df
    
    for field in fields_to_add:
        col_name = field['name']
        col_function = field['function']

        if col_function == 'current_timestamp':
            temp_df = temp_df.withColumn(col_name, F.current_timestamp())
        elif col_function == 'current_date':
            temp_df = temp_df.withColumn(col_name, F.current_date())
        # --- Añadir más funciones de Spark aquí ---
        else:
            log.warning(f"Función '{col_function}' no reconocida para '{col_name}'.")

    return temp_df


def _apply_data_masking(df: DataFrame, params: dict) -> DataFrame:
    """
    Aplica enmascaramiento de datos simple (SHA2, MD5) a las columnas.
    """
    rules = params.get('masking_rules', [])
    temp_df = df
    
    for rule in rules:
        field = rule['field']
        func = rule['function']
        
        # Comprobamos que la columna existe antes de intentar enmascararla
        if field in temp_df.columns:
            if func == 'sha2':
                log.info(f"Aplicando máscara SHA2 al campo '{field}'")
                temp_df = temp_df.withColumn(field, F.sha2(F.col(field).cast("string"), 256))
            
            elif func == 'md5':
                log.info(f"Aplicando máscara MD5 al campo '{field}'")
                temp_df = temp_df.withColumn(field, F.md5(F.col(field).cast("string")))
            
            # --- Añade más funciones de masking aquí ---
            # elif func == 'redact':
            #    temp_df = temp_df.withColumn(field, F.lit("REDACTED"))
            
            else:
                log.warning(f"Función de masking '{func}' no reconocida para '{field}'.")
        else:
            log.warning(f"Campo '{field}' no encontrado en el DataFrame. Omitiendo masking.")
            
    return temp_df