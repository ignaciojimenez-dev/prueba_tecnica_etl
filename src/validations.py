# src/validations.py

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple

log = logging.getLogger(__name__)

# --- 1. Funciones de Validación Modulares ---
# Recibe el nombre de la columna y devuelve una EXPRESIÓN de Spark.
# La expresión devuelve un código de error (ej: "notNull") si falla,
# o 'None' (nulo) si la validación pasa.

def _validate_not_null(col_name: str) -> F.Column:
    """
    Devuelve una expresión que comprueba si la columna es nula.
    """
    return F.when(
        F.col(col_name).isNull(),
        F.lit("notNull")  
    ).otherwise(None)     


def _validate_not_empty(col_name: str) -> F.Column:
    """
    Devuelve una expresión que comprueba si la columna está vacía o es nula.
    """
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == ""),
        F.lit("notEmpty") 
    ).otherwise(None)     


# --- Aquí puedes añadir fácilmente más reglas ---
def _validate_is_email(col_name: str) -> F.Column:
     """
     Devuelve una expresión que comprueba un patrón de email.
     """
     email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
     return F.when(
         ~F.col(col_name).rlike(email_regex), 
         F.lit("invalidEmailFormat")
     ).otherwise(None)


# Esta funcion mapea el nombre de la regla a la
# función de validación modular correspondiente.

def _get_validation_expression(validation_name: str, col_name: str) -> F.Column:
    """
    Mapea un nombre de regla (ej: "notNull") a su función de validación.
    """
    if validation_name == 'notNull':
        return _validate_not_null(col_name)
    
    elif validation_name == 'notEmpty':
        return _validate_not_empty(col_name)
    
    #  Añade aquí las nuevas reglas 
    #  validation_name == 'isEmail':
    #


    else:
        # Si no encuentra  la regla
        log.warning(f"Regla de validación desconocida '{validation_name}'. Ignorando.")
        return F.lit(None).cast("string")


#  La Función  Principal
# desde (transformers.py) solo se llama a esta funcion.

def validate_fields(df: DataFrame, validation_rules: list) -> Tuple[DataFrame, DataFrame]:
    """
    Aplica un conjunto de reglas de validación a un DataFrame de forma modular
    y lo divide en DataFrames 'OK' (válidos) y 'KO' (inválidos).

    :param df: El DataFrame de entrada.
    :param validation_rules: La lista de reglas desde los metadatos.
    :return: Una tupla de dos DataFrames: (df_ok, df_ko)
    """
    log.info(f"Aplicando {len(validation_rules)} reglas de validación (versión modular)...")

    temp_error_cols = [] # Nombres de columnas de error temporales 

    #  1 Construir columnas de error para CADA campo ---
    for rule_set in validation_rules:
        field_name = rule_set['field']
        validations_to_apply = rule_set['validations']
        
        error_col_name = f"__err_{field_name}"
        temp_error_cols.append(error_col_name)

        # Inicializa la columna de error para este campo como nula
        current_error_col = F.lit(None).cast("string")

        #  2 aplica segun la lista , todas las valiaciones a cada campo.
        for val_name in validations_to_apply:
            
            error_expr = _get_validation_expression(val_name, field_name)
            
            # F.coalesce() toma el primer valor no nulo.
            # Si 'current_error_col' ya tiene un error (ej: "notNull"),
            # se queda con ese. Si es nulo, comprueba 'error_expr'.
            # Esto captura el PRIMER error encontrado para el campo.
            current_error_col = F.coalesce(current_error_col, error_expr)
        
        # 3 Añadir la columna de error final del campo al DataFrame
        df = df.withColumn(error_col_name, current_error_col)

    
    # --- 4. Construir la columna final 'arraycoderrorbyfield' 
    
    # Creamos una lista de [key, value
    map_components = []
    for col_name in temp_error_cols:
        field_name_key = col_name.replace("__err_", "") # "office"
        map_components.append(F.lit(field_name_key))    #  'key'
        map_components.append(F.col(col_name))          #  'value'  "notEmpty" o null)

    # Creamos el mapa
    df = df.withColumn("arraycoderrorbyfield", F.map_from_arrays(
        F.array([k for i, k in enumerate(map_components) if i % 2 == 0]),
        F.array([v for i, v in enumerate(map_components) if i % 2 != 0])
    ))

    # Limpiamos el mapa: quitamos las 'keys' cuyo 'value' es null
    df = df.withColumn(
        "arraycoderrorbyfield",
        # CAMBIO: Usamos 'map_filter' en lugar de 'filter' para Mapas
        F.expr("map_filter(arraycoderrorbyfield, (k, v) -> v is not null)")
    )

    
    #  5 Separar DataFrames OK y KO 
    
    # Un registro es válido si su mapa de errores está vacío
    df = df.withColumn("__is_valid__", F.size(F.col("arraycoderrorbyfield")) == 0)

    df_ok = df.filter(F.col("__is_valid__") == True)
    df_ko = df.filter(F.col("__is_valid__") == False)

    
    #  6 Limpieza final de columnas 
    
    cols_to_drop = ["__is_valid__"] + temp_error_cols
    
    df_ok = df_ok.drop(*cols_to_drop, "arraycoderrorbyfield")
    df_ko = df_ko.drop(*cols_to_drop)
    
    log.info("Validación (modular) completada. DataFrames OK y KO generados.")

    return df_ok, df_ko