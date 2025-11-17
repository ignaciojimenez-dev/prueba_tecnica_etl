import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType

# Importamos las funciones que queremos probar
from src.transformers import _apply_add_fields, apply_transform

def test_transformers_add_fields(spark: SparkSession):
    """
    Prueba la función interna _apply_add_fields.
    Verifica que añade la columna 'dt' con el tipo correcto.
    """
    # 1. Preparar
    test_data = [("Fran",)]
    df = spark.createDataFrame(test_data, ["name"])
    
    # Esta es la regla que probamos
    params = {
        "addFields": [{"name": "dt", "function": "current_timestamp"}]
    }

    # 2. Actuar
    result_df = _apply_add_fields(df, params) #

    # 3. Comprobar : añade la columna correctamente, la columna añadida tiene el tipo correcto,
    # el número de filas no cambia
    assert "dt" in result_df.columns
    assert isinstance(result_df.schema["dt"].dataType, TimestampType)
    assert result_df.count() == 1

def test_transformers_dispatcher(spark: SparkSession):
    """
    Prueba la función principal 'apply_transform' (el dispatcher).
    Verifica que actualiza el dataframes_state correctamente.
    """
    # 1. Preparar
    test_data = [("Alberto",)]
    df = spark.createDataFrame(test_data, ["name"])
    
    # El estado inicial como en orchestrator.py
    dataframes_state = {"input_df": df}
    
    # La configuración de transformación de metadata.json
    transform_config = {
      "name": "person_ok_with_date",
      "type": "add_fields",
      "params": { 
        "input": "input_df", 
        "addFields": [{"name": "dt", "function": "current_timestamp"}]
      }
    }

    # 2. Actuar
    apply_transform(spark, dataframes_state, transform_config)

    # 3. Comprobar
    # Verificamos que el estado se ha actualizado con la clave 'name'
    # de la transformación 'person_ok_with_date'
    assert "person_ok_with_date" in dataframes_state
    assert dataframes_state["person_ok_with_date"].count() == 1
    assert "dt" in dataframes_state["person_ok_with_date"].columns