# tests/test_validations.py

import pytest
from pyspark.sql import SparkSession


from src.validations import validate_fields

def test_validation_split_and_error_content(spark: SparkSession):
    """
    Prueba unitaria para 'validate_fields'.
    Comprueba 3 cosas:
    1. Que los registros OK se separan correctamente.
    2. Que los registros KO se separan correctamente.
    3. Que los registros KO tienen el mapa de errores correcto.
    
    El argumento 'spark' lo pasa 'conftest.py'.
    """
    
    #  1.Preparar
    
    #  datos de ejemplo de tu enunciado
    test_data = [
        ("Xabier", 39, ""),      # KO: office está vacío
        ("Miguel", None, "RIO"), # KO: age es nulo
        ("Fran", 31, "RIO")      # OK: todo está bien
    ]
    schema = ["name", "age", "office"]
    df = spark.createDataFrame(test_data, schema)
    
    #  las reglas que queremos probar
    rules = [
        {"field": "office", "validations": ["notEmpty"]},
        {"field": "age", "validations": ["notNull"]}
    ]

    #  2. Act  
    
    #  la función que queremos probar
    df_ok, df_ko = validate_fields(df, rules)

    #  3. Assert  
    
    # Comprobamos las cuentas
    assert df_ok.count() == 1
    assert df_ko.count() == 2
    
    # mirar que el valor ok es de fran
    ok_names = [row.name for row in df_ok.collect()]
    assert ok_names == ["Fran"]
    
    # sacamos los datos ko
    ko_results = {row.name: row.arraycoderrorbyfield for row in df_ko.collect()}
    
    # comprobar que para cada name ko aparece como notempty y notnull
    assert "Xabier" in ko_results
    assert ko_results["Xabier"] == {"office": "notEmpty"}
    assert "Miguel" in ko_results
    assert ko_results["Miguel"] == {"age": "notNull"}