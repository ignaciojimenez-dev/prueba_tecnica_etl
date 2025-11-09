# tests/conftest.py

from typing import Generator
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Crea una SparkSession de prueba (Fixture de Pytest).
    Esta sesión se comparte entre todos los tests.
    """
    print("--- Creando SparkSession local para tests ---")
    
    # Usamos una configuración local mínima
    # (No necesitamos Delta aquí, solo probamos la lógica de DF)
    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-spark-session")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    
    # 'yield' entrega la sesión a los tests
    yield spark_session
    
    # 'finally' (implícito) se ejecuta al final de todo
    print("--- Deteniendo SparkSession de tests ---")
    spark_session.stop()