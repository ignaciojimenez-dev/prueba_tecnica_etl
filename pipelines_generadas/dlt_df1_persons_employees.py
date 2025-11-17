# Databricks notebook source
# --- GENERADO AUTOMÁTICAMENTE POR JINJA2 ---
# Dataflow: df1_persons_employees
# ------------------------------------------

import pyspark.pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Importamos los módulos de lógica reutilizable
from src import dlt_helpers

# --- 1. CAPA BRONCE (GENERADA DESDE 'sources') ---
@dp.table(
    name="bronze_person",
    comment="Carga incremental (Autoloader) desde /Volumes/workspace/elt_modular/data/inputs/events/person/*"
)
def bronze_person():
    """ Tabla Bronze para person_inputs """
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "JSON")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("ignoreMissingFiles", "true")
            .load("/Volumes/workspace/elt_modular/data/inputs/events/person/*")
    )

@dp.table(
    name="bronze_employees",
    comment="Carga incremental (Autoloader) desde /Volumes/workspace/elt_modular/data/inputs/events/employees/*"
)
def bronze_employees():
    """ Tabla Bronze para employees_inputs """
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "JSON")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("ignoreMissingFiles", "true")
            .load("/Volumes/workspace/elt_modular/data/inputs/events/employees/*")
    )

# --- 2. CAPA PLATA (GENERADA DESDE 'transformations_silver') ---


@dp.table(
    name="silver_pre_quality_bronze_person",
    comment="Aplica reglas de calidad DLT a la tabla bronze_person",
)
# --- uso de 'expect_all_or_drop' y el helper ---
@dp.expect_all_or_drop(dlt_helpers.generate_validation_rules([{'field': 'office', 'validations': ['notEmpty']}, {'field': 'age', 'validations': ['notNull']}]))
def silver_pre_quality_bronze_person():
    """ Aplica expectativas y descarta registros malos de bronze_person """
    return dp.read_stream("bronze_person")


@dp.table(
    name="silver_person_ok",
    comment="Registros OK de bronze_person, enriquecidos."
)
def silver_person_ok():
    """ 
    Lee los registros que pasaron la calidad de silver_pre_quality_bronze_person
    y aplica transformaciones finales.
    """
    # --- sin filter quarentine , mirar si se puede aplicar ---
    df_ok = dp.read_stream("silver_pre_quality_bronze_person")
    
    # Usamos la nueva función genérica 'apply_transformations'
    return dlt_helpers.apply_transformations(
        df_ok,
        [{"name": "person_ok_with_date", "params": {"addFields": [{"function": "current_timestamp", "name": "dt"}], "input": "validation_person_ok"}, "type": "add_fields"}]
    )



# --- 3. CAPA ORO (GENERADA DESDE 'transformations_gold') ---

  
@dp.table(
    name="gold_persons_anonymized",     comment="Capa Oro: mask_person_pii"
)
def gold_persons_anonymized():
    """
    Lee desde la capa Plata (silver_person_ok) 
    y aplica transformaciones de Oro (mask_person_pii).
    """
    df_silver = dp.read_stream("silver_person_ok")
    
    # Reutilizamos la  función helper generica
    # Pasamos la configuración de 'apply_masking'
    return dlt_helpers.apply_transformations(
        df_silver,
        [{"name": "mask_person_pii", "params": {"input": "silver_person_ok", "masking_rules": [{"field": "email", "function": "sha2"}, {"field": "name", "function": "md5"}]}, "type": "apply_masking"}]
    )

