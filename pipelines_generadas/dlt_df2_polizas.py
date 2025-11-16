# Databricks notebook source
# --- GENERADO AUTOMÁTICAMENTE POR JINJA2 ---
# Dataflow: df2_polizas
# ------------------------------------------

import pyspark.pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Importamos los módulos de lógica reutilizable
from src import dlt_helpers

# --- 1. CAPA BRONCE (GENERADA DESDE 'sources') ---
@dp.table(
    name="bronze_polizas",
    comment="Carga incremental (Autoloader) desde /Volumes/workspace/elt_modular/data/inputs/events/polizas/*"
)
def bronze_polizas():
    """ Tabla Bronze para polizas_inputs """
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "JSON")
            .option("cloudFiles.inferColumnTypes", "true")
            #.option("cloudFiles.schemaLocation", "/Volumes/workspace/elt_modular/schemas/bronze_polizas")
            .option("ignoreMissingFiles", "true")
            .load("/Volumes/workspace/elt_modular/data/inputs/events/polizas/*")
    )

# --- 2. CAPA PLATA (GENERADA DESDE 'transformations_silver') ---


@dp.table(
    name="silver_pre_quality_bronze_polizas",
    comment="Aplica reglas de calidad DLT a la tabla bronze_polizas",
)
# --- CAMBIO 1: Usamos 'expect_all_or_drop' y el helper ---
@dp.expect_all_or_drop(dlt_helpers.generate_validation_rules([{'field': 'policy_id', 'validations': ['notEmpty']}, {'field': 'premium', 'validations': ['notNull']}]))
def silver_pre_quality_bronze_polizas():
    """ Aplica expectativas y descarta registros malos de bronze_polizas """
    return dp.read_stream("bronze_polizas")


@dp.table(
    name="silver_polizas_ok",
    comment="Registros OK de bronze_polizas, enriquecidos."
)
def silver_polizas_ok():
    """ 
    Lee los registros que pasaron la calidad de silver_pre_quality_bronze_polizas
    y aplica transformaciones finales.
    """
    # --- CAMBIO 2: Eliminamos el .filter("quarantine IS NULL") ---
    df_ok = dp.read_stream("silver_pre_quality_bronze_polizas")
    
    # --- CAMBIO 3: Aplicamos las transformaciones usando el helper ---
    return dlt_helpers.apply_silver_transformations(
        df_ok,
        [{"name": "polizas_ok_with_date", "params": {"addFields": [{"function": "current_timestamp", "name": "dt_ingestion"}], "input": "validation_polizas_ok"}, "type": "add_fields"}]
    )

