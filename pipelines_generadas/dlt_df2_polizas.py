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
    name="silver_pre_quality_polizas_inputs",
    comment="Aplica reglas de calidad DLT a la tabla polizas_inputs",
)
@dp.expect_all(dlt_helpers.generate_validation_rules([{'field': 'policy_id', 'validations': ['notEmpty']}, {'field': 'premium', 'validations': ['notNull']}]))
def silver_pre_quality_polizas_inputs():
    """ Aplica expectativas a polizas_inputs """
    return dp.read_stream("polizas_inputs")


@dp.table(
    name="silver_polizas_ok",
    comment="Registros OK de polizas_inputs, enriquecidos."
)
def silver_polizas_ok():
    """ 
    Filtra los registros OK (sin 'quarantine') de silver_pre_quality_polizas_inputs
    y aplica transformaciones finales.
    """
    df_ok = dp.read_stream("silver_pre_quality_polizas_inputs").filter("quarantine IS NULL")
    
    # Aplicamos las transformaciones 'add_fields'
    return dlt_helpers.apply_silver_transformations(
        df_ok,
        [{"name": "polizas_ok_with_date", "params": {"addFields": [{"function": "current_timestamp", "name": "dt_ingestion"}], "input": "validation_polizas_ok"}, "type": "add_fields"}]
    )


@dp.table(
    name="discards_polizas_ko",
    comment="Registros KO (descartados) de polizas_inputs"
)
def discards_polizas_ko():
    """ 
    Filtra los registros KO (con 'quarantine') de silver_pre_quality_polizas_inputs
    para análisis de errores.
    """
    return (
        dp.read_stream("silver_pre_quality_polizas_inputs")
            .filter("quarantine IS NOT NULL")
            .withColumn("ingestion_dt", F.current_timestamp())
    )

