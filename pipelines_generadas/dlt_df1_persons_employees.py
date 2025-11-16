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
            #.option("cloudFiles.schemaLocation", "/Volumes/workspace/elt_modular/schemas/bronze_person")
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
            #.option("cloudFiles.schemaLocation", "/Volumes/workspace/elt_modular/schemas/bronze_employees")
            .option("ignoreMissingFiles", "true")
            .load("/Volumes/workspace/elt_modular/data/inputs/events/employees/*")
    )

# --- 2. CAPA PLATA (GENERADA DESDE 'transformations_silver') ---


@dp.table(
    name="silver_pre_quality_bronze_person",
    comment="Aplica reglas de calidad DLT a la tabla bronze_person",
)
@dp.expect_all(dlt_helpers.generate_validation_rules([{"field": "office", "validations": ["notEmpty"]}, {"field": "age", "validations": ["notNull"]}]))
def silver_pre_quality_bronze_person():
    """ Aplica expectativas a bronze_person """
    return dp.read_stream("bronze_person")


@dp.table(
    name="silver_person_ok",
    comment="Registros OK de bronze_person, enriquecidos."
)
def silver_person_ok():
    """ 
    Filtra los registros OK (sin 'quarantine') de silver_pre_quality_bronze_person
    y aplica transformaciones finales.
    """
    df_ok = dp.read_stream("silver_pre_quality_bronze_person").filter("quarantine IS NULL")
    
    # Aplicamos las transformaciones 'add_fields'
    return dlt_helpers.apply_silver_transformations(
        df_ok,
        [{"name": "person_ok_with_date", "params": {"addFields": [{"function": "current_timestamp", "name": "dt"}], "input": "validation_person_ok"}, "type": "add_fields"}]
    )


@dp.table(
    name="discards_person_ko",
    comment="Registros KO (descartados) de bronze_person"
)
def discards_person_ko():
    """ 
    Filtra los registros KO (con 'quarantine') de silver_pre_quality_bronze_person
    para análisis de errores.
    """
    return (
        dp.read_stream("silver_pre_quality_bronze_person")
            .filter("quarantine IS NOT NULL")
            .withColumn("ingestion_dt", F.current_timestamp())
    )

