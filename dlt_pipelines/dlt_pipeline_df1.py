# Importamos Declarative Pipelines no DLT
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- 1. Lógica de Metadatos  ---

# Paths de los sources 
PERSON_SOURCE_PATH = "/Volumes/workspace/elt_modular/data/inputs/events/person/*"
EMPLOYEES_SOURCE_PATH = "/Volumes/workspace/elt_modular/data/inputs/events/employees/*"

# Reglas de validacion para person
person_validation_rules = {
    "office_not_empty": "office IS NOT NULL AND office != ''",
    "age_not_null": "age IS NOT NULL"
}

# Reglas de validacion para employees
employees_validation_rules = {
    "employee_id_not_null": "employee_id IS NOT NULL" 
}


# --- 2. Capa de Bronce con Auto Loader---

@dp.table(
    name="bronze_person",
    comment="Carga incremental de archivos JSON de personas"
)
def bronze_person():
    """ Reemplaza a readers.py y bronze.py del otro proyecto """
    return (
        spark.readStream.format("cloudFiles") # type: ignore
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", "/Workspace/Users/ignaqwert00@gmail.com/pipeline_root_folder_dlt/bronze_person") 
            .load(PERSON_SOURCE_PATH)
    )

@dp.table(
    name="bronze_employees",
    comment="Carga incremental de archivos JSON de empleados"
)
def bronze_employees():
    """ Reemplaza a readers.py y bronze.py """
    return (
        spark.readStream.format("cloudFiles") # type: ignore
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", "/Workspace/Users/ignaqwert00@gmail.com/pipeline_root_folder_dlt/bronze_employees")
            .load(EMPLOYEES_SOURCE_PATH)
    )


# --- 3. Capa de Plata Validación y Transformación ---

# --- Sección PERSONAS ---

@dp.table(
    name="silver_person_pre_quality",
    comment="Aplica reglas de calidad a person y descarta (drop) los KO"
)
@dp.expect_all_or_drop(person_validation_rules)
def silver_person_pre_quality():
    """
    Define una tabla intermedia que aplica las reglas de calidad.
    Los registros KO se descartan (drop).
    """
    return dp.read_stream("bronze_person")

@dp.table(
    name="silver_person_ok",
    comment="Registros OK de personas, enriquecidos con fecha"
)
def silver_person_ok():
    """
    Lee solo los registros OK de la tabla anterior.
    Aplica la lógica de 'add_fields' de transformers.py.
    """
    return (
        dp.read("silver_person_pre_quality")
           .withColumn("dt", F.current_timestamp())
    )
    
# --- Sección EMPLEADOS ---

@dp.table(
    name="silver_employees_pre_quality",
    comment="Aplica reglas de calidad a 'employees' y descarta (drop) los KO"
)
@dp.expect_all_or_drop(employees_validation_rules)
def silver_employees_pre_quality():
    """
    Define una tabla intermedia que aplica las reglas de calidad.
    Los registros KO se descartan (drop).
    """
    return dp.read_stream("bronze_employees")

@dp.table(
    name="silver_employees_ok",
    comment="Registros OK de empleados, enriquecidos"
)
def silver_employees_ok():
    """
    Define la tabla final OK de empleados.
    Lee solo los registros buenos de la tabla anterior.
    """
    return (
        dp.read("silver_employees_pre_quality")
           .withColumn("ingestion_dt", F.current_timestamp()) 
    )