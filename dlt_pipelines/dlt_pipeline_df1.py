import dlt # type: ignore
from pyspark.sql import functions as F

# --- 1. Lógica de Metadatos  ---

# Paths de los 'sources'
PERSON_SOURCE_PATH = "/Volumes/workspace/elt_modular/data/inputs/events/person/*"
EMPLOYEES_SOURCE_PATH = "/Volumes/workspace/elt_modular/data/inputs/events/employees/*"

# Reglas de validación para 'person_inputs'
person_validation_rules = {
    "office_not_empty": "office IS NOT NULL AND office != ''",
    "age_not_null": "age IS NOT NULL"
}

# Reglas de validación para 'employees_inputs'
employees_validation_rules = {
    "employee_id_not_null": "employee_id IS NOT NULL" 
}


# --- 2. Capa de Bronce con Auto Loader ---

@dlt.table(
    name="bronze_person",
    comment="Carga incremental (Auto Loader) de archivos JSON de personas"
)
def bronze_person():
    """ Reemplaza a readers.py y bronze.py para 'person' """
    return (
        spark.readStream.format("cloudFiles") # type: ignore
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            # Ruta donde Auto Loader guarda los archivos que ya ha leído
            .option("cloudFiles.schemaLocation", "/tmp/dlt/schemas/bronze_person") 
            .load(PERSON_SOURCE_PATH)
    )

@dlt.table(
    name="bronze_employees",
    comment="Carga incremental (Auto Loader) de archivos JSON de empleados"
)
def bronze_employees():
    """ Reemplaza a readers.py y bronze.py para 'employees' """
    return (
        spark.readStream.format("cloudFiles") # type: ignore
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", "/tmp/dlt/schemas/bronze_employees")
            .load(EMPLOYEES_SOURCE_PATH)
    )


# --- 3. Capa de Plata Validacion y Transformacion ---

# --- Seccion PERSONAS ---

@dlt.table(
    name="silver_person_pre_quality",
    comment="Aplica reglas de calidad a 'person' y desvía (quarantine) los KO"
)
@dlt.expect_all_or_quarantine(person_validation_rules)
def silver_person_pre_quality():
    """
    Lee de la tabla de bronce.
    Aplica el diccionario 'person_validation_rules'.
    Los KO se envían a 'silver_person_pre_quality_quarantined'.
    """
    return dlt.read_stream("bronze_person")

@dlt.table(
    name="silver_person_ok",
    comment="Registros OK de personas, enriquecidos con fecha"
)
def silver_person_ok():
    """
    Lee solo los registros OK ('LIVE') de la tabla anterior.
    Aplica la lógica de 'add_fields' de transformers.py.
    """
    return (
        dlt.read("silver_person_pre_quality")
           .withColumn("dt", F.current_timestamp()) # 'person_ok_with_date'
    )
    
# --- Sección EMPLEADOS ---

@dlt.table(
    name="silver_employees_pre_quality",
    comment="Aplica reglas de calidad a 'employees' y desvía (quarantine) los KO"
)
@dlt.expect_all_or_quarantine(employees_validation_rules)
def silver_employees_pre_quality():
    """
    Lee de la tabla de bronce de empleados.
    Aplica las reglas de 'employees_validation_rules'.
    Los KO se envían a 'silver_employees_pre_quality_quarantined'.
    """
    return dlt.read_stream("bronze_employees")

@dlt.table(
    name="silver_employees_ok",
    comment="Registros OK de empleados, enriquecidos"
)
def silver_employees_ok():
    """
    Lee solo los registros OK ('LIVE') de empleados.
    (Aquí aplicarías cualquier 'add_fields' para empleados)
    """
    return (
        dlt.read("silver_employees_pre_quality")
           .withColumn("ingestion_dt", F.current_timestamp())
    )