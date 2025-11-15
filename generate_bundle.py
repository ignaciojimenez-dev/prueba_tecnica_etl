import os
import json
import logging
from jinja2 import Environment, FileSystemLoader

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- Configuración de Rutas (ACTUALIZADA) ---
METADATA_PATH = "metadata.json"
# 1. Lee las plantillas desde la carpeta /templates
TEMPLATES_DIR = "templates"
DLT_TEMPLATE_FILE = "dlt_pipeline.py.j2"
BUNDLE_TEMPLATE_FILE = "databricks.yml.j2"
# 2. Vuelca los .py generados en /pipelines_generadas
GENERATED_PIPELINES_DIR = "pipelines_generadas"
# 3. Vuelca el .yml final en la raíz del proyecto
GENERATED_BUNDLE_FILE = "databricks.yml"

def load_metadata(path: str) -> dict:
    """Carga el fichero de metadatos."""
    log.info(f"Cargando metadatos desde: {path}")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log.error(f"Error fatal al cargar {path}: {e}", exc_info=True)
        raise

def main():
    log.info("--- Iniciando Generador de Bundles DLT ---")
    
    # 1. Cargar metadatos
    config = load_metadata(METADATA_PATH)
    dataflows = config.get('dataflows', [])
    if not dataflows:
        log.error("No se encontraron 'dataflows' en el metadatos.")
        return

    # 2. Configurar Jinja2 para leer desde /templates
    env = Environment(loader=FileSystemLoader(TEMPLATES_DIR), trim_blocks=True, lstrip_blocks=True)
    
    # 3. Generar los Pipelines DLT en /pipelines_generadas
    log.info(f"Generando {len(dataflows)} pipelines DLT en '{GENERATED_PIPELINES_DIR}'...")
    os.makedirs(GENERATED_PIPELINES_DIR, exist_ok=True)
    
    try:
        dlt_template = env.get_template(DLT_TEMPLATE_FILE)
    except Exception as e:
        log.error(f"Error cargando plantilla DLT '{DLT_TEMPLATE_FILE}': {e}")
        return

    for df_config in dataflows:
        df_name = df_config['name']
        # Crea el fichero de salida dentro de la carpeta /pipelines_generadas
        output_filename = os.path.join(GENERATED_PIPELINES_DIR, f"dlt_{df_name}.py")
        
        log.info(f"Renderizando {output_filename}...")
        try:
            rendered_code = dlt_template.render(
                dataflow=df_config,
                spark="spark"
            )
            
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(rendered_code)
        except Exception as e:
            log.error(f"Error renderizando pipeline para {df_name}: {e}", exc_info=True)

    log.info("--- Generación de Pipelines DLT completada. ---")

    # 4. Generar el databricks.yml en la raíz
    log.info(f"Generando {GENERATED_BUNDLE_FILE} en la raíz...")
    try:
        bundle_template = env.get_template(BUNDLE_TEMPLATE_FILE)
        
        rendered_yaml = bundle_template.render(
            dataflows=dataflows
        )
        
        # Escribe el .yml en la raíz
        with open(GENERATED_BUNDLE_FILE, 'w', encoding='utf-8') as f:
            f.write(rendered_yaml)
            
        log.info(f"--- {GENERATED_BUNDLE_FILE} generado exitosamente. ---")
        
    except Exception as e:
        log.error(f"Error renderizando {GENERATED_BUNDLE_FILE}: {e}", exc_info=True)


if __name__ == "__main__":
    main()