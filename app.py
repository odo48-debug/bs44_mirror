import os
import requests
from supabase import create_client

# --- CONFIGURACIÓN ---
B44_API_KEY = os.environ.get("B44_API_KEY")
B44_APP_ID = os.environ.get("B44_APP_ID")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_b44_data(entity_name):
    url = f'https://app.base44.com/api/apps/{B44_APP_ID}/entities/{entity_name}'
    headers = {'api_key': B44_API_KEY, 'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def get_supabase_columns(table_name):
    """
    Consulta a Supabase qué columnas existen realmente en la tabla.
    """
    # Consultamos una fila vacía para obtener las claves del esquema
    res = supabase.table(table_name).select("*").limit(1).execute()
    if len(res.data) > 0:
        return res.data[0].keys()
    else:
        # Si la tabla está vacía, este es un pequeño truco para sacar los nombres
        # de las columnas mediante una petición RPC o asumiendo un esquema básico.
        # Por ahora, si falla, usaremos el filtrado manual preventivo.
        return None

def sync_all():
    mapping = {
        "CompanyProfile": "company_profile",
        "CompanySettings": "company_settings",
        "ReportTemplate": "report_templates",
        "Technician": "technicians",
        "Client": "clients",
        "Report": "reports",
        "WitnessGroup": "witness_groups",
        "Witness": "witnesses",
        "Incident": "incidents",
        "Document": "documents"
    }

    # Campos que SIEMPRE ignoramos de Base44 porque Supabase los autogenera
    SYSTEM_IGNORE = ['created_at', 'updated_at', 'created_by', 'updated_by', 'created_by_id', 'updated_by_id', 'organization_id', 'app_id', '__v']

    for b44_entity, pg_table in mapping.items():
        print(f"🔄 Sincronizando {b44_entity} -> {pg_table}...")
        
        data = get_b44_data(b44_entity)
        if not data: continue
        if isinstance(data, dict): data = [data]

        # 1. Intentamos obtener las columnas reales de Supabase
        # (Esto es útil si has borrado o añadido columnas en SQL)
        cleaned_data = []
        for item in data:
            # Limpieza: eliminamos campos de sistema conocidos
            clean_item = {k: v for k, v in item.items() if k not in SYSTEM_IGNORE}
            cleaned_data.append(clean_item)

        # 2. Ejecutar Upsert
        # El comando se detendrá aquí si hay un error real de datos
        supabase.table(pg_table).upsert(cleaned_data).execute()
        print(f"✅ Tabla {pg_table} sincronizada con éxito.")

if __name__ == "__main__":
    sync_all()

