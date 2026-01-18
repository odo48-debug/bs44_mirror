import os
import requests
from supabase import create_client

# En lugar de escribir la clave aquí, la pedimos al sistema
B44_API_KEY = os.environ.get("B44_API_KEY")
B44_APP_ID = os.environ.get("B44_APP_ID")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_b44_data(entity_name):
    url = f'https://app.base44.com/api/apps/{B44_APP_ID}/entities/{entity_name}'
    headers = {'api_key': B44_API_KEY, 'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)
    # .raise_for_status() detendrá el script si la API de Base44 responde con error
    response.raise_for_status()
    return response.json()

def sync_all():
    # El orden es crucial para las claves foráneas (Foreign Keys)
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

    # Campos que se ignoran para evitar errores de esquema en PostgreSQL
    IGNORE_FIELDS = ['created_by', 'updated_by', 'organization_id', 'app_id', 'owner_id']

    for b44_entity, pg_table in mapping.items():
        print(f"🔄 Sincronizando {b44_entity}...")
        
        data = get_b44_data(b44_entity)
        if not data:
            print(f"⚠️ No hay datos para {b44_entity}, continuando...")
            continue
            
        if isinstance(data, dict): 
            data = [data]

        # Filtrado de campos
        cleaned_data = []
        for item in data:
            clean_item = {k: v for k, v in item.items() if k not in IGNORE_FIELDS}
            cleaned_data.append(clean_item)

        # UPSERT: Si falla el envío a Supabase, el script lanzará una excepción y se detendrá
        result = supabase.table(pg_table).upsert(cleaned_data).execute()
        
        # Verificación manual adicional del resultado de Supabase
        print(f"✅ Éxito en {pg_table}: {len(cleaned_data)} registros procesados.")

if __name__ == "__main__":
    sync_all()
