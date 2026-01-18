import os
import requests
from supabase import create_client

# En lugar de escribir la clave aquí, la pedimos al sistema
B44_API_KEY = os.environ.get("B44_API_KEY")
B44_APP_ID = os.environ.get("B44_APP_ID")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_b44_data(entity_name):
    """ Función basada en tu ejemplo de Base44 """
    url = f'https://app.base44.com/api/apps/{APP_ID}/entities/{entity_name}'
    headers = {'api_key': API_KEY, 'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def sync_all():
    # Listado de entidades en el orden correcto de jerarquía SQL
    mapping = {
        "CompanyProfile": "company_profile",
        "Technician": "technicians",
        "Client": "clients",
        "ReportTemplate": "report_templates",
        "Report": "reports",
        "WitnessGroup": "witness_groups",
        "Witness": "witnesses",
        "Incident": "incidents",
        "Document": "documents"
    }

    for b44_entity, pg_table in mapping.items():
        print(f"🔄 Sincronizando {b44_entity}...")
        
        # 1. Extraer (como en tu ejemplo)
        data = get_b44_data(b44_entity)
        
        # 2. Asegurar que es una lista
        if isinstance(data, dict): data = [data]
        
        if data:
            # 3. Upsert en Supabase (Profesional: Inserta o Actualiza)
            # Esto evita duplicados y mantiene el espejo al día
            supabase.table(pg_table).upsert(data).execute()
            print(f"✅ {len(data)} registros en {pg_table}")

if __name__ == "__main__":
    sync_all()