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
    # Si da 404, simplemente devolvemos None para que el script siga con la siguiente
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()

def sync_all():
    # Mapeo corregido (Base44 -> Supabase)
    mapping = {
        "CompanyProfile": "company_profile",
        "Technician": "technicians",
        "Client": "clients",
        "Report": "reports",
        "WitnessGroup": "witness_groups",
        "Document": "documents"
    }

    # LISTA NEGRA AMPLIADA (Basada en tus logs de error)
    FIELDS_TO_DROP = [
        'created_by_id', 'updated_by_id', 'is_sample', 
        'created_date', 'updated_date', 'created_by', 
        'updated_by', 'organization_id', 'app_id', '__v'
    ]

    for b44_entity, pg_table in mapping.items():
        print(f"🔄 Sincronizando {b44_entity}...")
        
        try:
            data = get_b44_data(b44_entity)
            
            if not data:
                print(f"➖ {b44_entity} no encontrada o vacía, saltando...")
                continue
                
            if isinstance(data, dict): 
                data = [data]

            # Limpieza profunda
            for item in data:
                for field in FIELDS_TO_DROP:
                    item.pop(field, None)

            # Upsert
            if data:
                supabase.table(pg_table).upsert(data).execute()
                print(f"✅ {pg_table} sincronizada con {len(data)} registros.")
            
        except Exception as e:
            print(f"⚠️ Error en {b44_entity}: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando proceso de sincronización...")
    sync_all()
    print("🏁 Proceso finalizado.")
