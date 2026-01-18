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
    mapping = {
        # Base44 (API)      :  Supabase (Tabla)
        "CompanyProfile": "company_profile",
        "Technician": "technicians",
        "Client": "clients",
        "ReportTemplate": "report_templates",
        "Report": "reports",
        "WitnessGroup": "witness_groups",
        "Witness": "witnesses",
        "Incident": "incidents",
        "UserDocument": "user_documents",
        "Document": "documents",
        "CompanyDocument": "company_documents",
        "CompanySettings": "company_settings" 
    }

    # 1. QUITAMOS 'file_url' de esta lista para que SÍ se sincronice
    FIELDS_TO_DROP = [
        'created_by_id', 'updated_by_id', 'is_sample', 
        'created_date', 'updated_date', 'created_by', 
        'updated_by', 'organization_id', 'app_id', '__v'
    ]

    for b44_entity, pg_table in mapping.items():
        print(f"🔄 Sincronizando {b44_entity}...")
        try:
            data = get_b44_data(b44_entity)
            if not data: continue
            if isinstance(data, dict): data = [data]

            for item in data:
                # Limpieza de campos de sistema
                for field in FIELDS_TO_DROP:
                    item.pop(field, None)

                # 2. CORRECCIÓN DE FECHAS VACÍAS (Mantenemos esta por seguridad)
                if pg_table == "reports":
                    if item.get("date") == "":
                        item["date"] = None

            # Enviar a Supabase
            if data:
                supabase.table(pg_table).upsert(data).execute()
                print(f"✅ {pg_table} sincronizada con {len(data)} registros.")
            
        except Exception as e:
            print(f"⚠️ Error en {b44_entity}: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando proceso de sincronización...")
    sync_all()
    print("🏁 Proceso finalizado.")
