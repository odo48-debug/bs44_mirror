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

    FIELDS_TO_DROP = ['created_by_id', 'updated_by_id', 'is_sample', 'created_date', 'updated_date', 'organization_id', 'app_id', '__v']

    for b44_entity, pg_table in mapping.items():
        print(f"🔄 Sincronizando {b44_entity}...")
        try:
            data = get_b44_data(b44_entity)
            if not data: continue
            if isinstance(data, dict): data = [data]

            for item in data:
                # 1. Eliminar campos basura
                for field in FIELDS_TO_DROP:
                    item.pop(field, None)

                # 2. LIMPIEZA DE DATOS SEGÚN EL ERROR
                for key, value in item.items():
                    # Corregir números decimales enviados como strings (ej: "26799.0" -> 26799)
                    if isinstance(value, str) and value.replace('.','',1).isdigit() and '.0' in value:
                        item[key] = int(float(value))
                    
                    # Corregir fechas o estados vacíos (convertir "" en None/Null)
                    if value == "":
                        item[key] = None

            # Enviar a Supabase
            if data:
                supabase.table(pg_table).upsert(data).execute()
                print(f"✅ {pg_table} sincronizada correctamente.")
            
        except Exception as e:
            print(f"⚠️ Error en {b44_entity}: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando proceso de sincronización...")
    sync_all()
    print("🏁 Proceso finalizado.")
