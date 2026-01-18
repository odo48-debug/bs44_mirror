import os
import requests
from supabase import create_client

# --- CONFIGURACIÓN ---
# Asegúrate de que en GitHub Secrets los nombres coinciden exactamente con estos
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

def sync_all():
    # Mapeo: Nombre Entidad Base44 -> Nombre Tabla Supabase
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

    # Campos que eliminamos para evitar errores de columna no encontrada
    FIELDS_TO_DROP = ['created_date', 'updated_date', 'created_by', 'updated_by', 'organization_id']

    for b44_entity, pg_table in mapping.items():
        print(f"🔄 Sincronizando {b44_entity}...")
        
        try:
            data = get_b44_data(b44_entity)
            
            if not data:
                print(f"➖ {b44_entity} está vacía, saltando...")
                continue
                
            if isinstance(data, dict): 
                data = [data]

            # Limpieza de datos antes de enviar
            for item in data:
                for field in FIELDS_TO_DROP:
                    item.pop(field, None)

            # Enviamos a Supabase
            supabase.table(pg_table).upsert(data).execute()
            print(f"✅ {pg_table} sincronizada correctamente con {len(data)} registros.")
            
        except Exception as e:
            print(f"⚠️ Error en {b44_entity}: {e}")

# --- ESTA ES LA PARTE QUE FALTABA ---
# Sin esto, el script no hace nada al ejecutarse
if __name__ == "__main__":
    print("🚀 Iniciando proceso de sincronización...")
    sync_all()
    print("🏁 Proceso finalizado.")
