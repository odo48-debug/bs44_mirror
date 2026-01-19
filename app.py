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
    if response.status_code == 404: return None
    response.raise_for_status()
    return response.json()

def sync_all():
    mapping = {
        # Base44 (API)      :  Supabase (Tabla)
        "CompanyProfile": "company_profile",
        "Technician": "technicians",
        "Client": "clients",
        "Template": "report_templates",
        "Report": "reports",
        "WitnessGroup": "witness_groups",
        "Testigo": "witnesses",
        "Incident": "incidents",
        "UserDocument": "user_documents",
        "Document": "documents",
        "CompanyDocument": "company_documents",
        "CompanySettings": "company_settings" 
    }

    FIELDS_TO_DROP = [
        'created_by_id', 'updated_by_id', 'is_sample', 'created_date', 
        'updated_date', 'organization_id', 'app_id', '__v', 'updated_by'
    ]

    for b44_entity, pg_table in mapping.items():
        print(f"🔄 Sincronizando {b44_entity}...")
        try:
            data = get_b44_data(b44_entity)
            if not data: continue
            if isinstance(data, dict): data = [data]

            for item in data:
                # 1. Eliminar campos técnicos
                for field in FIELDS_TO_DROP:
                    item.pop(field, None)

                # 2. LIMPIEZA GENERAL (Números y vacíos)
                for key in list(item.keys()):
                    val = item[key]
                    # Corregir números .0 (26799.0 -> 26799)
                    if isinstance(val, (str, float)) and str(val).endswith('.0'):
                        try: item[key] = int(float(val))
                        except: pass
                    # Corregir textos vacíos
                    if val == "":
                        item[key] = None

                # 3. TRADUCCIÓN DE BOOLEANOS (1/2 -> True/False)
                # Aplicamos a cualquier tabla que pueda tener estos campos
                bool_map = {"1": True, "2": False}
                for b_field in ['elevator', 'air_conditioning', 'has_vistas']:
                    if item.get(b_field) in ["1", "2", 1, 2]:
                        item[b_field] = bool_map.get(str(item[b_field]))

                # 4. NORMALIZACIÓN DE TEXTOS (Reports y Witnesses)
                if pg_table in ["reports", "witnesses"]:
                    for text_field in ["property_type", "witness_status"]:
                        if item.get(text_field):
                            # minúsculas y quitar comas para coincidir con Enums
                            item[text_field] = str(item[text_field]).lower().replace(",", "")

            # 5. Envío a Supabase
            if data:
                supabase.table(pg_table).upsert(data).execute()
                print(f"✅ {pg_table} sincronizada correctamente con {len(data)} registros.")
            
        except Exception as e:
            print(f"⚠️ Error en {b44_entity}: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando proceso de sincronización...")
    sync_all()
    print("🏁 Proceso finalizado.")

