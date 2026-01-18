import os
import requests
from supabase import create_client

# --- CONFIGURACIÓN ---
B44_API_KEY = os.environ.get("B44_API_KEY")
B44_APP_ID = os.environ.get("B44_APP_ID")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- LISTA BLANCA DE COLUMNAS (Sincronizado con tu SQL) ---
ALLOWED_COLUMNS = {
    "company_profile": ["id", "name", "email", "phone", "address", "cif", "website", "logo_url"],
    "technicians": ["id", "full_name", "email", "phone", "dni_nie", "role", "profession", "license_number", "provincia", "is_active"],
    "clients": ["id", "name", "contact_person", "email", "phone", "address", "tax_id", "client_type"],
    "report_templates": ["id", "name", "description", "version", "is_active"],
    "reports": ["id", "report_number", "date", "status", "client_id", "technician_id", "template_id", "address", "municipio", "provincia"],
    "witness_groups": ["id", "report_id", "group_name", "order"],
    "witnesses": ["id", "group_id", "name", "value", "unit", "comment"],
    "incidents": ["id", "report_id", "description", "severity", "status", "resolution"],
    "documents": ["id", "name", "file_url", "file_type", "entity_type", "entity_id"]
}

def get_b44_data(entity_name):
    url = f'https://app.base44.com/api/apps/{B44_APP_ID}/entities/{entity_name}'
    headers = {'api_key': B44_API_KEY, 'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def sync_all():
    # Mapeo exacto entre Base44 y Supabase
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
        
        data = get_b44_data(b44_entity)
        
        # Si no hay datos, pasamos a la siguiente tabla
        if not data:
            print(f"➖ Sin datos para {b44_entity}")
            continue
            
        if isinstance(data, dict): data = [data]

        # Filtrado por Lista Blanca
        if pg_table in ALLOWED_COLUMNS:
            valid_fields = ALLOWED_COLUMNS[pg_table]
            cleaned_data = []
            for item in data:
                # Solo permitimos campos que existan en nuestro SQL de Supabase
                clean_item = {k: v for k, v in item.items() if k in valid_fields}
                # Solo añadimos si el item no quedó vacío
                if clean_item:
                    cleaned_data.append(clean_item)
            
            if cleaned_data:
                supabase.table(pg_table).upsert(cleaned_data).execute()
                print(f"✅ {pg_table} sincronizada con {len(cleaned_data)} registros.")
        else:
            print(f"⚠️ Advertencia: {pg_table} no está configurada en ALLOWED_COLUMNS.")

if __name__ == "__main__":
    sync_all()
