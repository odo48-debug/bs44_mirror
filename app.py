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
        "CompanyProfile": "company_profile",
        "Technician": "technicians",
        "Client": "clients",
        "Template": "report_templates",
        "Report": "reports",
        "WitnessGroup": "witness_groups",
        "Testigo": "witnesses",
        "Incidencia": "incidents",
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
                    if isinstance(val, (str, float)) and str(val).endswith('.0'):
                        try: item[key] = int(float(val))
                        except: pass
                    if val == "":
                        item[key] = None

                # 3. TRADUCCIÓN DE BOOLEANOS
                bool_map = {"1": True, "2": False}
                for b_field in ['elevator', 'air_conditioning', 'has_vistas']:
                    if item.get(b_field) in ["1", "2", 1, 2]:
                        item[b_field] = bool_map.get(str(item[b_field]))

                # 4. NORMALIZACIÓN DE TEXTOS
                if pg_table in ["reports", "witnesses"]:
                    for text_field in ["property_type", "witness_status"]:
                        if item.get(text_field):
                            item[text_field] = str(item[text_field]).lower().replace(",", "")

            # 5. Envío de la entidad principal a Supabase
            if data:
                supabase.table(pg_table).upsert(data).execute()
                print(f"✅ {pg_table} sincronizada ({len(data)} registros).")

            # 6. LÓGICA ESPECIAL: Relación Granular de Testigos
            if pg_table == "reports":
                print("🔗 Generando vínculos relacionales de testigos...")
                relations = []
                for report in data:
                    elements = report.get('valued_elements', [])
                    if isinstance(elements, list):
                        for el in elements:
                            item_name = el.get('item_name', 'Activo sin nombre')
                            
                            # Testigos de Venta
                            v_ids = el.get('witness_ids', [])
                            if isinstance(v_ids, list):
                                for w_id in v_ids:
                                    relations.append({
                                        "report_id": report['id'],
                                        "witness_id": w_id,
                                        "valued_item_name": item_name,
                                        "witness_type": "venta"
                                    })
                            
                            # Testigos de Alquiler (dentro de valuation_results)
                            val_res = el.get('valuation_results', {})
                            r_ids = val_res.get('rental_witnesses_used', [])
                            if isinstance(r_ids, list):
                                for rw_id in r_ids:
                                    relations.append({
                                        "report_id": report['id'],
                                        "witness_id": rw_id,
                                        "valued_item_name": item_name,
                                        "witness_type": "alquiler"
                                    })

                if relations:
                    # Usamos upsert para evitar duplicados si el script corre varias veces
                    supabase.table("report_witness_assignment").upsert(relations).execute()
                    print(f"   -> {len(relations)} vínculos creados/actualizados en report_witness_assignment.")

        except Exception as e:
            print(f"⚠️ Error en {b44_entity}: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando proceso de sincronización...")
    sync_all()
    print("🏁 Proceso finalizado.")
