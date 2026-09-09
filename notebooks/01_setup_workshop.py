# Databricks notebook source
# MAGIC %md
# MAGIC # Preparar el taller de Corona
# MAGIC
# MAGIC Este notebook crea el catálogo `corona_workshop`, genera datos sintéticos de
# MAGIC manufactura y clientes, y carga todos los documentos de GreenSheen.
# MAGIC
# MAGIC Los datos son ficticios. Los correos usan dominios reservados y ninguna persona,
# MAGIC cuenta o transacción corresponde a información real.

# COMMAND ----------

# MAGIC %pip install pypdf==5.1.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from datetime import date, datetime, timedelta
from pathlib import Path
from zipfile import ZipFile
import io
import os
import random
import re

import numpy as np
import pandas as pd
import requests
from pypdf import PdfReader

CATALOG = "corona_workshop"
ASSET_SCHEMA = "assets"
OPERATIONS_SCHEMA = "operaciones"
CUSTOMER_SCHEMA = "clientes"
GREENSHEEN_SCHEMA = "greensheen"
ASSET_URL = (
    "https://raw.githubusercontent.com/scaravacap/"
    "corona-day1-workshop/main/assets/greensheen_demo.zip"
)

dbutils.widgets.text("asset_url", ASSET_URL, "URL del paquete GreenSheen")
asset_url = dbutils.widgets.get("asset_url").strip() or ASSET_URL

random.seed(2026)
np.random.seed(2026)

print(f"Catálogo objetivo: {CATALOG}")
print(f"Paquete GreenSheen: {asset_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear catálogo, schemas y volumen

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in [ASSET_SCHEMA, OPERATIONS_SCHEMA, CUSTOMER_SCHEMA, GREENSHEEN_SCHEMA]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{ASSET_SCHEMA}.greensheen_files"
)

VOLUME_ROOT = Path(f"/Volumes/{CATALOG}/{ASSET_SCHEMA}/greensheen_files")
VOLUME_ROOT.mkdir(parents=True, exist_ok=True)
print(f"Volumen listo: {VOLUME_ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Datos operacionales de Corona
# MAGIC
# MAGIC La historia tiene tres plantas ficticias y una operación diaria de producción.
# MAGIC La mayor parte de los datos sigue rangos normales. Algunas filas traen defectos
# MAGIC intencionales para que el perfil de calidad tenga algo que descubrir.

# COMMAND ----------

PLANTS = {
    "Sabaneta": {"city": "Medellín", "temp": 1160, "energy": 620, "water": 1.8},
    "Sopó": {"city": "Bogotá", "temp": 1140, "energy": 590, "water": 1.6},
    "Girardota": {"city": "Medellín", "temp": 1180, "energy": 650, "water": 2.0},
}
LINES = ["Horno-1", "Horno-2", "Prensa-1", "Esmaltado-1"]
FAMILIES = ["Pisos", "Revestimientos", "Sanitarios", "Pegantes"]
SKUS = {
    "Pisos": ["PISO-ANDINO-60", "PISO-URBANO-45", "PISO-MARFIL-60"],
    "Revestimientos": ["REV-CALIZA-30", "REV-NUBE-25", "REV-TERRA-30"],
    "Sanitarios": ["SAN-ACUAPLUS", "SAN-ECOFLOW", "SAN-CONTEMPO"],
    "Pegantes": ["PEG-FLEX-25", "PEG-PRO-25", "BOQ-PREMIUM-20"],
}
SHIFTS = ["A", "B", "C"]

end_date = date.today() - timedelta(days=1)
start_date = end_date - timedelta(days=119)
production_rows = []
sequence = 0

for day_offset in range(120):
    production_date = start_date + timedelta(days=day_offset)
    weekday_factor = 0.92 if production_date.weekday() == 6 else 1.0
    seasonal = 1.0 + 0.06 * np.sin(day_offset / 12)
    for plant, plant_cfg in PLANTS.items():
        for line in LINES:
            for shift in SHIFTS:
                sequence += 1
                family = random.choice(FAMILIES)
                sku = random.choice(SKUS[family])
                base_tons = {
                    "Horno-1": 46,
                    "Horno-2": 42,
                    "Prensa-1": 37,
                    "Esmaltado-1": 31,
                }[line]
                tons = max(
                    4.0,
                    np.random.normal(base_tons * weekday_factor * seasonal, 3.8),
                )
                defect_rate = np.clip(
                    np.random.beta(2.2, 65) + (0.012 if plant == "Girardota" else 0),
                    0.001,
                    0.18,
                )
                temp = np.random.normal(
                    plant_cfg["temp"] + (12 if "Horno" in line else -35), 13
                )
                moisture = np.clip(np.random.normal(5.8, 0.65), 2.5, 9.5)
                energy = np.random.normal(plant_cfg["energy"], 34)
                sequence_time = datetime.combine(
                    production_date,
                    datetime.min.time(),
                ) + timedelta(hours={"A": 5, "B": 13, "C": 21}[shift])
                production_rows.append(
                    {
                        "lote_id": f"LOT-{production_date:%Y%m%d}-{sequence:06d}",
                        "fecha_produccion": production_date,
                        "fecha_evento": sequence_time,
                        "planta": plant,
                        "ciudad": plant_cfg["city"],
                        "linea": line,
                        "turno": shift,
                        "familia_producto": family,
                        "sku": sku,
                        "toneladas_producidas": round(float(tons), 2),
                        "tasa_defectos": round(float(defect_rate), 4),
                        "humedad_pct": round(float(moisture), 2),
                        "temperatura_proceso_c": round(float(temp), 1),
                        "energia_kwh_ton": round(float(energy), 2),
                        "estado_calidad": (
                            "Revisar" if defect_rate >= 0.08 else "Conforme"
                        ),
                        "incidente_id": None,
                    }
                )

df_production = pd.DataFrame(production_rows)

# Defectos intencionales para el perfil inicial.
rng = np.random.default_rng(2026)
null_sku_idx = rng.choice(df_production.index, 35, replace=False)
df_production.loc[null_sku_idx, "sku"] = None
negative_tons_idx = rng.choice(df_production.index, 12, replace=False)
df_production.loc[negative_tons_idx, "toneladas_producidas"] = -5.0
bad_rate_idx = rng.choice(df_production.index, 18, replace=False)
df_production.loc[bad_rate_idx, "tasa_defectos"] = 1.25
temp_outlier_idx = rng.choice(df_production.index, 15, replace=False)
df_production.loc[temp_outlier_idx, "temperatura_proceso_c"] = 1510.0

(
    spark.createDataFrame(df_production)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{OPERATIONS_SCHEMA}.produccion_calidad")
)

spark.sql(
    f"""
    COMMENT ON TABLE {CATALOG}.{OPERATIONS_SCHEMA}.produccion_calidad IS
    'Producción sintética diaria por planta, línea y turno para el taller de Corona'
    """
)

print(
    f"produccion_calidad: {len(df_production):,} filas, "
    "35 SKU nulos, 12 toneladas negativas, 18 tasas mayores a 1 y 15 temperaturas extremas"
)

# COMMAND ----------

resource_rows = []
for day_offset in range(120):
    usage_date = start_date + timedelta(days=day_offset)
    demand_factor = 1.0 + 0.08 * np.sin(day_offset / 15)
    for plant, cfg in PLANTS.items():
        production_tons = max(220, np.random.normal(410 * demand_factor, 35))
        electricity_mwh = production_tons * np.random.normal(cfg["energy"], 18) / 1000
        gas_m3 = production_tons * np.random.normal(42, 3.5)
        water_m3 = production_tons * np.random.normal(cfg["water"], 0.12)
        resource_rows.append(
            {
                "fecha": usage_date,
                "planta": plant,
                "ciudad": cfg["city"],
                "toneladas_producidas": round(float(production_tons), 2),
                "electricidad_mwh": round(float(electricity_mwh), 2),
                "gas_natural_m3": round(float(gas_m3), 2),
                "agua_m3": round(float(water_m3), 2),
                "co2_ton": round(float(gas_m3 * 0.00194), 3),
                "costo_energia_cop": round(float(electricity_mwh * 690000), 0),
            }
        )

df_resources = pd.DataFrame(resource_rows)
(
    spark.createDataFrame(df_resources)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{OPERATIONS_SCHEMA}.consumo_recursos")
)
print(f"consumo_recursos: {len(df_resources):,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Datos sintéticos con información sensible
# MAGIC
# MAGIC Esta tabla existe para Data Classification. Los valores son sintéticos y usan
# MAGIC dominios reservados, pero los nombres de columna y los formatos permiten que el
# MAGIC clasificador detecte nombres, correos, teléfonos, documentos y direcciones.

# COMMAND ----------

FIRST_NAMES = [
    "María",
    "Juan",
    "Camila",
    "Andrés",
    "Valentina",
    "Santiago",
    "Daniela",
    "Carlos",
    "Laura",
    "Felipe",
]
LAST_NAMES = [
    "García",
    "Rodríguez",
    "Martínez",
    "López",
    "González",
    "Ramírez",
    "Vargas",
    "Rojas",
    "Herrera",
    "Medina",
]
CITIES = {
    "Medellín": ["Calle 10 # 35-20", "Carrera 43A # 1-50", "Calle 33 # 75-10"],
    "Bogotá": ["Carrera 7 # 72-41", "Calle 93 # 14-20", "Avenida 19 # 100-10"],
    "Cali": ["Calle 5 # 66-20", "Carrera 100 # 15-30", "Avenida 6N # 24-10"],
    "Barranquilla": ["Carrera 53 # 80-20", "Calle 72 # 45-30", "Vía 40 # 85-10"],
}

customer_rows = []
for customer_id in range(1, 5001):
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    city = random.choice(list(CITIES))
    signup_date = start_date - timedelta(days=random.randint(1, 1800))
    email_name = (
        f"{customer_id}.{first}.{last}".lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
    customer_rows.append(
        {
            "cliente_id": f"CLI-{customer_id:07d}",
            "nombre_completo": f"{first} {last}",
            "numero_documento": str(1000000000 + customer_id * 37),
            "correo_electronico": f"{email_name}@clientes.example.com",
            "telefono_movil": f"+57 3{customer_id % 10:01d}0 {1000000 + customer_id:07d}",
            "direccion_residencia": random.choice(CITIES[city]),
            "ciudad": city,
            "codigo_postal": f"{50000 + customer_id % 40000:06d}",
            "fecha_nacimiento": date(1960, 1, 1)
            + timedelta(days=random.randint(6500, 22000)),
            "fecha_registro": signup_date,
            "segmento": random.choice(["Hogar", "Maestro", "Arquitecto", "Constructor"]),
            "autoriza_contacto": bool(random.getrandbits(1)),
        }
    )

df_customers = pd.DataFrame(customer_rows)
(
    spark.createDataFrame(df_customers)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{CUSTOMER_SCHEMA}.clientes_sinteticos")
)
print(f"clientes_sinteticos: {len(df_customers):,} filas")

supplier_rows = []
for supplier_id in range(1, 301):
    city = random.choice(list(CITIES))
    supplier_rows.append(
        {
            "proveedor_id": f"PRV-{supplier_id:05d}",
            "razon_social": f"Proveedor Circular {supplier_id:03d} SAS",
            "nit": f"900{supplier_id:06d}-{supplier_id % 9}",
            "nombre_contacto": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            "email_contacto": f"contacto{supplier_id}@proveedores.example.com",
            "telefono_contacto": f"+57 601 {7000000 + supplier_id:07d}",
            "direccion": random.choice(CITIES[city]),
            "ciudad": city,
            "cuenta_bancaria_enmascarada": f"****{supplier_id:04d}",
            "categoria": random.choice(
                ["Minerales", "Empaques", "Logística", "Químicos", "Servicios"]
            ),
        }
    )

df_suppliers = pd.DataFrame(supplier_rows)
(
    spark.createDataFrame(df_suppliers)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{CUSTOMER_SCHEMA}.proveedores_sinteticos")
)
print(f"proveedores_sinteticos: {len(df_suppliers):,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cargar el paquete completo de GreenSheen

# COMMAND ----------

response = requests.get(asset_url, timeout=120)
response.raise_for_status()

zip_path = Path("/tmp/greensheen_demo.zip")
zip_path.write_bytes(response.content)

with ZipFile(zip_path) as archive:
    archive.extractall(VOLUME_ROOT)

all_files = sorted(path for path in VOLUME_ROOT.rglob("*") if path.is_file())
print(f"Archivos GreenSheen cargados: {len(all_files)}")
for folder in ["greensheeninvoices", "Handwritten", "knowledgebase", "calltranscripts"]:
    count = sum(1 for path in all_files if folder in path.parts)
    print(f"  {folder}: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Convertir documentos de GreenSheen a tablas Delta
# MAGIC
# MAGIC Agent Bricks trabaja mejor cuando recibe fuentes claras. El notebook extrae texto
# MAGIC de los PDF y TXT. La factura manuscrita incluye una transcripción sintética para
# MAGIC que el ejercicio de Information Extraction sea reproducible.

# COMMAND ----------

def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def extract_pdf(path: Path) -> str:
    reader = PdfReader(str(path))
    return clean_text(" ".join(page.extract_text() or "" for page in reader.pages))


invoice_records = []
for path in sorted((VOLUME_ROOT / "greensheeninvoices").glob("*.pdf")):
    invoice_records.append(
        {
            "document_id": path.stem,
            "file_name": path.name,
            "document_path": str(path),
            "document_type": "digital_pdf",
            "text": extract_pdf(path),
        }
    )

handwritten_path = VOLUME_ROOT / "Handwritten" / "HandwrittenInvoice1.jpg"
invoice_records.append(
    {
        "document_id": "HandwrittenInvoice1",
        "file_name": handwritten_path.name,
        "document_path": str(handwritten_path),
        "document_type": "handwritten_jpg",
        "text": (
            "Supplier: Jyotsna. Address: 2101 Saratoga Drive, NJ 08550, USA. "
            "PO number: 2123456. Bill to: GreenSheen Paints. "
            "Line item 1: QuickDry Additive, quantity 10, rate 10, total 100. "
            "Line item 2: Sealer, quantity 10, rate 25, total 250. "
            "Subtotal 350. Tax 24.50. Total due 374.50."
        ),
    }
)

df_invoice_text = pd.DataFrame(invoice_records)
(
    spark.createDataFrame(df_invoice_text)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GREENSHEEN_SCHEMA}.invoice_text")
)

knowledge_records = []
for path in sorted((VOLUME_ROOT / "knowledgebase").iterdir()):
    if path.suffix.lower() == ".pdf":
        text = extract_pdf(path)
    elif path.suffix.lower() == ".txt":
        text = clean_text(path.read_text(encoding="utf-8"))
    else:
        continue
    knowledge_records.append(
        {
            "document_id": path.stem.lower().replace(" ", "_"),
            "file_name": path.name,
            "document_path": str(path),
            "text": text,
        }
    )

df_knowledge = pd.DataFrame(knowledge_records)
(
    spark.createDataFrame(df_knowledge)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GREENSHEEN_SCHEMA}.knowledge_base")
)

transcript_records = []
for path in sorted((VOLUME_ROOT / "calltranscripts").glob("*.pdf")):
    transcript_records.append(
        {
            "document_id": path.stem.lower(),
            "file_name": path.name,
            "document_path": str(path),
            "text": extract_pdf(path),
        }
    )

df_transcripts = pd.DataFrame(transcript_records)
(
    spark.createDataFrame(df_transcripts)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GREENSHEEN_SCHEMA}.call_transcripts")
)

COACHING_BY_FILE = {
    "GreenSheen_Transcript_1.pdf": (
        "Summary and sentiment: The customer is frustrated by application and durability "
        "issues across four products. Strengths: Sophie acknowledges frustration, asks "
        "diagnostic questions and offers guides, replacement and specialist support. "
        "Improvement areas: she should confirm the promised follow-up owner and timing for "
        "each product. Coaching: recap the agreed actions before closing. Training use: "
        "practice de-escalation and evidence-based troubleshooting. Escalation: review "
        "label clarity for RustBlock odor and RainShield moisture guidance."
    ),
    "GreenSheen_Transcript_2.pdf": (
        "Summary and sentiment: The customer remains dissatisfied with finish quality, "
        "color expectations and unclear product guidance. Strengths: Jenna validates the "
        "feedback and gives concrete technique advice. Improvement areas: she offers help "
        "without confirming a resolution acceptable to the customer. Coaching: use a clear "
        "issue-action-owner recap. Training use: handle multiple complaints in one call. "
        "Escalation: improve AquaGlow, VividHue and EcoSeal instructions."
    ),
    "GreenSheen_Transcript_3.pdf": (
        "Summary and sentiment: The customer has lost trust after drying, texture and "
        "additive problems. Strengths: Liam asks about temperature, humidity and coat "
        "thickness, then offers refund and replacement options. Improvement areas: avoid "
        "moving to a solution before confirming the customer's process. Coaching: summarize "
        "diagnosis and next steps. Training use: troubleshooting under pressure. Escalation: "
        "review cold-paint guidance for QuickDry Additive."
    ),
    "GreenSheen_Transcript_Positive.pdf": (
        "Summary and sentiment: The customer is highly satisfied and plans to recommend "
        "GreenSheen. Strengths: Rachel responds warmly, recognizes the product combination "
        "and confirms AquaGlow's fit for humid rooms. No material improvement area was "
        "found because the call achieved its purpose and closed clearly. Coaching: use this "
        "call as a positive example of concise recognition. Training use: reinforce advocacy."
    ),
    "GreenSheen_Call_Transcripts.pdf": (
        "Summary and sentiment: The document contains several customer interactions with "
        "mixed sentiment. Strengths: agents acknowledge concerns and provide product-specific "
        "guidance. Improvement areas: every call should end with an explicit action, owner "
        "and timing. Coaching: cite the customer's words before offering a diagnosis. "
        "Training use: compare de-escalation patterns. Escalation: consolidate recurring "
        "feedback about labels, preparation and humidity."
    ),
}

training_records = []
for index in range(120):
    source = transcript_records[index % len(transcript_records)]
    scenario = [
        "First contact",
        "Repeat contact",
        "Contractor account",
        "Homeowner account",
        "Escalated follow-up",
    ][index % 5]
    training_records.append(
        {
            "example_id": f"CALL-TRAIN-{index + 1:04d}",
            "scenario": scenario,
            "transcript": f"Scenario: {scenario}. {source['text']}",
            "coaching_reference": COACHING_BY_FILE[source["file_name"]],
            "source_file": source["file_name"],
        }
    )

df_training = pd.DataFrame(training_records)
(
    spark.createDataFrame(df_training)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GREENSHEEN_SCHEMA}.call_transcript_training")
)

print(f"invoice_text: {len(df_invoice_text):,} documentos")
print(f"knowledge_base: {len(df_knowledge):,} documentos")
print(f"call_transcripts: {len(df_transcripts):,} documentos")
print(f"call_transcript_training: {len(df_training):,} ejemplos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Historial de clientes y pedidos para Genie y el Supervisor Agent

# COMMAND ----------

COMPANIES = [
    "Vesta Builders",
    "Northstar Renovations",
    "Blue Peak Contractors",
    "Urban Habitat Design",
    "Summit Property Group",
    "Cedar Lane Homes",
]
PRODUCTS = [
    "EcoGuard_Primer",
    "NatureTint_Base",
    "RustBlock_Enamel",
    "AquaGlow_Semi_Gloss",
    "Rain_Shield_Exterior",
]

green_customers = [
    {
        "customer_id": "GS-C001",
        "customer_name": "Jane Doe",
        "company": "Vesta Builders",
        "email": "jane.doe@vesta.example.com",
        "phone": "+1 555 010 1001",
        "account_tier": "Gold",
        "region": "West",
    }
]
for index in range(2, 101):
    first = random.choice(["Alex", "Morgan", "Taylor", "Jordan", "Casey", "Riley"])
    last = random.choice(["Smith", "Brown", "Davis", "Wilson", "Clark", "Lewis"])
    company = random.choice(COMPANIES)
    green_customers.append(
        {
            "customer_id": f"GS-C{index:03d}",
            "customer_name": f"{first} {last}",
            "company": company,
            "email": f"customer{index}@greensheen-demo.example.com",
            "phone": f"+1 555 020 {index:04d}",
            "account_tier": random.choice(["Standard", "Silver", "Gold"]),
            "region": random.choice(["West", "Midwest", "South", "Northeast"]),
        }
    )

green_orders = [
    {
        "order_id": "GS-O0001",
        "customer_id": "GS-C001",
        "customer_name": "Jane Doe",
        "company": "Vesta Builders",
        "order_date": date.today() - timedelta(days=45),
        "product": "EcoGuard_Primer",
        "product_version": "Low VOC v3",
        "quantity_gallons": 240,
        "order_status": "Delivered",
        "support_notes": "Asked whether the primer can be tinted for a hospitality project.",
    }
]
for index in range(2, 801):
    customer = random.choice(green_customers)
    green_orders.append(
        {
            "order_id": f"GS-O{index:04d}",
            "customer_id": customer["customer_id"],
            "customer_name": customer["customer_name"],
            "company": customer["company"],
            "order_date": date.today() - timedelta(days=random.randint(1, 720)),
            "product": random.choice(PRODUCTS),
            "product_version": random.choice(["Standard v2", "Low VOC v3", "Pro v1"]),
            "quantity_gallons": random.choice([25, 50, 100, 200, 600, 1000]),
            "order_status": random.choice(["Delivered", "In Transit", "Processing"]),
            "support_notes": random.choice(
                [
                    None,
                    "Requested an SDS document.",
                    "Asked about drying time in high humidity.",
                    "Requested color matching guidance.",
                ]
            ),
        }
    )

(
    spark.createDataFrame(pd.DataFrame(green_customers))
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GREENSHEEN_SCHEMA}.customers")
)
(
    spark.createDataFrame(pd.DataFrame(green_orders))
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GREENSHEEN_SCHEMA}.customer_orders")
)

print(f"GreenSheen customers: {len(green_customers):,}")
print(f"GreenSheen customer_orders: {len(green_orders):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validación final

# COMMAND ----------

expected_tables = {
    OPERATIONS_SCHEMA: ["produccion_calidad", "consumo_recursos"],
    CUSTOMER_SCHEMA: ["clientes_sinteticos", "proveedores_sinteticos"],
    GREENSHEEN_SCHEMA: [
        "invoice_text",
        "knowledge_base",
        "call_transcripts",
        "call_transcript_training",
        "customers",
        "customer_orders",
    ],
}

print("=" * 72)
print("CORONA DAY 1 WORKSHOP")
print("=" * 72)
for schema, tables in expected_tables.items():
    for table in tables:
        count = spark.table(f"{CATALOG}.{schema}.{table}").count()
        print(f"{CATALOG}.{schema}.{table}: {count:,} filas")

sample = spark.sql(
    f"""
    SELECT customer_name, company, product, product_version, quantity_gallons
    FROM {CATALOG}.{GREENSHEEN_SCHEMA}.customer_orders
    WHERE customer_name = 'Jane Doe' AND company = 'Vesta Builders'
    """
)
display(sample)

print()
print("Listo. Abre la app del taller y empieza por la Tarea 1.")
