# Databricks notebook source
# MAGIC %md
# MAGIC # Simular un incidente de calidad
# MAGIC
# MAGIC Ejecuta este notebook después de crear y refrescar el perfil de calidad sobre
# MAGIC `corona_workshop.operaciones.produccion_calidad`.
# MAGIC
# MAGIC El notebook agrega un lote controlado con una subida de defectos, temperatura y
# MAGIC consumo de energía en Sabaneta. Puedes refrescar el perfil y comparar el cambio.

# COMMAND ----------

from datetime import date, datetime, timedelta
import random

import pandas as pd

CATALOG = "corona_workshop"
SCHEMA = "operaciones"
TABLE = "produccion_calidad"
INCIDENT_ID = "INC-CALIDAD-SABANETA"

random.seed(2026)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mantener la ejecución idempotente

# COMMAND ----------

spark.sql(
    f"""
    DELETE FROM {CATALOG}.{SCHEMA}.{TABLE}
    WHERE incidente_id = '{INCIDENT_ID}'
    """
)

# COMMAND ----------

incident_rows = []
incident_date = date.today()

for index in range(1, 181):
    shift = ["A", "B", "C"][index % 3]
    event_time = datetime.combine(incident_date, datetime.min.time()) + timedelta(
        hours={"A": 5, "B": 13, "C": 21}[shift],
        minutes=index,
    )
    incident_rows.append(
        {
            "lote_id": f"INC-{incident_date:%Y%m%d}-{index:04d}",
            "fecha_produccion": incident_date,
            "fecha_evento": event_time,
            "planta": "Sabaneta",
            "ciudad": "Medellín",
            "linea": "Horno-2",
            "turno": shift,
            "familia_producto": "Pisos",
            "sku": None if index % 4 == 0 else "PISO-ANDINO-60",
            "toneladas_producidas": round(random.uniform(28, 38), 2),
            "tasa_defectos": round(random.uniform(0.21, 0.38), 4),
            "humedad_pct": round(random.uniform(8.8, 11.5), 2),
            "temperatura_proceso_c": round(random.uniform(1305, 1390), 1),
            "energia_kwh_ton": round(random.uniform(880, 1040), 2),
            "estado_calidad": "Revisar",
            "incidente_id": INCIDENT_ID,
        }
    )

spark.createDataFrame(pd.DataFrame(incident_rows)).write.mode("append").saveAsTable(
    f"{CATALOG}.{SCHEMA}.{TABLE}"
)

print(f"Incidente agregado: {len(incident_rows)} filas")
print(f"Tabla: {CATALOG}.{SCHEMA}.{TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar el contraste

# COMMAND ----------

comparison = spark.sql(
    f"""
    SELECT
      CASE
        WHEN incidente_id = '{INCIDENT_ID}' THEN 'Incidente'
        ELSE 'Línea base'
      END AS periodo,
      COUNT(*) AS lotes,
      ROUND(AVG(tasa_defectos), 4) AS tasa_defectos_promedio,
      ROUND(AVG(temperatura_proceso_c), 1) AS temperatura_promedio_c,
      ROUND(AVG(energia_kwh_ton), 1) AS energia_promedio_kwh_ton,
      ROUND(AVG(CASE WHEN sku IS NULL THEN 1.0 ELSE 0.0 END), 4) AS fraccion_sku_nulo
    FROM {CATALOG}.{SCHEMA}.{TABLE}
    GROUP BY 1
    ORDER BY 1
    """
)

display(comparison)
print("Ahora vuelve al perfil de calidad y selecciona Refresh.")
