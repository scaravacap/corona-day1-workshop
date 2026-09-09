# Corona Databricks Day 1 Workshop

Taller práctico para el día 1 del Deep Dive de Corona en Medellín. El contenido
convierte cuatro bloques de la agenda en tareas ejecutables:

1. Gobierno con Governance Hub, consumo, Data Quality Monitoring y Data Classification.
2. Agent Bricks con los cuatro casos de la empresa ficticia GreenSheen.
3. Gobierno de LLMs con rate limits, PII y temas en Unity AI Gateway.
4. Una Databricks App creada con Genie Code, datos operacionales y un modelo fundacional.

Yo preparé el repositorio para que ustedes puedan seguir el taller desde una
Databricks App y crear todos los datos con un solo notebook. Las tareas 1, 2 y 3
usan la interfaz gráfica. Solo la tarea 4 usa Genie Code.

Todos los datos son sintéticos.

## Contenido

```text
corona-day1-workshop/
├── app.yaml
├── main.py
├── requirements.txt
├── data/
│   └── tasks.json
├── frontend/
│   ├── index.html
│   └── img/
├── notebooks/
│   ├── 01_setup_workshop.py
│   └── 02_inject_quality_incident.py
├── dashboards/
│   └── Account_Usage_Dashboard_v2.lvdash.json
└── assets/
    ├── greensheen_demo.zip
    └── greensheen/
```

La app solo sirve instrucciones. No consulta datos ni necesita recursos de
Databricks durante su ejecución.

## Preparación

### 1. Crear o abrir una cuenta de Databricks

Puedes usar [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
para recorrer el taller. Algunas funciones de gobierno dependen del tipo de
workspace, permisos o previews habilitados. La app indica qué resultado esperar
cuando una función no está disponible.

### 2. Clonar el repositorio en un Git Folder

En el workspace:

1. Abre **Workspace**.
2. Selecciona **Create** y luego **Git folder**.
3. Pega `https://github.com/scaravacap/corona-day1-workshop`.
4. Usa la rama `main`.

### 3. Generar los datos

Abre `notebooks/01_setup_workshop.py`, conecta compute serverless y selecciona
**Run all**.

El notebook crea:

- `corona_workshop.operaciones.produccion_calidad`
- `corona_workshop.operaciones.consumo_recursos`
- `corona_workshop.clientes.clientes_sinteticos`
- `corona_workshop.clientes.proveedores_sinteticos`
- `corona_workshop.greensheen.invoice_text`
- `corona_workshop.greensheen.knowledge_base`
- `corona_workshop.greensheen.call_transcripts`
- `corona_workshop.greensheen.call_transcript_training`
- `corona_workshop.greensheen.customers`
- `corona_workshop.greensheen.customer_orders`
- `corona_workshop.ai_governance`
- `/Volumes/corona_workshop/assets/greensheen_files/`

El paquete GreenSheen incluye 106 facturas PDF, una factura manuscrita, cinco
transcripciones, manuales de producto y preguntas frecuentes. El notebook
descarga el ZIP desde este repositorio, carga los archivos al volumen y crea las
tablas de texto que usan los ejercicios.

### 4. Crear la app de instrucciones

Desde la interfaz:

1. Abre **New** y luego **App**.
2. Selecciona **Custom**.
3. Usa **Git repository** como origen.
4. Pega `https://github.com/scaravacap/corona-day1-workshop`.
5. Usa `main` como rama y `corona-day1-workshop` como nombre.
6. Crea la app y abre su URL.

Si el workspace no ofrece deploy automático desde Git, usa un snapshot:

```bash
databricks apps create corona-day1-workshop --profile <perfil>
databricks apps start corona-day1-workshop --profile <perfil>
databricks sync . /Workspace/Users/<correo>/corona-day1-workshop-src \
  --full --exclude 'notebooks/**' --exclude 'assets/greensheen/**' --exclude '.git'
databricks apps deploy corona-day1-workshop \
  --source-code-path /Workspace/Users/<correo>/corona-day1-workshop-src \
  --mode SNAPSHOT --profile <perfil>
```

## Tarea 1: Gobierno

Descarga `dashboards/Account_Usage_Dashboard_v2.lvdash.json` e impórtalo desde
**SQL > Dashboards > Create dashboard > Import dashboard from file**.

El dashboard usa tablas de sistema para consumo, precios, workspaces, compute,
Lakeflow y Model Serving. Necesitas acceso a los schemas correspondientes. En
Free Edition puedes importar el archivo aunque el historial de billing esté vacío.

Después:

- Habilita Anomaly Detection en `corona_workshop.operaciones`.
- Crea un perfil de serie de tiempo sobre `produccion_calidad`.
- Ejecuta `notebooks/02_inject_quality_incident.py`.
- Refresca el perfil para comparar la línea base con el incidente.
- Habilita Data Classification para `clientes` y `greensheen`.

Anomaly Detection aprende la cadencia de commits para freshness y completeness.
Un schema nuevo no genera una historia temporal instantánea. Data Classification
también puede tardar hasta 24 horas en mostrar detecciones nuevas. En la cuenta
Free Edition de validación, su activación devolvió `The required model is not
available for this workspace`. Usa el workspace de Corona si tu cuenta muestra
el mismo límite.

Documentación:

- [Data Quality Monitoring](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-quality-monitoring/)
- [Data Classification](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-classification)

## Tarea 2: Agent Bricks

La app guía cuatro casos de GreenSheen:

- Information Extraction sobre facturas digitales y manuscritas.
- Knowledge Assistant sobre manuales y preguntas frecuentes.
- Information Extraction para análisis y coaching estructurado de llamadas.
- Multi-Agent Supervisor con Genie para historial de pedidos y un Knowledge Assistant para documentación.

El caso final usa a Jane Doe de Vesta Builders y `EcoGuard_Primer`, igual que el
flujo del demo de referencia. La tabla de entrenamiento trae 120 ejemplos.

El deck original usa Custom LLM para el tercer caso. Ese tile no aparece en la
interfaz de Free Edition validada el 9 de septiembre de 2026, así que el taller
actual usa Information Extraction con un esquema de coaching. Si el workspace de
Corona conserva Custom LLM, puedes ejecutar ambos y comparar.

La cuenta Free Edition validada el 9 de septiembre de 2026 muestra Knowledge
Assistant dentro de **Agents**. La página pública de limitaciones todavía afirma
lo contrario, así que el taller usa la capacidad observada en el producto vivo y
pide confirmar el rollout si otra cuenta no muestra el tile.

## Tarea 3: Unity AI Gateway

La cuenta Free Edition usada para la validación mostró ocho modelos de chat:
GPT OSS 120B y 20B, Qwen3 Next 80B, Qwen3.5 122B, Llama 4 Maverick, Gemma 3 12B,
Llama 3.1 8B y Llama 3.3 70B. El taller usa `databricks-gpt-oss-120b`.

El flujo actual de Model Services también quedó validado en Free Edition. Se
creó `corona_workshop.ai_governance.quality_chat` con GPT OSS 120B como destino.

La app propone:

- 60 requests por minuto para el servicio.
- 10 requests por minuto por usuario.
- 20.000 tokens por minuto por usuario.
- PII redaction para entrada y PII blocking para salida.
- Unsafe Content para entrada y salida.
- Una service policy personalizada para limitar el servicio a calidad, producción,
  recursos, mantenimiento, sostenibilidad y documentación técnica.

`valid_topics` e `invalid_keywords` pertenecen al API anterior y están
deprecados. El taller usa service policies en el camino actual de Unity Gateway.
Estas políticas están en Beta al 2 de septiembre de 2026 y un account admin debe
habilitarlas desde Previews. La cuenta Free Edition de validación expone Model
Services en Unity Gateway; usa Policies si la pestaña está visible y, si no lo
está, limita el ejercicio a rate limits y PII en el endpoint.

Estos valores sirven para aprender el mecanismo. Una política de producción debe
partir de demanda, latencia, presupuesto y criticidad.

## Tarea 4: Databricks App con Genie Code

La tarjeta incluye un prompt completo para crear `corona-quality-copilot`, un
Centro de Decisiones de Calidad que:

- consulta producción y consumo de recursos;
- muestra KPIs con período, comparación, frescura y fuente;
- destaca lotes que requieren revisión;
- genera un briefing de turno en español con un endpoint de chat;
- conserva los KPIs si el modelo falla;
- muestra el contexto numérico enviado al modelo.

El prompt no contiene tokens ni IDs de recursos. Genie Code debe declarar el SQL
warehouse con `CAN_USE` y el serving endpoint con `CAN_QUERY`.

## Ejecutar la app localmente

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Abre `http://localhost:8000`.

Verificaciones rápidas:

```bash
curl http://localhost:8000/api/health
curl http://localhost:8000/api/tasks
curl http://localhost:8000/api/tasks/governance
```

## Fuentes

- [Agenda del día 1 de Corona](./data/tasks.json)
- [Agent Bricks Demo Setup Guide, GreenSheen](https://docs.google.com/presentation/d/1pLE3B8ih8cwGgQ-V3dFsuPdt7Q4v3t0WdNHz5lqXa3I/edit)
- [Unity AI Gateway](https://learn.microsoft.com/en-us/azure/databricks/ai-gateway/)
- [Databricks Apps](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/)
