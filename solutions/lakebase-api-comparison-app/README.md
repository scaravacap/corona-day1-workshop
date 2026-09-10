# Corona Lakehouse vs. Lakebase

Databricks App funcional para comparar la latencia percibida por una aplicación
al consultar los mismos lotes por dos caminos:

- Lakebase Data API sobre `public.produccion_calidad`.
- DBSQL Statement Execution API sobre
  `corona_workshop.operaciones.produccion_calidad`.

La interfaz ejecuta ambos requests en paralelo, muestra tiempo total, filas,
identificador de request, `Server-Timing` cuando está disponible y paridad de
los `lote_id`.

Esta medición no es un benchmark entre motores. Data API lee una copia
operacional sincronizada en Lakebase. Statement Execution ejecuta una consulta
analítica en un SQL warehouse. El resultado sirve para entender el efecto del
patrón de serving, el arranque de compute y el lag de sincronización.

## Recursos esperados

- Lakebase project: `projects/corona-reverse-etl`
- Branch: `projects/corona-reverse-etl/branches/production`
- Database:
  `projects/corona-reverse-etl/branches/production/databases/databricks-postgres`
- Synced table: `corona_lakebase.public.produccion_calidad`
- Fuente Delta: `corona_workshop.operaciones.produccion_calidad`
- SQL warehouse con permiso `CAN_USE`

## Importar como Databricks App

### 1. Subir el código

Desde la raíz del repositorio del taller:

```bash
databricks sync solutions/lakebase-api-comparison-app \
  /Workspace/Users/<correo>/corona-api-compare-src \
  --full --profile <perfil>
```

### 2. Crear la app y agregar el warehouse

1. Crea una Custom App llamada `corona-api-compare`.
2. Agrega un recurso SQL warehouse con la clave `sql-warehouse` y `CAN_USE`.
3. Copia `service_principal_client_id` desde los detalles de la app.
4. No agregues todavía el recurso Postgres.

### 3. Dar acceso a Data API

1. Abre `setup/grant_data_api_access.sql`.
2. Reemplaza `<APP_SERVICE_PRINCIPAL_CLIENT_ID>`.
3. Ejecuta el archivo en Lakebase SQL Editor como propietario del proyecto.
4. En Lakebase abre **Data API** y selecciona **Refresh schema cache**.

El archivo crea el rol de la identidad si hace falta, permite que
`authenticator` lo asuma y concede solo `USAGE` sobre `public` y `SELECT` sobre
la synced table.

El orden importa. Si adjuntas primero el recurso Postgres, Databricks crea el
rol sin entregar al propietario la opción administrativa necesaria para
concederlo a `authenticator`. Si ya lo adjuntaste, quita temporalmente el
recurso, ejecuta el SQL y vuelve a adjuntarlo.

### 4. Agregar Postgres y desplegar

1. Agrega el recurso Lakebase Postgres con la clave `postgres` y
   `CAN_CONNECT_AND_CREATE`.
2. Selecciona el proyecto, la rama y la base listados arriba.
3. Despliega la app.

```bash
databricks apps deploy corona-api-compare \
  --source-code-path /Workspace/Users/<correo>/corona-api-compare-src \
  --mode SNAPSHOT --profile <perfil>
```

`app.yaml` usa `valueFrom`, por lo que Databricks inyecta el warehouse,
`PGHOST`, `PGDATABASE`, `PGUSER`, `DATABRICKS_WORKSPACE_ID` y
`LAKEBASE_ENDPOINT`. La app construye la URL de Data API con esos valores. No
hay tokens, hosts ni IDs escritos en el código.

## Desplegar con el bundle

Usa este camino después de crear la app y ejecutar el bootstrap del rol en los
pasos 2 y 3. `databricks.yml` declara el estado final de ambos recursos. Pasa sus
valores sin escribirlos en el código:

```bash
databricks bundle validate \
  --var warehouse_id=<warehouse-id> \
  --var lakebase_branch=projects/<project>/branches/<branch> \
  --var lakebase_database=projects/<project>/branches/<branch>/databases/<database>

databricks apps deploy \
  --var warehouse_id=<warehouse-id> \
  --var lakebase_branch=projects/<project>/branches/<branch> \
  --var lakebase_database=projects/<project>/branches/<branch>/databases/<database>
```

Después ejecuta el grant de Data API descrito arriba.

## Ejecutar en local con datos mock

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
USE_MOCK_DATA=true python main.py
```

Abre `http://localhost:8000` y selecciona **Ejecutar ambos APIs**.

## Contrato de los endpoints

- `GET /api/config`: configuración y fuente de identidad.
- `GET /api/query/data-api?planta=Sabaneta&limit=25`: Data API.
- `GET /api/query/statement-api?planta=Sabaneta&limit=25`: Statement API.
- `GET /api/compare?planta=Sabaneta&limit=25`: ejecución paralela y paridad.
- `GET /api/health`: health check.

La consulta DBSQL usa parámetros enlazados. Data API usa filtros PostgREST,
`limit` y `order`. La app limita cada resultado a 100 filas.

## Documentación

- [Lakebase Data API](https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/data-api)
- [Statement Execution API](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/sql-execution-tutorial)
- [Lakebase synced tables](https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/sync-tables)
- [Databricks Apps](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/)
