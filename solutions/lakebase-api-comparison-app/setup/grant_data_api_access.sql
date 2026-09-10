-- Ejecuta este archivo antes de adjuntar el recurso Postgres a la app.
-- Reemplaza el valor una sola vez con service_principal_client_id de la app.
-- Ejecuta en Lakebase SQL Editor como propietario del proyecto.

CREATE EXTENSION IF NOT EXISTS databricks_auth;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = '<APP_SERVICE_PRINCIPAL_CLIENT_ID>'
  ) THEN
    PERFORM databricks_create_role(
      '<APP_SERVICE_PRINCIPAL_CLIENT_ID>',
      'SERVICE_PRINCIPAL'
    );
  END IF;
END
$$;

GRANT "<APP_SERVICE_PRINCIPAL_CLIENT_ID>" TO authenticator;
GRANT USAGE ON SCHEMA public TO "<APP_SERVICE_PRINCIPAL_CLIENT_ID>";
GRANT SELECT ON TABLE public.produccion_calidad
  TO "<APP_SERVICE_PRINCIPAL_CLIENT_ID>";
