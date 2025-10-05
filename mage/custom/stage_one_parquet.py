
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.shared.secrets import get_secret_value
import snowflake.connector
import os
import pathlib
from urllib.request import urlopen
import uuid 

@custom
def transform_custom(*args, **kwargs):
    """
    Descarga 1 parquet (service/year/month) a /home/src/data/nyctlc/<service>/
    lo sube al stage @STG_NYCTLC_PARQUET en la ruta:
      service=<service>/year=<yyyy>/month=<mm>/
    Luego hace COPY INTO a BRONZE.<SERVICE>_PARQUET_RAW (con metadatos)
    y lista el stage para verificar.
    """
    
    if not kwargs.get('pipeline_runtime'):
        kwargs['pipeline_runtime'] = {
            'variables': {
                'service': 'green',
                'year': 2019,
                'month': 1,
            }
        }

    pr = kwargs.get('pipeline_runtime') or {}
    service = (pr.get('variables', {}).get('service', 'yellow') or 'yellow').lower()
    year = int(pr.get('variables', {}).get('year', 2019))
    month = int(pr.get('variables', {}).get('month', 1))
    mm = f'{month:02d}'
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{service}_tripdata_{year}-{mm}.parquet'
    stage_subdir = f"service={service}/year={year}/month={mm}/"
    print(f"➡️ Vars: service={service} year={year} month={month} mm={mm}")

    # Rutas locales
    base_dir = pathlib.Path('/home/src/data/nyctlc') / service
    base_dir.mkdir(parents=True, exist_ok=True)
    local_path = base_dir / f'{service}_tripdata_{year}-{mm}.parquet'

    print(f'🔽 Descargando: {url}')
    with urlopen(url) as r, open(local_path, 'wb') as f:
        chunk = r.read()
        f.write(chunk)

    size_mb = round(local_path.stat().st_size / (1024*1024), 2)
    print(f' Archivo descargado en {local_path} ({size_mb} MB)')

    
    user = get_secret_value('SNOWFLAKE_USER')
    password = get_secret_value('SNOWFLAKE_PASSWORD')
    account = get_secret_value('SNOWFLAKE_ACCOUNT')
    role = get_secret_value('SNOWFLAKE_ROLE')
    warehouse = get_secret_value('SNOWFLAKE_WAREHOUSE')
    database = get_secret_value('SNOWFLAKE_DB')               
    schema = get_secret_value('SNOWFLAKE_SCHEMA_BRONZE')      

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
        login_timeout=60,
        network_timeout=300,
        client_session_keep_alive=True,
        authenticator='snowflake',
        insecure_mode=True,   
    )

    cur = conn.cursor()

    try:
        
        cur.execute(f"USE ROLE {role}")
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(f"USE DATABASE {database}")
        cur.execute(f"USE SCHEMA {schema}")
        cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 1200")
        cur.execute("ALTER SESSION SET LOCK_TIMEOUT = 3600")

        
        print('☁️  Subiendo al stage @STG_NYCTLC_PARQUET ...')
        put_sql = (
            f"PUT file://{local_path} @STG_NYCTLC_PARQUET/{stage_subdir} "
            "AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL=1"
        )
        print(put_sql)
        cur.execute(put_sql)
        _ = cur.fetchall()
        print(' Subida completada.')

       
        dest_table = 'YELLOW_PARQUET_RAW' if service == 'yellow' else 'GREEN_PARQUET_RAW'
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{dest_table} (
          v VARIANT,
          src_file STRING,
          ingest_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
          src_service STRING,
          src_year INT,
          src_month INT,
          run_id STRING
        );
        """
        print(f'🛠️  Asegurando tabla destino: {schema}.{dest_table}')
        cur.execute(create_sql)

       
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        copy_sql = f"""
        COPY INTO {schema}.{dest_table} (v, src_file, ingest_ts, src_service, src_year, src_month, run_id)
        FROM (
          SELECT
            $1,
            METADATA$FILENAME,
            CURRENT_TIMESTAMP(),
            %s,      -- src_service
            %s,      -- src_year
            %s,      -- src_month
            %s       -- run_id
          FROM @STG_NYCTLC_PARQUET/{stage_subdir}
        )
        FILE_FORMAT = (FORMAT_NAME = {schema}.FF_PARQUET)
        ON_ERROR = 'ABORT_STATEMENT';
        """
        print('📥 Ejecutando COPY INTO (esto carga a BRONZE)...')
        cur.execute(copy_sql, (service, year, month, run_id))
        copy_result = cur.fetchall()
        print('✅ COPY completado.')
        
        list_sql = f"LIST @STG_NYCTLC_PARQUET/{stage_subdir}"
        cur.execute(list_sql)
        files = cur.fetchall()
        print(f'📦 Archivos en {stage_subdir}: {len(files)}')
        for row in files[:3]:
            print(' -', row[0])

        
        cur.execute(f"select count(*) from {schema}.{dest_table} where run_id = %s", (run_id,))
        inserted_count = cur.fetchone()[0]
        print(f' Filas insertadas en este run: {inserted_count}')

        return {
            "local_file": str(local_path),
            "size_mb": size_mb,
            "stage_prefix": stage_subdir,
            "files_in_stage": len(files),
            "dest_table": f"{schema}.{dest_table}",
            "run_id": run_id,
            "rows_inserted": inserted_count,
        }

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

@test
def test_output(output, *args) -> None:
    assert output is not None, "No hubo output."
    assert output.get("files_in_stage", 0) >= 1, "No se subió ningún archivo al stage."
    assert output.get("rows_inserted", 0) >= 1, "No se insertaron filas en BRONZE."
