
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.shared.secrets import get_secret_value
import snowflake.connector

@custom
def transform_custom(*args, **kwargs):
    """
    Crea FILE FORMAT Parquet y STAGE interno en DM_NYCTLC.BRONZE
    """
    user = get_secret_value('SNOWFLAKE_USER')
    password = get_secret_value('SNOWFLAKE_PASSWORD')
    account = get_secret_value('SNOWFLAKE_ACCOUNT')
    role = get_secret_value('SNOWFLAKE_ROLE')
    warehouse = get_secret_value('SNOWFLAKE_WAREHOUSE')
    database = get_secret_value('SNOWFLAKE_DB')
    schema = get_secret_value('SNOWFLAKE_SCHEMA_BRONZE')  

    print(f"Usando {account} / {database}.{schema} con WH {warehouse} y rol {role}")

    sql_commands = [
       
        f"USE ROLE {role}",
        f"USE WAREHOUSE {warehouse}",
        f"USE DATABASE {database}",
        f"USE SCHEMA {schema}",

       
        """
        CREATE OR REPLACE FILE FORMAT FF_PARQUET
          TYPE = PARQUET
        """,

       
        """
        CREATE STAGE IF NOT EXISTS STG_NYCTLC_PARQUET
          FILE_FORMAT = FF_PARQUET
          COMMENT = 'Stage interno para archivos Parquet'
        """,
    ]

    conn = None
    cur = None
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            login_timeout=20,
            network_timeout=20,
            client_session_keep_alive=False,
            authenticator='snowflake',
        )
        cur = conn.cursor()
        for i, cmd in enumerate(sql_commands, start=1):
            print(f"▶ Ejecutando {i}/{len(sql_commands)}:")
            print(cmd.strip())
            cur.execute(cmd)
        print(" FILE FORMAT y STAGE creados/asegurados en BRONZE.")

        
        cur.execute("LIST @STG_NYCTLC_PARQUET")
        rows = cur.fetchall()
        print(f"Contenido actual del stage: {len(rows)} archivo(s).")
        return {"stage_files": len(rows)}

    finally:
        try:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()
        except Exception:
            pass

@test
def test_output(output, *args) -> None:
    assert output is not None, "No hubo output."
    assert "stage_files" in output, "Falta métrica stage_files."
