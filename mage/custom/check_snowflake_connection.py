if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.shared.secrets import get_secret_value
import snowflake.connector

@custom
def transform_custom(*args, **kwargs):
    """
    Conecta a Snowflake usando Secrets y hace un SELECT de verificación.
    Forzamos timeouts para evitar bloqueos largos.
    """
    print(" Intentando conectar con Snowflake (timeouts cortos)...")

    user = get_secret_value('SNOWFLAKE_USER')
    password = get_secret_value('SNOWFLAKE_PASSWORD')
    account = get_secret_value('SNOWFLAKE_ACCOUNT')  
    role = get_secret_value('SNOWFLAKE_ROLE')
    warehouse = get_secret_value('SNOWFLAKE_WAREHOUSE')
    database = get_secret_value('SNOWFLAKE_DB')
    schema = get_secret_value('SNOWFLAKE_SCHEMA_BRONZE')

    
    print(f"Cuenta: {account}, Usuario: {user}, DB: {database}, Schema: {schema}, WH: {warehouse}, Rol: {role}")

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
        cur.execute("SELECT CURRENT_VERSION(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();")
        v, r, d, s = cur.fetchone()

        print(" Conexión exitosa a Snowflake!")
        print(f"Versión: {v}")
        print(f"Rol actual: {r}")
        print(f"Base de datos: {d}")
        print(f"Esquema: {s}")
        return {'version': v, 'role': r, 'database': d, 'schema': s}

    except Exception as e:
        print(" Error al conectar a Snowflake:")
        print(repr(e))
       
        raise
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
    assert output is not None, 'El output es None; la conexión no devolvió datos.'
