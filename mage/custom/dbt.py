if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

from mage_ai.data_preparation.shared.secrets import get_secret_value
import os, subprocess

@custom
def transform_custom(*args, **kwargs):
    
    env = os.environ.copy()
    env['SNOWFLAKE_ACCOUNT']   = get_secret_value('SNOWFLAKE_ACCOUNT')
    env['SNOWFLAKE_USER']      = get_secret_value('SNOWFLAKE_USER')
    env['SNOWFLAKE_PASSWORD']  = get_secret_value('SNOWFLAKE_PASSWORD')
    env['SNOWFLAKE_ROLE']      = get_secret_value('SNOWFLAKE_ROLE')
    env['SNOWFLAKE_WAREHOUSE'] = get_secret_value('SNOWFLAKE_WAREHOUSE')
    env['SNOWFLAKE_DB']        = get_secret_value('SNOWFLAKE_DB')
    

    
    env['DBT_PROFILES_DIR'] = '/home/src/dbt'

    
    cmd = "cd /home/src/dbt && dbt run --select silver_trips_unified fct_trips --full-refresh --project-dir /home/src/dbt --profiles-dir /home/src/dbt"
    print(f"▶ Ejecutando: {cmd}")

   
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("Error ejecutando dbt build")

    print(" DBT ejecutado correctamente.")
