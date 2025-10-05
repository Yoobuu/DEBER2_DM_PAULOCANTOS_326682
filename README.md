
# Deber2 (PSet 2)

**Alumno:** Paulo Cantos  
**Fecha:** 5/10/2025  
**Codigo Banner:** 326682  

# NYCTLC — Pipeline Mage + Snowflake + dbt (Bronze → Silver → Gold)

## Objetivo
Construir un pipeline reproducible para ingestar, transformar y modelar datos de taxis NYC (NYC TLC) usando **Mage** (orquestación), **Snowflake** (almacenamiento/compute) y **dbt** (transformaciones y tests).

---

## Arquitectura (medallion)
- **BRONZE**: tablas crudas desde Parquet con metadatos (run_id, src_year, src_month, src_file, ingest_ts).
- **SILVER**: limpieza, unión de servicios y enriquecimiento con zonas; deduplicación por `trip_hash`.
- **GOLD**: tabla de hechos `FCT_TRIPS` y dimensión `DIM_ZONE` para analítica.
El pipeline sigue la arquitectura medallion, donde:
- La capa **Bronze** conserva los datos crudos de los taxis amarillos y verdes tal como se descargan desde los Parquet originales.
- La capa **Silver** aplica limpieza, normalización de columnas y enriquecimiento geográfico con zonas, unificando ambos servicios en una tabla incremental (`silver_trips_unified`).
- La capa **Gold** consolida los datos finales para análisis, generando una tabla de hechos (`fct_trips`) y una dimensión (`dim_zone`) con claves sustitutas.  
Estas capas se ejecutan de forma orquestada en Mage y son verificadas mediante tests automáticos de dbt.


Esquemas creados por dbt:
- `SILVER_SILVER` → staging + silver
- `SILVER_GOLD` → gold



## Componentes del pipeline (Mage)
1. `check_snowflake_connection.py` — verifica credenciales/rol/warehouse.
2. `sf_setup_bronze.py` — crea `FF_PARQUET`, `FF_CSV_TAXI_ZONES`, `STG_NYCTLC_PARQUET`.
3. `stage_one_parquet.py` — descarga Parquet, `PUT` al stage y `COPY INTO` a BRONZE (`YELLOW_PARQUET_RAW` / `GREEN_PARQUET_RAW`).
4. `zones_load.py` — carga `TAXI_ZONES_RAW` desde CSV.
5. **dbt (custom block con subprocess)** — `dbt build` usando `DBT_PROFILES_DIR=/home/src/dbt`.
6. `coverage_update.py` — escribe/actualiza `docs/coverage.csv`.

**Orden final**: Check → Setup Bronze → Stage Parquet → Zones Load → **DBT** → Coverage Update.

---

## Transformaciones dbt (resumen)
- **Staging**: `stg_trips_yellow` (fallback `tpep_*`), `stg_trips_green`, `stg_taxi_zones`.
- **Silver**: `silver_trips_unified` (incremental + `unique_key=trip_hash`, `merge`, dedup por `ingest_ts` y filtro de fechas válidas 2009–2025).
- **Gold**: `dim_zone` (surrogate key `zone_sk`) y `fct_trips` (incremental con FK a `dim_zone`).

**Tests (schema.yml)**  
- `not_null` en `trip_hash` (silver y gold).  
- `unique` en `dim_zone.zone_sk`.  
- `relationships` de `fct_trips.(pu_zone_sk/do_zone_sk)` → `dim_zone.zone_sk`.  
**Resultado:** `PASS=12, ERROR=0`.

---

## Estado actual de los datos (verificado)
- **BRONZE.Yellow 2019-01** cargado: ~7.7M filas del primer mes probado.
- **SILVER_SILVER.SILVER_TRIPS_UNIFIED**: **23,232,662** filas.
- **SILVER_GOLD.FCT_TRIPS**: **23,232,662** filas (post filtro 2009–2025).
- Fechas en `FCT_TRIPS`: **2009-01-01** → **2019-09-29**.

Ejemplos de validación:
```sql

USE ROLE ROLE_DM_SVC; USE WAREHOUSE WH_DM; USE DATABASE DM_NYCTLC;


SELECT COUNT(*) FROM SILVER_SILVER.SILVER_TRIPS_UNIFIED;
SELECT COUNT(*) FROM SILVER_GOLD.FCT_TRIPS;
SELECT MIN(pickup_datetime), MAX(pickup_datetime) FROM SILVER_GOLD.FCT_TRIPS;

## Verificación final y notebook
Se desarrolló un notebook de validación en Google Colab (`nyctlc_analysis.ipynb`) que:
- Conecta a Snowflake con las mismas credenciales de ROLE_DM_SVC.
- Ejecuta consultas de control de calidad (`COUNT`, `MIN/MAX`, `GROUP BY service_type`).
- Genera métricas resumidas de volumen y revenue por mes y tipo de servicio.

Ejemplo de resultados:
| YEAR_MONTH | SERVICE_TYPE | TRIPS | REVENUE |
|-------------|---------------|-------|----------|
| 2019-01 | yellow | 7,684,271 | 121,462,915.49 |
| 2019-01 | green  | 669,432 | 12,077,964.45 |
| 2019-02 | yellow | 7,032,323 | 131,422,971.51 |
| 2019-03 | yellow | 7,845,787 | 151,529,445.76 |
