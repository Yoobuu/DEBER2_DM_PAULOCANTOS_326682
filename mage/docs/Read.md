# Proyecto DM2025 – NYC TLC Pipeline (Bronze → Silver → Gold)

##  Descripción general
Pipeline implementado en **Mage + Snowflake** para ingestión de datos de taxis de Nueva York (NYC TLC) en formato Parquet.

- Fuente: https://d37ci6vzurychx.cloudfront.net/trip-data/
- Arquitectura: Bronze (raw) → Silver (curated) → Gold (aggregated)
- Entorno: Mage.ai (Docker), Snowflake (WH_DM, DB=DM_NYCTLC)

##  Bloques implementados
| Nivel | Bloque | Descripción |
|-------|---------|-------------|
| Bronze | `stage_one_parquet.py` | Descarga y subida de Parquet a Snowflake Stage |
| Bronze | `bronze_load_yellow_2019_01.py` | Carga de Parquet a tabla BRONZE |
| Silver | `silver_yellow_2019_01.py` | Limpieza y normalización hacia TRIPS_YELLOW |
| Gold | `gold_yellow_monthly.py` | Métricas mensuales agregadas por mes |
| Docs | `coverage_update.py` | Actualiza la matriz de cobertura CSV |

##  Período procesado
| Service | Year | Months loaded | Rows promedio |
|----------|------|----------------|----------------|
| yellow | 2019 | 01, 02, 03 | ~7.5M por mes |

📄 Ver `coverage.csv` para detalle de filas por mes.

##  Evidencias
Incluye capturas de:
- Stage OK (Mage log con “✅ Subida completada”)
- COPY OK (Bronze)
- Insert OK (Silver)
- GOLD con métricas
- coverage.csv actualizado

##  Conclusiones
- El pipeline funciona con 3 meses de datos (enero–marzo 2019).
- Escalable a todos los meses/años (solo cambiando variables en Mage).
- Cumple arquitectura ETL en 3 capas con idempotencia.
