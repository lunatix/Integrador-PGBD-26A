# ============================================================
# PIPELINE MEDALLION (src/pipeline.py)
# ============================================================
#
# Ejecuta de principio a fin la arquitectura medallion:
#   BRONZE -> CSV crudos a Parquet sin transformar  (src.ingestion.loader)
#   SILVER -> limpieza y validación por fuente      (src.transformations.clean, src.quality.validators)
#   GOLD   -> dataset maestro analítico             (src.transformations.integrate)
#
# Cada capa se etiqueta en la Spark UI y sus métricas (jobs, tareas,
# MB leídos/escritos, shuffle) quedan en reports/evidencia_ejecucion_act11.txt.
# notebooks/01_ingesta.ipynb llama a estas mismas funciones capa por
# capa para mostrar el plan físico de Spark en cada paso.
#
# Uso desde la raíz del repositorio:
#   python -m src.pipeline
# ============================================================

from pyspark.sql import SparkSession, DataFrame
from src.ingestion.loader import (
    get_spark, extract_sources, to_bronze, write_layer, read_layer,
    BRONZE_DIR, SILVER_DIR, REPORTS_DIR,
)
from src.transformations.clean import clean_all
from src.quality.validators import validate_raw, validate_clean_sources, validate_master
from src.transformations.integrate import build_master_dataset, save_and_verify_parquet
from src.monitoring import etapa, metricas, entorno


def capa_bronze(spark: SparkSession) -> dict[str, int]:
    """CSV crudos -> data/bronze (una lectura del CSV de SHCP). Devuelve filas por fuente."""
    etapa(spark, "bronze")
    write_layer(to_bronze(extract_sources(spark)), BRONZE_DIR)
    return validate_raw(read_layer(spark, BRONZE_DIR))


def capa_silver(spark: SparkSession) -> tuple[dict[str, DataFrame], dict[str, dict]]:
    """data/bronze -> limpieza -> data/silver -> validación de calidad sobre lo escrito."""
    etapa(spark, "silver")
    write_layer(clean_all(read_layer(spark, BRONZE_DIR)), SILVER_DIR)
    silver = read_layer(spark, SILVER_DIR)
    return silver, validate_clean_sources(silver)


def capa_gold(spark: SparkSession, silver: dict[str, DataFrame]) -> tuple[DataFrame, dict, object]:
    """data/silver -> dataset maestro -> data/gold (Parquet particionado por año)."""
    etapa(spark, "gold")
    maestro = build_master_dataset(silver)
    reporte = validate_master(maestro)
    return maestro, reporte, save_and_verify_parquet(spark, maestro)


def run() -> None:
    spark = get_spark()
    lineas = []

    def log(msg: str = "") -> None:
        print(msg)
        lineas.append(msg)

    def log_spark(capa: str) -> None:
        m = metricas(spark, capa)
        log(
            f"   Spark: {m['jobs']} jobs | {m['tareas']} tareas | lee {m['lectura_MB']} MB | "
            f"escribe {m['escritura_MB']} MB | shuffle {m['shuffle_KB']} KB | {m['tiempo_tareas_s']} s de tareas"
        )

    try:
        log("============================================================")
        log("EJECUCIÓN PIPELINE MEDALLION - ACTIVIDAD 11")
        log("============================================================")
        log("Configuración Spark: " + " | ".join(f"{k}={v}" for k, v in entorno(spark).items() if k != "Spark UI"))

        log("\n1. BRONZE (data/raw -> data/bronze, Parquet sin transformar)")
        for fuente, n in capa_bronze(spark).items():
            log(f"   - {fuente:5}: {n:>9,} filas")
        log_spark("bronze")

        log("\n2. SILVER (limpieza, homologación y validación de calidad)")
        log("   - Llave común `fecha` (primer día de mes, tipo date)")
        log("   - SHCP filtrado por claves XBC, deduplicado de registros idénticos (2018-2025) y convertido de miles a millones (/1000)")
        log("   - SAT limpio de marcas preliminares (*)")
        log("   - IGAE transformado de formato ancho a largo (serie total base 2018=100)")
        silver, rep_silver = capa_silver(spark)
        log("   Validación en ventana 2015-01-01 a 2025-03-01:")
        for fuente, info in rep_silver.items():
            log(
                f"   - {fuente:5}: {info['filas_ventana']:>4} filas | "
                f"{info['meses_unicos']} meses únicos | {info['rango']}"
            )
        log_spark("silver")

        log("\n3. GOLD (dataset maestro analítico)")
        maestro, rep_gold, ruta = capa_gold(spark, silver)
        log(
            f"   - DataFrame maestro: {rep_gold['filas']} filas x {rep_gold['columnas']} columnas "
            f"| Granularidad: 1 fila = 1 mes ({rep_gold['rango']})"
        )
        log(f"   - Salida verificada en: {ruta.relative_to(ruta.parents[2])}")
        log_spark("gold")

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        (REPORTS_DIR / "evidencia_ejecucion_act11.txt").write_text("\n".join(lineas), encoding="utf-8")

        print("\n=== ESQUEMA FINAL Y MUESTRA DE RESULTADOS ===")
        maestro.printSchema()
        maestro.select(
            "fecha", "anio", "shcp_xbc", "sat_total_tributarios",
            "ratio_tributario_gasto", "igae_total", "inpc", "igae_var_anual_pct"
        ).show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
