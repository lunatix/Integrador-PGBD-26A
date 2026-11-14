# ============================================================
# PIPELINE MODULAR INTEGRADO (src/pipeline.py)
# ============================================================
#
# Ejecuta de principio a fin las etapas de la Actividad 11:
#   1. Extracción        -> src.ingestion.loader
#   2. Limpieza          -> src.transformations.clean
#   3. Validación        -> src.quality.validators
#   4. Integración       -> src.transformations.integrate
#   5. Carga y relectura -> src.transformations.integrate
#
# Uso desde la raíz del repositorio:
#   python -m src.pipeline
# ============================================================

from src.ingestion.loader import get_spark, extract_sources, REPORTS_DIR
from src.transformations.clean import clean_all
from src.quality.validators import validate_raw, validate_clean_sources, validate_master
from src.transformations.integrate import build_master_dataset, save_and_verify_parquet


def run() -> None:
    spark = get_spark()
    lineas_reporte = []

    def log(msg: str = "") -> None:
        print(msg)
        lineas_reporte.append(msg)

    try:
        log("============================================================")
        log("EJECUCIÓN PIPELINE MODULAR - ACTIVIDAD 11")
        log("============================================================")

        log("\n1. EXTRACCIÓN (Fuentes crudas en data/raw/)")
        raw_dfs = extract_sources(spark)
        conteos_raw = validate_raw(raw_dfs)
        for fuente, n in conteos_raw.items():
            log(f"   - {fuente:5}: {n:>9,} filas leídas")

        log("\n2. LIMPIEZA Y HOMOLOGACIÓN")
        log("   - Llave común `fecha` (primer día de mes, tipo date)")
        log("   - SHCP filtrado por claves XBC, deduplicado de registros idénticos (2018-2025) y convertido de miles a millones (/1000)")
        log("   - SAT limpio de marcas preliminares (*)")
        log("   - IGAE transformado de formato ancho a largo (serie total base 2018=100)")
        clean_dfs = clean_all(raw_dfs)

        log("\n3. VALIDACIÓN DE CALIDAD (Ventana 2015-01-01 a 2025-03-01)")
        rep_clean = validate_clean_sources(clean_dfs)
        for fuente, info in rep_clean.items():
            log(
                f"   - {fuente:5}: {info['filas_ventana']:>4} filas | "
                f"{info['meses_unicos']} meses únicos | {info['rango']}"
            )

        log("\n4. INTEGRACIÓN Y TRANSFORMACIÓN ANALÍTICA")
        df_maestro = build_master_dataset(clean_dfs)
        rep_master = validate_master(df_maestro)
        log(
            f"   - DataFrame maestro: {rep_master['filas']} filas x {rep_master['columnas']} columnas "
            f"| Granularidad: 1 fila = 1 mes ({rep_master['rango']})"
        )

        log("\n5. ALMACENAMIENTO Y VERIFICACIÓN DE RELECTURA")
        ruta_salida = save_and_verify_parquet(spark, df_maestro)
        log(f"   - Salida verificada en: {ruta_salida.relative_to(ruta_salida.parents[2])}")

        # Guardar evidencia breve en reports/ para documentación en GitHub
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        (REPORTS_DIR / "evidencia_ejecucion_act11.txt").write_text(
            "\n".join(lineas_reporte), encoding="utf-8"
        )

        print("\n=== ESQUEMA FINAL Y MUESTRA DE RESULTADOS ===")
        df_maestro.printSchema()
        df_maestro.select(
            "fecha", "anio", "shcp_xbc", "sat_total_tributarios",
            "ratio_tributario_gasto", "igae_total", "inpc", "igae_var_anual_pct"
        ).show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()