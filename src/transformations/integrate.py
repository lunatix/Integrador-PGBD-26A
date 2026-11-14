# ============================================================
# MÓDULO DE INTEGRACIÓN Y CARGA (src/transformations/integrate.py)
# ============================================================
#
# Construye el DataFrame maestro analítico con granularidad
# mensual (1 fila = 1 mes, 123 registros de 2015-01 a 2025-03):
# 1. Recorta las 4 fuentes a la ventana común [2015-01-01, 2025-03-01].
# 2. Pivotea SHCP por `fecha` y `CLAVE_DE_CONCEPTO`.
# 3. Une las tablas por `fecha` usando `broadcast()` (inner join)
#    para evitar shuffle innecesario dado su tamaño (~123 filas).
# 4. Calcula variables deflactadas con INPC, ratios fiscales y
#    variaciones anuales (`Window.orderBy("fecha")` + `lag(12)`)
#    para neutralizar la estacionalidad de las series.
# 5. Almacena en Parquet particionado por `anio` y verifica relectura.
# ============================================================

from pathlib import Path
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from src.ingestion.loader import FECHA_INICIO, FECHA_FIN, PROCESSED_DIR


# ============================================================
# 1. INTEGRACIÓN Y CÁLCULOS ANALÍTICOS
# ============================================================
def build_master_dataset(clean_dfs: dict[str, DataFrame]) -> DataFrame:
    # Recortar todas las fuentes a la ventana común de estudio
    recortados = {
        nombre: df.filter(F.col("fecha").between(FECHA_INICIO, FECHA_FIN))
        for nombre, df in clean_dfs.items()
    }

    # Pivot de conceptos presupuestarios SHCP por fecha
    shcp_pivoted = (
        recortados["shcp"]
        .groupBy("fecha")
        .pivot("CLAVE_DE_CONCEPTO")
        .agg(F.first("monto_millones"))
    )
    for col_name in shcp_pivoted.columns:
        if col_name != "fecha":
            shcp_pivoted = shcp_pivoted.withColumnRenamed(
                col_name, f"shcp_{col_name.lower().replace('-', '_')}"
            )

    # Integración por `fecha` con broadcast() para optimizar el join en Spark
    maestro = (
        shcp_pivoted
        .join(F.broadcast(recortados["sat"]), on="fecha", how="inner")
        .join(F.broadcast(recortados["igae"]), on="fecha", how="inner")
        .join(F.broadcast(recortados["inpc"]), on="fecha", how="inner")
    )

    # Ventana ordenada cronológicamente para variaciones interanuales (12 meses)
    w_anual = Window.orderBy("fecha")

    return (
        maestro
        # Recaudación tributaria total deflactada con INPC (base 2Q jul 2018 = 100)
        .withColumn(
            "sat_total_real",
            F.round(F.col("sat_total_tributarios") / (F.col("inpc") / F.lit(100.0)), 4),
        )
        # Ratio fiscal 1: cobertura del gasto total del Gobierno Federal (XBC) con ingresos tributarios
        .withColumn(
            "ratio_tributario_gasto",
            F.round(F.col("sat_total_tributarios") / F.col("shcp_xbc"), 4),
        )
        # Ratio fiscal 2: peso del ISR dentro del total de ingresos tributarios
        .withColumn(
            "ratio_isr_tributario",
            F.round(F.col("sat_isr") / F.col("sat_total_tributarios"), 4),
        )
        # Variaciones porcentuales anuales (mismo mes del año previo)
        .withColumn(
            "igae_var_anual_pct",
            F.round((F.col("igae_total") / F.lag("igae_total", 12).over(w_anual) - 1.0) * 100.0, 2),
        )
        .withColumn(
            "inflacion_anual_pct",
            F.round((F.col("inpc") / F.lag("inpc", 12).over(w_anual) - 1.0) * 100.0, 2),
        )
        .withColumn(
            "sat_real_var_anual_pct",
            F.round((F.col("sat_total_real") / F.lag("sat_total_real", 12).over(w_anual) - 1.0) * 100.0, 2),
        )
        # Columna de partición para el almacenamiento Parquet
        .withColumn("anio", F.year("fecha"))
        .orderBy("fecha")
    )


# ============================================================
# 2. ALMACENAMIENTO PARQUET Y VERIFICACIÓN DE RELECTURA
# ============================================================
def save_and_verify_parquet(
    spark: SparkSession,
    df: DataFrame,
    output_dir: Path = PROCESSED_DIR,
    dataset_name: str = "dataset_maestro.parquet",
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    destino = output_dir / dataset_name

    # Guardar en Parquet particionado por año
    df.write.mode("overwrite").partitionBy("anio").parquet(str(destino))

    # Releer el Parquet almacenado para comprobar integridad y esquema
    df_releido = spark.read.parquet(str(destino))
    conteo_escrito = df.count()
    conteo_leido = df_releido.count()

    assert conteo_escrito == conteo_leido, (
        f"[CARGA] Discrepancia al releer Parquet: {conteo_escrito} escritas vs {conteo_leido} leídas."
    )
    assert set(df.columns) == set(df_releido.columns), (
        "[CARGA] El esquema releído no coincide con el esquema guardado."
    )

    return destino