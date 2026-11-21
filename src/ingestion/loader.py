# ============================================================
# MÓDULO DE INGESTA / CAPA BRONZE (src/ingestion/loader.py)
# ============================================================
#
# Lee las cuatro fuentes crudas descargadas previamente por
# `notebooks/01_ingesta.ipynb` en `data/raw/` y define las rutas
# de la arquitectura medallion:
#   data/raw     archivos tal como se descargan (CSV)
#   data/bronze  mismas filas y columnas, en Parquet, con metadatos de ingesta
#   data/silver  una tabla limpia y validada por fuente
#   data/gold    dataset maestro analítico (1 fila = 1 mes)
#
# Decisiones técnicas:
# - Configura `PYSPARK_PYTHON` y `PYSPARK_DRIVER_PYTHON` con
#   `sys.executable` para evitar conflictos de rutas con espacios
#   en Windows.
# - Define esquemas explícitos (`StructType`) en SHCP e INPC
#   para garantizar tipos consistentes y evitar doble lectura.
# ============================================================

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
)

# ============================================================
# 1. RUTAS Y CONSTANTES DEL PROYECTO
# ============================================================
ROOT_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT_DIR / "data" / "raw"
BRONZE_DIR = ROOT_DIR / "data" / "bronze"
SILVER_DIR = ROOT_DIR / "data" / "silver"
GOLD_DIR = ROOT_DIR / "data" / "gold"
REPORTS_DIR = ROOT_DIR / "reports"
EVENTOS_DIR = ROOT_DIR / "spark-events"  # event log para revisar la Spark UI en el History Server

# Ventana común de estudio: enero 2015 a marzo 2025 (123 meses).
# Se corta en 2025-03 porque hasta esa fecha llega el IGAE abierto.
# Inicia en 2015 (aunque SHCP llega a 2011) para dejar fuera la Reforma
# Hacendaria de 2014: el IEPS fue negativo en 2011-2013 y la recaudación
# creció 31 % en 2015, un salto de régimen y no de actividad económica.
FECHA_INICIO = "2015-01-01"
FECHA_FIN = "2025-03-01"
MESES_ESPERADOS = 123

# ============================================================
# 2. ESQUEMAS EXPLÍCITOS (SHCP e INPC)
# ============================================================
SCHEMA_SHCP = StructType([
    StructField("CICLO", IntegerType(), True),
    StructField("MES", StringType(), True),
    StructField("CLAVE_DE_CONCEPTO", StringType(), True),
    StructField("NOMBRE", StringType(), True),
    StructField("TEMA", StringType(), True),
    StructField("SUBTEMA", StringType(), True),
    StructField("SECTOR", StringType(), True),
    StructField("AMBITO", StringType(), True),
    StructField("TIPO_DE_INFORMACION", StringType(), True),
    StructField("BASE_DE_REGISTRO", StringType(), True),
    StructField("UNIDAD_DE_MEDIDA", StringType(), True),
    StructField("PERIODO_INICIO", TimestampType(), True),
    StructField("PERIODO_FINAL", TimestampType(), True),
    StructField("FRECUENCIA", StringType(), True),
    StructField("DIFUSION", StringType(), True),
    StructField("MONTO", DoubleType(), True),
])

SCHEMA_INPC = StructType([
    StructField("COBERTURA", StringType(), True),
    StructField("PERIODICIDAD", StringType(), True),
    StructField("FECHA", StringType(), True),
    StructField("CONCEPTO", StringType(), True),
    StructField("VALOR", DoubleType(), True),
    StructField("UNIDAD_MEDIDA", StringType(), True),
    StructField("ESTATUS", StringType(), True),
])


# ============================================================
# 3. SESIÓN DE SPARK Y LECTURA DE FUENTES
# ============================================================
def get_spark(app_name: str = "Integrador_PGBD_Act11") -> SparkSession:
    """Sesión local de PySpark. La justificación medida de cada valor
    (núcleos, particiones de shuffle, memoria) está en docs/guia_optimizacion.md."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    EVENTOS_DIR.mkdir(exist_ok=True)
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENTOS_DIR.as_uri())
        .getOrCreate()
    )


def resolve_raw_paths(raw_dir: Path = RAW_DIR) -> dict[str, Path]:
    """Verifica que existan los 4 archivos crudos antes de iniciar el flujo."""
    igae_dir = raw_dir / "igae_mensual_csv" / "conjunto_de_datos"
    igae_matches = sorted(igae_dir.glob("conjunto_de_datos_igae_*.csv"))
    if not igae_matches:
        raise FileNotFoundError(
            f"No se encontró el CSV de IGAE en {igae_dir}. Ejecuta notebooks/01_ingesta.ipynb primero."
        )

    rutas = {
        "shcp": raw_dir / "ingreso_gasto_finan.csv",
        "sat": raw_dir / "ingresos_tributarios_sat.csv",
        "igae": igae_matches[0],
        "inpc": raw_dir / "inpc_mensual_csv" / "conjunto_de_datos" / "conjunto_de_datos_inpc_mensual.csv",
    }

    for nombre, ruta in rutas.items():
        if not ruta.exists():
            raise FileNotFoundError(
                f"Falta la fuente '{nombre}' en {ruta}. Ejecuta notebooks/01_ingesta.ipynb primero."
            )
    return rutas


def extract_sources(spark: SparkSession, raw_dir: Path = RAW_DIR) -> dict[str, DataFrame]:
    """Carga las 4 fuentes en DataFrames de Spark."""
    rutas = resolve_raw_paths(raw_dir)
    return {
        "shcp": spark.read.csv(str(rutas["shcp"]), header=True, schema=SCHEMA_SHCP),
        "sat": spark.read.csv(str(rutas["sat"]), header=True, inferSchema=True),
        "igae": spark.read.csv(str(rutas["igae"]), header=True, inferSchema=True),
        "inpc": spark.read.csv(str(rutas["inpc"]), header=True, schema=SCHEMA_INPC),
    }


# ============================================================
# 4. ESCRITURA Y LECTURA DE CAPAS (BRONZE / SILVER / GOLD)
# ============================================================
def to_bronze(raw_dfs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Bronze = datos crudos sin transformar + trazabilidad (archivo y momento de ingesta)."""
    return {
        n: df.withColumn("_archivo_origen", F.input_file_name())
             .withColumn("_fecha_ingesta", F.current_timestamp())
        for n, df in raw_dfs.items()
    }


def write_layer(dfs: dict[str, DataFrame], capa_dir: Path) -> None:
    for nombre, df in dfs.items():
        df.write.mode("overwrite").parquet(str(capa_dir / nombre))


def read_layer(spark: SparkSession, capa_dir: Path, nombres=("shcp", "sat", "igae", "inpc")) -> dict[str, DataFrame]:
    return {n: spark.read.parquet(str(capa_dir / n)) for n in nombres}