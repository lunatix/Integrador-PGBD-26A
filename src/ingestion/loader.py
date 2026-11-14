# ============================================================
# MÓDULO DE INGESTA / EXTRACCIÓN (src/ingestion/loader.py)
# ============================================================
#
# Lee las cuatro fuentes crudas descargadas previamente por
# `notebooks/01_ingesta.ipynb` en `data/raw/`.
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
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
)

# ============================================================
# 1. RUTAS Y CONSTANTES DEL PROYECTO
# ============================================================
ROOT_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT_DIR / "data" / "raw"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
REPORTS_DIR = ROOT_DIR / "reports"

# Ventana común de estudio: enero 2015 a marzo 2025 (123 meses).
# Se corta en 2025-03 porque hasta esa fecha llega el IGAE abierto.
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
    """Inicializa la sesión local de PySpark con 4 particiones de shuffle."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
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