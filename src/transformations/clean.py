# ============================================================
# MÓDULO DE LIMPIEZA Y HOMOLOGACIÓN (src/transformations/clean.py)
# ============================================================
#
# Estandariza cada fuente hacia una llave común `fecha` (tipo date,
# primer día de cada mes) y homologa unidades monetarias:
# - SHCP: filtra claves `XBC`, conserva unidades monetarias (`pesos`),
#   divide miles de pesos entre 1,000 para expresar en millones y
#   elimina los 2,523 registros exactamente duplicados (2018-2025).
# - SAT: ya está en millones de pesos; limpia saltos de línea en
#   encabezados, asteriscos (*) en años preliminares y texto en mes.
# - IGAE: pasa de formato ancho ("AAAA|Mes<R>/<P>") a formato largo
#   mediante `F.stack` filtrando la serie total base 2018=100.
# - INPC: filtra el índice general y parsea `FECHA` (`dd/MM/yyyy`)
#   al primer día del mes.
# ============================================================

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MESES = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]


def _mes_map():
    """Mapa de nombre de mes en español a número entero (1..12)."""
    return F.create_map(*[x for i, m in enumerate(MESES, 1) for x in (F.lit(m), F.lit(i))])


def _build_fecha(anio_col, mes_col):
    """Construye la llave común `fecha` = primer día del mes (tipo date)."""
    return F.to_date(
        F.format_string("%04d-%02d-01", anio_col.cast("int"), mes_col.cast("int")),
        "yyyy-MM-dd",
    )


def _clean_double(col_name: str):
    """Elimina comas, espacios o marcas preliminares (*) y convierte a double."""
    cleaned = F.regexp_replace(F.col(col_name).cast("string"), r"[^0-9.\-]", "")
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned).cast("double")


# ============================================================
# 1. LIMPIEZA SHCP (Ingresos y gastos del Gobierno Federal)
# ============================================================
def clean_shcp(df: DataFrame, prefijo_clave: str = "XBC") -> DataFrame:
    mes_num = _mes_map()
    base = df

    # Conservar únicamente cifras mensuales (evita mezclar acumulados)
    if "FRECUENCIA" in df.columns:
        base = base.filter(F.trim(F.col("FRECUENCIA")) == "Mensual")

    # Filtrar conceptos presupuestarios XBC (Gasto del Gobierno Federal)
    base = base.filter(F.col("CLAVE_DE_CONCEPTO").startswith(prefijo_clave))

    # Conservar solo unidades monetarias (descarta Porcentaje del PIB, variaciones, etc.)
    unidad = F.lower(F.trim(F.col("UNIDAD_DE_MEDIDA")))
    base = base.filter(unidad.contains("pesos"))

    # Homologar unidades de miles de pesos a millones de pesos (/ 1,000)
    monto_millones = F.when(
        unidad.contains("millones"), F.col("MONTO").cast("double")
    ).otherwise(F.col("MONTO").cast("double") / F.lit(1000.0))

    return (
        base.withColumn("fecha", _build_fecha(F.col("CICLO"), mes_num[F.trim(F.col("MES"))]))
        .withColumn("monto_millones", monto_millones)
        .select("fecha", "CLAVE_DE_CONCEPTO", "monto_millones")
        .dropna(subset=["fecha", "CLAVE_DE_CONCEPTO", "monto_millones"])
        # SHCP publica filas exactamente duplicadas desde 2018 (2,523 registros);
        # se deduplican registros idénticos manteniendo la integridad del monto.
        .dropDuplicates()
    )


# ============================================================
# 2. LIMPIEZA SAT (Ingresos tributarios)
# ============================================================
def clean_sat(df: DataFrame) -> DataFrame:
    # Normalizar nombres de columnas (el XLS original trae saltos de línea \n)
    for col_orig in df.columns:
        col_limpia = re.sub(r"\s+", " ", col_orig).strip()
        if col_orig != col_limpia:
            df = df.withColumnRenamed(col_orig, col_limpia)

    mes_num = _mes_map()
    # Extraer los 4 dígitos del año ignorando asteriscos (*) de cifras preliminares
    sat_anio = F.regexp_extract(F.col("Año").cast("string"), r"(\d{4})", 1).cast("int")
    sat_mes = mes_num[F.trim(F.col("Mes"))]

    return (
        df.withColumn("fecha", _build_fecha(sat_anio, sat_mes))
        .select(
            "fecha",
            _clean_double("Total de Ingresos tributarios").alias("sat_total_tributarios"),
            _clean_double("Impuesto Sobre la Renta").alias("sat_isr"),
            _clean_double("Impuesto al Valor Agregado").alias("sat_iva"),
            _clean_double("Impuesto Especial sobre Producción y Servicios").alias("sat_ieps"),
        )
        .dropna(subset=["fecha", "sat_total_tributarios"])
    )


# ============================================================
# 3. LIMPIEZA IGAE (Índice Global de la Actividad Económica)
# ============================================================
def clean_igae(df: DataFrame) -> DataFrame:
    # Seleccionar únicamente la serie total base 2018=100
    serie_total = df.filter(
        F.col("Descriptores") == "Índice de volumen físico base 2018=100|Total"
    )

    # Identificar columnas mensuales "AAAA|Mes" limpiando etiquetas <R> y <P>
    pares = []
    for c in serie_total.columns:
        if "|" in c and "Anual" not in c:
            anio = int(c[:4])
            mes_txt = c[5:].split("<")[0].strip()
            if mes_txt in MESES:
                mes_idx = MESES.index(mes_txt) + 1
                fecha_str = f"{anio:04d}-{mes_idx:02d}-01"
                pares.append((fecha_str, f"`{c}`"))

    # Transformar de formato ancho (columnas por mes) a largo (una fila por mes)
    stack_args = ", ".join([f"'{f}', cast({col} as double)" for f, col in pares])
    return (
        serie_total.select(F.expr(f"stack({len(pares)}, {stack_args}) as (fecha_str, igae_total)"))
        .withColumn("fecha", F.to_date(F.col("fecha_str"), "yyyy-MM-dd"))
        .select("fecha", "igae_total")
        .dropna(subset=["fecha", "igae_total"])
    )


# ============================================================
# 4. LIMPIEZA INPC (Índice Nacional de Precios al Consumidor)
# ============================================================
def clean_inpc(df: DataFrame) -> DataFrame:
    # Conservar la serie general del índice nacional
    inpc_general = df.filter(
        F.col("CONCEPTO").endswith("Precios al Consumidor (INPC)")
    )

    # Parsear FECHA ("dd/MM/yyyy", ej. "01/01/2003") al primer día del mes
    fecha_raw = F.to_date(F.trim(F.col("FECHA")), "dd/MM/yyyy")

    return (
        inpc_general.withColumn("fecha", F.trunc(fecha_raw, "month"))
        .select("fecha", F.col("VALOR").cast("double").alias("inpc"))
        .dropna(subset=["fecha", "inpc"])
    )


# ============================================================
# 5. ORQUESTACIÓN DE LIMPIEZA DE LAS 4 FUENTES
# ============================================================
def clean_all(raw_dfs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    return {
        "shcp": clean_shcp(raw_dfs["shcp"]),
        "sat": clean_sat(raw_dfs["sat"]),
        "igae": clean_igae(raw_dfs["igae"]),
        "inpc": clean_inpc(raw_dfs["inpc"]),
    }