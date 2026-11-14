# ============================================================
# MÓDULO DE VALIDACIÓN DE CALIDAD (src/quality/validators.py)
# ============================================================
#
# Implementa las compuertas de calidad (fail-fast) exigidas antes
# y después de las transformaciones:
# 1. Pre-limpieza: verifica existencia de datos y columnas clave.
# 2. Post-limpieza: comprueba ausencia de nulos, ausencia de
#    duplicados (`groupBy("fecha").count() > 1`) y cobertura exacta
#    de los 123 meses entre 2015-01-01 y 2025-03-01.
# 3. Pre-carga y post-carga: verifica granularidad 1 fila = 1 mes
#    en el DataFrame maestro y consistencia al releer el Parquet.
# ============================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.ingestion.loader import FECHA_INICIO, FECHA_FIN, MESES_ESPERADOS


# ============================================================
# 1. VALIDACIÓN INICIAL (DATOS CRUDOS)
# ============================================================
def validate_raw(raw_dfs: dict[str, DataFrame]) -> dict[str, int]:
    requeridas = {
        "shcp": {"CICLO", "MES", "CLAVE_DE_CONCEPTO", "UNIDAD_DE_MEDIDA", "MONTO"},
        "sat": {"Año", "Mes"},
        "igae": {"Descriptores"},
        "inpc": {"FECHA", "CONCEPTO", "VALOR"},
    }
    conteos = {}
    for nombre, df in raw_dfs.items():
        n = df.count()
        assert n > 0, f"[VALIDACIÓN RAW] La fuente '{nombre}' está vacía (0 filas)."
        faltantes = requeridas[nombre] - set(df.columns)
        assert not faltantes, f"[VALIDACIÓN RAW] '{nombre}' carece de columnas: {faltantes}"
        conteos[nombre] = n
    return conteos


# ============================================================
# 2. VALIDACIÓN POST-LIMPIEZA (POR FUENTE EN VENTANA 2015-2025)
# ============================================================
def _assert_sin_duplicados(df: DataFrame, llave: list[str], etiqueta: str) -> None:
    duplicados = df.groupBy(*llave).count().filter(F.col("count") > 1).count()
    assert duplicados == 0, (
        f"[VALIDACIÓN] '{etiqueta}' contiene {duplicados} llaves duplicadas en {llave}."
    )


def validate_clean_sources(clean_dfs: dict[str, DataFrame]) -> dict[str, dict]:
    reporte = {}
    for nombre, df in clean_dfs.items():
        sub = df.filter(F.col("fecha").between(FECHA_INICIO, FECHA_FIN))

        # Comprobar ausencia de nulos en todas las columnas
        for c in sub.columns:
            n_null = sub.filter(F.col(c).isNull()).count()
            assert n_null == 0, f"[VALIDACIÓN CLEAN] '{nombre}' tiene {n_null} nulos en '{c}'."

        # Comprobar rango temporal mínimo y máximo
        rango = sub.agg(F.min("fecha").alias("min_f"), F.max("fecha").alias("max_f")).first()
        assert str(rango["min_f"]) == FECHA_INICIO and str(rango["max_f"]) == FECHA_FIN, (
            f"[VALIDACIÓN CLEAN] '{nombre}' cubre {rango['min_f']} a {rango['max_f']} "
            f"(se esperaba {FECHA_INICIO} a {FECHA_FIN})."
        )

        # Comprobar unicidad de llave (SHCP aún conserva CLAVE_DE_CONCEPTO antes del pivot)
        llave = ["fecha", "CLAVE_DE_CONCEPTO"] if nombre == "shcp" else ["fecha"]
        _assert_sin_duplicados(sub, llave, nombre)

        # Comprobar continuidad mensual sin huecos (123 meses exactos)
        meses_unicos = sub.select("fecha").distinct().count()
        assert meses_unicos == MESES_ESPERADOS, (
            f"[VALIDACIÓN CLEAN] '{nombre}' tiene {meses_unicos} meses; faltan {MESES_ESPERADOS - meses_unicos}."
        )

        reporte[nombre] = {
            "filas_ventana": sub.count(),
            "meses_unicos": meses_unicos,
            "rango": f"{rango['min_f']} a {rango['max_f']}",
        }
    return reporte


# ============================================================
# 3. VALIDACIÓN FINAL DEL DATAFRAME MAESTRO
# ============================================================
def validate_master(df_maestro: DataFrame) -> dict[str, int | str]:
    _assert_sin_duplicados(df_maestro, ["fecha"], "DataFrame maestro")

    total = df_maestro.count()
    assert total == MESES_ESPERADOS, (
        f"[VALIDACIÓN FINAL] Se esperaban {MESES_ESPERADOS} filas y se obtuvieron {total}."
    )

    rango = df_maestro.agg(F.min("fecha").alias("min_f"), F.max("fecha").alias("max_f")).first()
    assert str(rango["min_f"]) == FECHA_INICIO and str(rango["max_f"]) == FECHA_FIN, (
        f"[VALIDACIÓN FINAL] Rango incorrecto: {rango['min_f']} a {rango['max_f']}."
    )

    return {
        "filas": total,
        "columnas": len(df_maestro.columns),
        "rango": f"{rango['min_f']} a {rango['max_f']}",
    }