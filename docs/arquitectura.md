# Arquitectura del pipeline

## Arquitectura implementada: medallion

| Capa | Carpeta | Contenido | Código |
| :--- | :--- | :--- | :--- |
| Raw | `data/raw/` | Archivos tal como se descargan | `notebooks/01_ingesta.ipynb` |
| Bronze | `data/bronze/<fuente>` | Mismas filas y columnas en Parquet + `_archivo_origen`, `_fecha_ingesta` | `capa_bronze()` en `src/pipeline.py` |
| Silver | `data/silver/<fuente>` | Una tabla limpia por fuente con llave `fecha`, validada sobre lo escrito | `capa_silver()` |
| Gold | `data/gold/dataset_maestro` | Dataset maestro (1 fila = 1 mes), particionado por año | `capa_gold()` |

Cada capa se materializa en Parquet, así que el CSV de SHCP (225 MB) se lee una sola vez y las capas siguientes se reprocesan desde Parquet. Las métricas de Spark por capa están en `reports/evidencia_ejecucion_act11.txt`; la justificación de la configuración, en `guia_optimizacion.md`.

---

# Diseño inicial (sesión 1)

## Diagrama del flujo de datos

```mermaid
flowchart TD
    A1[SHCP<br/>Ingresos y gastos GF<br/>Parquet/CSV] --> B
    A2[SAT<br/>Ingresos tributarios<br/>XLS] --> B
    A3[INEGI IGAE<br/>CSV] --> B
    A4[INEGI INPC<br/>CSV] --> B
    B[Ingesta<br/>spark.read] --> C[Validación y limpieza<br/>esquema, tipos, nulos, rangos de fechas]
    C --> D[Transformación<br/>fecha mensual, pivot SHCP, unidades a millones,<br/>variación anual, ratio fiscal]
    D --> E[Integración<br/>join por fecha → DataFrame maestro]
    E --> F[Almacenamiento<br/>Parquet particionado por año]
    F --> G[Análisis<br/>rezagos, correlaciones, agregados por periodo]
```

## Operaciones PySpark por etapa

| Etapa | Operaciones | Notas |
| :--- | :--- | :--- |
| Ingesta | `spark.read.csv(header=True)`, `spark.read.parquet` | El `.xls` del SAT se convierte primero a CSV con pandas, porque Spark no lee Excel de forma nativa. |
| Validación | `printSchema`, `count`, `filter(col.isNull())`, `agg(min, max)` sobre la fecha | Se verifica que no falten meses y que no haya duplicados por fecha (`groupBy(fecha).count() > 1`). |
| Limpieza | `cast("double")`, `to_date`, `dropna`, `filter` | Se quitan las filas de notas al pie del SAT y los meses en texto se mapean a número. |
| Transformación | `groupBy(fecha).pivot("CLAVE_DE_CONCEPTO")`, `withColumn`, `Window.orderBy(fecha)` + `lag(12)` | SHCP está en miles de pesos y SAT en millones, así que se homologan a millones. |
| Integración | `join(on="fecha", how="inner")` | Son tablas pequeñas (unos 200 meses), por lo que conviene `broadcast()` para evitar shuffle. |
| Almacenamiento | `write.mode("overwrite").partitionBy("anio").parquet(...)` | Los datos y las salidas no se versionan (ver `.gitignore`). |
| Análisis | `stat.corr`, `lag(k)` con k = 1, 3, 6, 12, `groupBy(periodo).agg(avg)`, Spark SQL | Periodos: prepandemia, pandemia y pospandemia. |

## Relación entre fuentes
- **Llave:** `fecha` = primer día del mes (`date`). Todas las fuentes son mensuales.
- **Ventana común:** 2011-01 a 2025-03 (171 meses). El análisis se restringe a 2015-01 – 2025-03. El fin se debe a que el IGAE de datos abiertos termina en 2025-03. Inicia en 2015 porque los datos existen desde 2011-01 (límite de SHCP), pero la Reforma Hacendaria de 2014 cambió el régimen de recaudación: el IEPS fue negativo en 2011-2013 por el subsidio a gasolinas y la recaudación total creció 31 % en 2015. Desde 2015 todas las variaciones anuales comparan meses bajo el mismo régimen fiscal, así que el salto de la reforma no se confunde con efectos de los impuestos sobre el IGAE o la inflación.
- **Riesgos:** las series son estacionales (el IGAE en niveles y la recaudación de abril con la declaración anual), por lo que se usan variaciones anuales. El INPC tiene base 2Q jul 2018 = 100 y se usa como índice. Las cifras más recientes del SAT son preliminares.
