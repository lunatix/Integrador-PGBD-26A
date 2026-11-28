# Proyecto Integrador — Política fiscal, crecimiento e inflación en México (2015–2025)

Pipeline de procesamiento y análisis de datos macroeconómicos y fiscales de México desarrollado en **PySpark** bajo una **arquitectura medallion** (Bronze → Silver → Gold + Capa de Consumo Analítico) reproducible. Integra series mensuales de gasto público (**SHCP**), recaudación tributaria (**SAT**), actividad económica (**INEGI - IGAE**) e inflación (**INEGI - INPC**) para el periodo **enero de 2015 a marzo de 2025** (123 meses).

## Integrantes

- Díaz Aguila Mauricio Adrián
- Dimas Salgado Aline Yaremys
- Romero Gálvez Roberto Alejandro
- Ruesga Rodriguez Gloria Viridiana

## Pregunta de investigación

¿De qué manera influye la relación entre el dinero que gasta el gobierno (Secretaría de Hacienda) y lo que cobra en impuestos (SAT) en el crecimiento económico y en la subida de precios (inflación) en México, durante el periodo de enero de 2015 a marzo de 2025?

## Objetivos

- **Objetivo General:**
  Evaluar cómo afectan de forma conjunta el gasto del gobierno y la recaudación de impuestos al crecimiento de la economía y al aumento del costo de vida en México mediante PySpark.
- **Objetivos Específicos:** 
  1. **Crecimiento económico (IGAE):** Identificar qué tan conectados están el gasto público y los impuestos principales (ISR e IVA) con el crecimiento económico del país, y cuánto tiempo tarda en verse su impacto.
  2. **Estabilidad de precios (INPC):** Descubrir si aumentar el cobro de impuestos ayuda a frenar la subida de precios provocada por el gasto del gobierno o si, por el contrario, la presiona al alza.

## Variables

| Variable | Tipo de Variable | Fuente | Indicador / Definición | Periodicidad | Cobertura Temporal |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Gasto del Gobierno Federal** | Explicativa (Fiscal - Egresos) | **SHCP** | Gasto total (`shcp_xbc`); el gasto programable (`shcp_xbc10`) se usa como prueba de robustez. | Mensual | 2015-01 – 2025-03 |
| **Recaudación Tributaria** | Explicativa (Fiscal - Ingresos) | **SAT** | Ingresos tributarios netos (`sat_total_tributarios`, con desglose por `sat_isr`, `sat_iva` y `sat_ieps`). | Mensual | 2015-01 – 2025-03 |
| **IGAE** | Dependiente (Producto) | **INEGI** | Índice Global de la Actividad Económica (Base 2018 = 100 y variación anual %). | Mensual | 2015-01 – 2025-03 |
| **INPC** | Dependiente (Precios) | **INEGI** | Índice Nacional de Precios al Consumidor (Base 2Q jul 2018 = 100 y tasa de inflación anual %). | Mensual | 2015-01 – 2025-03 |

## Resultados principales

| Pregunta | Hallazgo (2015–2025, datos mensuales) |
| :--- | :--- |
| **Objetivo 1: Crecimiento (IGAE)** | La recaudación total (`r = 0.33`, p < 0.01) y especialmente el **IVA** (`r = 0.35`, p < 0.001) crecen junto con la economía en el mismo mes, pero es la economía la que **anticipa** a la recaudación (causalidad de Granger, rezagos de 1 a 6 meses), no al revés. El gasto público **no** muestra relación significativa con el IGAE en ningún rezago (0–12 meses), ni con gasto total ni con gasto programable. |
| **Objetivo 2: Inflación (INPC)** | En el modelo multivariado con errores robustos Newey-West (HAC), el crecimiento de la recaudación se asocia con **menor inflación** (`coef = -0.030`, p = 0.049; `-0.045`, p < 0.001 al controlar por periodos). El gasto **anticipa presiones inflacionarias a 5–6 meses** (Granger, p ≈ 0.01). El ratio recaudación/gasto (promedio 70.7 %) **no** tiene efecto directo significativo. El modelo explica **23.5 %** de la inflación (**40.9 %** con efectos de periodo). |
| **Periodos estructurales** | La relación entre política fiscal, crecimiento e inflación no es estable en el tiempo: en prepandemia ninguna correlación es significativa; en pandemia la recaudación siguió de cerca el ciclo del IGAE (`r = 0.57`), y en pospandemia el dinamismo recaudatorio moderó presiones de precios (`r = -0.43`). |

*Nota: Las correlaciones y pruebas de Granger miden asociación estadística y precedencia temporal, no causalidad estructural aislada; las limitaciones completas se detallan al final de `notebooks/02_analisis_dataset_maestro_final.ipynb`.*

## Arquitectura del flujo de datos (Medallion + Capa de Consumo)

```mermaid
flowchart LR
    A1[SHCP<br/>CSV] --> R
    A2[SAT<br/>XLS → CSV] --> R
    A3[INEGI IGAE<br/>CSV] --> R
    A4[INEGI INPC<br/>CSV] --> R
    R[(data/raw)] --> B[(BRONZE<br/>data/bronze<br/>Parquet sin transformar<br/>+ metadatos de ingesta)]
    B --> S[(SILVER<br/>data/silver<br/>1 tabla limpia por fuente<br/>validada: nulos, duplicados, 123 meses)]
    S --> G[(GOLD<br/>data/gold/dataset_maestro<br/>1 fila = 1 mes, 43 columnas<br/>particionado por año)]
    G --> C[Capa de Consumo<br/>Spark SQL + Window + PySpark ML<br/>notebooks/02]

| Capa / Nivel | Código | Qué hace Spark (medido en la Spark UI) |
| :--- | :--- | :--- |
| **Bronze** | `capa_bronze()` → `src/ingestion/loader.py` | Lee los CSV una sola vez (228.8 MB, 12 particiones = 12 núcleos) y escribe 4.8 MB de Parquet (~50 veces menos). Sin shuffle. |
| **Silver** | `capa_silver()` → `clean.py` + `validators.py` | Lee 6.4 MB de Bronze (6 de 18 columnas, filtro XBC empujado a Parquet), deduplica con un shuffle de 181.5 KB en 4 particiones y valida lo escrito. |
| **Gold** | `capa_gold()` → `integrate.py` | Lee 0.5 MB de Silver, pivotea 29 claves, une con `BroadcastHashJoin`, calcula `lag(12)` en una partición y escribe Parquet por año. |
| **Consumo Analítico** | `notebooks/02_analisis_dataset_maestro_final.ipynb` | Consulta Gold vía Spark SQL (`vw_dataset_maestro`), calcula rezagos (`lag1`–`lag12`), persiste en memoria (`cache()`) y ejecuta correlaciones (`Correlation.corr`) y regresión multivariada (`pyspark.ml.regression.LinearRegression`). |

El plan físico de cada capa (`explain`) y sus métricas se muestran paso a paso en [`notebooks/01_ingesta.ipynb`](notebooks/01_ingesta.ipynb) y [`notebooks/02_analisis_dataset_maestro_final.ipynb`](notebooks/02_analisis_dataset_maestro_final.ipynb). La justificación medida de la configuración (instancias, particiones, caché, joins, memoria) está en [`docs/guia_optimizacion.md`](docs/guia_optimizacion.md#3-resultados-medidos-en-este-proyecto).

---

## Estructura del repositorio

```text
Integrador-PGBD-26A/
├── data/                             # En .gitignore: se regenera con notebooks/01_ingesta.ipynb o src.pipeline
│   ├── raw/                          # Archivos descargados (CSV, XLS, ZIP)
│   ├── bronze/                       # Parquet sin transformar + archivo de origen y fecha de ingesta
│   ├── silver/                       # Una tabla limpia y validada por fuente
│   └── gold/dataset_maestro/         # Dataset maestro analítico, Parquet particionado por año
├── docs/                             # Fuentes, arquitectura, roles y guía de optimización (resultados medidos)
├── notebooks/
│   ├── 01_ingesta.ipynb              # Descarga + Bronze → Silver → Gold, con el plan y las métricas de Spark en cada capa
│   └── 02_analisis_dataset_maestro_final.ipynb  # Capa de consumo: Spark SQL, MLlib, Granger, 5 gráficas y métricas por etapa
├── reports/
│   ├── evidencia_ejecucion_act11.txt # Conteos, validaciones y métricas de Spark por capa (generado por src.pipeline)
│   ├── figuras/                      # Gráficas ejecutivas exportadas del análisis
│   └── spark_ui/                     # Capturas de evidencia de ejecución en Spark History Server
├── spark-events/                     # Event log para reabrir la Spark UI en el History Server (en .gitignore)
├── src/
│   ├── pipeline.py                   # Orquestador medallion: capa_bronze, capa_silver, capa_gold
│   ├── monitoring.py                 # Métricas de la Spark UI (API REST) por etapa
│   ├── ingestion/
│   │   └── loader.py                 # Sesión Spark, esquemas explícitos, rutas de capas y escritura Bronze
│   ├── quality/
│   │   └── validators.py             # Compuertas de calidad: nulos, duplicados por llave y continuidad temporal
│   └── transformations/
│       ├── clean.py                  # Silver: llave `fecha`, deduplicación SHCP y homologación monetaria
│       └── integrate.py              # Gold: pivot SHCP, joins broadcast, ventanas YoY y escritura Parquet
├── .gitignore
├── requirements.txt
└── README.md

---

## Fuentes de datos y ventana de estudio

Todas las fuentes se homologan a una granularidad mensual común (`1 fila = 1 mes`) dentro de la ventana **2015-01-01 a 2025-03-01**. El fin lo marca el último dato de la serie abierta del IGAE. Inicia en 2015 porque los datos existen desde 2011-01 (límite de SHCP), pero la Reforma Hacendaria de 2014 cambió el régimen de recaudación: el IEPS fue negativo en 2011-2013 por el subsidio a gasolinas y la recaudación total creció 31 % en 2015. Desde 2015 todas las variaciones anuales comparan meses bajo el mismo régimen fiscal.

| Fuente | Institución | Archivo en `data/raw/` | Filas crudas | Filas limpias (ventana) | Unidad / Escala final |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **Gasto Presupuestario (`XBC*`)** | SHCP | `ingreso_gasto_finan.csv` | 746,676 | 3,567 *(29 claves × 123 meses)* | Millones de pesos corrientes |
| **Ingresos Tributarios** | SAT | `ingresos_tributarios_sat.csv` | 201 | 123 | Millones de pesos corrientes |
| **IGAE (Serie Total)** | INEGI | `igae_mensual_csv/.../conjunto_de_datos_igae_*.csv` | 120 | 123 | Índice de volumen físico (Base 2018 = 100) |
| **INPC (Índice General)** | INEGI | `inpc_mensual_csv/.../conjunto_de_datos_inpc_mensual.csv` | 2,002 | 123 | Índice de precios (Base 2Q jul 2018 = 100) |

---

## Operaciones PySpark por etapa (ETL y Consumo Analítico)

| Etapa | Módulo | Operaciones PySpark | Decisiones técnicas |
| :--- | :--- | :--- | :--- |
| **1. Ingesta → Bronze** | `notebooks/01_ingesta.ipynb`<br>`src/ingestion/loader.py` | `SparkSession.builder`, `spark.read.csv(header=True, schema=...)`, `input_file_name`, `current_timestamp`, `write.parquet` | Las fuentes se guardan en `data/bronze/` sin transformar, con columnas de trazabilidad `_archivo_origen` y `_fecha_ingesta`. El archivo `.xls` del SAT se convierte previamente a CSV en el notebook mediante `pandas` + `xlrd`. Se definen esquemas explícitos (`StructType`) para SHCP e INPC con el fin de evitar doble lectura y errores de inferencia. Se asigna `sys.executable` a `PYSPARK_PYTHON` para compatibilidad en Windows. |
| **2. Limpieza → Silver** | `src/transformations/clean.py` | `to_date`, `trunc`, `create_map`, `regexp_extract`, `regexp_replace`, `filter`, `stack`, `dropna`, `dropDuplicates` | Se construye la llave común `fecha` (primer día de cada mes, tipo `date`). En **SHCP** se filtran claves `XBC*` mensuales en pesos, se eliminan **2,523 registros exactamente duplicados** publicados desde 2018 y se divide entre `1,000` para convertir de miles a millones de pesos. En **SAT** se limpian saltos de línea en encabezados y marcas preliminares (`*`). En **IGAE** se aplica `F.stack` para pasar de formato ancho a largo. |
| **3. Validación de calidad (Silver)** | `src/quality/validators.py` | `count`, `filter(col.isNull())`, `agg(min, max)`, `groupBy(llave).count().filter(count > 1)` | Verifica conteos positivos en la extracción, ausencia total de nulos en llaves y métricas, cero duplicados por `(fecha, CLAVE_DE_CONCEPTO)` en SHCP y continuidad exacta de **123 meses únicos** por fuente antes de integrar. |
| **4. Integración → Gold** | `src/transformations/integrate.py` | `groupBy("fecha").pivot("CLAVE_DE_CONCEPTO")`, `join(broadcast(...), on="fecha", how="inner")`, `Window.orderBy("fecha")`, `lag(..., 12)` | Se pivotean las 29 claves presupuestarias de SHCP y se unen las 4 fuentes mediante `broadcast()` para evitar *shuffle* innecesario. Se calculan cifras reales deflactadas con INPC, ratios fiscales y variaciones porcentuales interanuales (`YoY`) para neutralizar la estacionalidad mensual. |
| **5. Almacenamiento y verificación (Gold)** | `src/transformations/integrate.py` | `write.mode("overwrite").partitionBy("anio").parquet(...)`, `spark.read.parquet(...)` | Persiste el dataset maestro en formato columnar Parquet particionado por año (`anio`) y ejecuta una relectura inmediata para validar que el conteo de filas (123) y el esquema (43 columnas) coincidan exactamente. |
| **6. Consumo analítico y modelado** | `notebooks/02_analisis_dataset_maestro_final.ipynb` | `createOrReplaceTempView`, `spark.sql`, `Window.orderBy`, `lag`, `cache()`, `VectorAssembler`, `Correlation.corr`, `LinearRegression`, `F.corr` | Registra la capa Gold como vista SQL (`vw_dataset_maestro`) para consultas agregadas ad-hoc; materializa rezagos (`lag1`–`lag12`) en caché (`InMemoryTableScan`, `0.0 KB` de shuffle en etapas posteriores) y ejecuta matrices de correlación y regresión multivariada OLS en `pyspark.ml`. |

---

## Variables del dataset maestro (capa Gold)

La capa Gold (`data/gold/dataset_maestro`) contiene **123 filas × 43 columnas**:

- **Llave y partición:** `fecha` (`date`), `anio` (`int`).
- **Gasto del Gobierno Federal (`SHCP`, millones de pesos):** `shcp_xbc` (Gasto Total del Gobierno Federal) y sus 28 desgloses programables, no programables, corrientes, de capital y costo financiero (`shcp_xbc10` a `shcp_xbc23`).
- **Recaudación Tributaria (`SAT`, millones de pesos):** `sat_total_tributarios`, `sat_isr`, `sat_iva`, `sat_ieps`.
- **Indicadores Macroeconómicos (`INEGI`):** `igae_total`, `inpc`.
- **Variables Analíticas Derivadas:**
  - `sat_total_real`: recaudación tributaria total deflactada con el INPC (`sat_total_tributarios / (inpc / 100)`).
  - `ratio_tributario_gasto`: proporción del gasto total del Gobierno Federal cubierta con ingresos tributarios (`sat_total_tributarios / shcp_xbc`).
  - `ratio_isr_tributario`: participación del ISR dentro del total de ingresos tributarios (`sat_isr / sat_total_tributarios`).
  - `igae_var_anual_pct`, `inflacion_anual_pct`, `sat_real_var_anual_pct`: variaciones porcentuales anuales (`lag` de 12 meses; `NULL` en 2015 al corresponder al primer año de la ventana).

---

## Instrucciones de ejecución

### 1. Instalar dependencias
Desde la raíz del repositorio:

    pip install -r requirements.txt

> **Java 17:** Spark 3.5 no arranca con Java 21 o superior (error `getSubject is not supported`). Si `java -version` no es 17, exportar `JAVA_HOME` a un JDK 17 antes de ejecutar (por ejemplo `export JAVA_HOME=/usr/lib/jvm/java-17-temurin-jdk`).

### 2. Descargar las fuentes y construir las capas
Ejecutar todas las celdas de `notebooks/01_ingesta.ipynb` (desde `notebooks/`). Descarga las cuatro fuentes a `data/raw/`, convierte el `.xls` del SAT a CSV y construye Bronze, Silver y Gold mostrando el plan y las métricas de Spark en cada capa.

Para reconstruir las capas sin el notebook (por ejemplo, después de actualizar `data/raw/`), desde la raíz:

    python -m src.pipeline

El pipeline escribe la evidencia, con métricas de Spark por capa, en `reports/evidencia_ejecucion_act11.txt`.

### 3. Análisis y capa de consumo (Actividad 12)
Ejecutar todas las celdas de `notebooks/02_analisis_dataset_maestro_final.ipynb`, que valida la capa Gold, ejecuta las consultas Spark SQL, calcula rezagos y correlaciones, entrena el modelo de regresión en PySpark ML, genera las 5 visualizaciones ejecutivas y reporta las métricas de ejecución por etapa.

### 4. Revisar la Spark UI
- **En vivo:** mientras corre un notebook o el pipeline, en la URL que imprime la sesión (normalmente http://localhost:4040). Los jobs llevan la etiqueta de su capa o sección (`bronze`, `silver`, `gold`, `2b_consumo_spark_sql`, `4_objetivo1`, …).
- **Después:** cada ejecución deja su *event log* en `spark-events/`. Para reabrirlo en el History Server:

    export SPARK_HOME=$(python -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")
    SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://$PWD/spark-events" $SPARK_HOME/sbin/start-history-server.sh
    # abrir http://localhost:18080

> **Nota en Windows con Anaconda:** si `python` no está registrado en el `PATH` global de la terminal, ejecutar especificando la ruta del intérprete del entorno, por ejemplo:
> `& "C:\Users\<usuario>\anaconda3\python.exe" -m src.pipeline`

---

## Evidencia de ejecución

El registro completo generado automáticamente por el pipeline ETL se encuentra en [`reports/evidencia_ejecucion_act11.txt`](reports/evidencia_ejecucion_act11.txt).

- **Bronze (filas por fuente):**
  - `shcp`: 746,676 filas
  - `sat`: 201 filas
  - `igae`: 120 filas
  - `inpc`: 2,002 filas
- **Silver, validación de calidad en ventana (`2015-01-01` a `2025-03-01`):**
  - `shcp`: 3,567 filas | 123 meses únicos | 0 duplicados
  - `sat`: 123 filas | 123 meses únicos | 0 duplicados
  - `igae`: 123 filas | 123 meses únicos | 0 duplicados
  - `inpc`: 123 filas | 123 meses únicos | 0 duplicados
- **Gold, salida verificada:**
  - **Ruta:** `data/gold/dataset_maestro` (particionado por `anio`)
  - **Dimensiones:** 123 filas × 43 columnas
  - **Integridad:** Relectura Parquet validada sin discrepancias de conteo, duplicados ni nulos en variables críticas.
- **Métricas de Spark por capa ETL (`src.pipeline`):**

  | Capa | Jobs | Tareas | Lee | Escribe | Shuffle |
  | :--- | ---: | ---: | ---: | ---: | ---: |
  | Bronze | 20 | 42 | 228.8 MB | 4.8 MB | 0.9 KB |
  | Silver | 61 | 72 | 6.4 MB | 0.07 MB | 181.5 KB |
  | Gold | 40 | 50 | 0.5 MB | 0.17 MB | 163.8 KB |

- **Métricas de Spark en Consumo Analítico (`notebooks/02_analisis_dataset_maestro_final.ipynb`):**

  | Etapa | Jobs | Tareas | Lee | Shuffle | Tiempo tareas |
  | :--- | ---: | ---: | ---: | ---: | ---: |
  | `2_carga` | 6 | 36 | 0.64 MB | 21.1 KB | 11.45 s |
  | `2_validacion_calidad` | 7 | 37 | 0.64 MB | 4.4 KB | 3.99 s |
  | `2b_consumo_spark_sql` | 5 | 35 | 1.03 MB | 3.2 KB | 3.83 s |
  | `3_indicadores` | 3 | 13 | 0.33 MB | 37.6 KB | 1.26 s |
  | `4_objetivo1` | 64 | 274 | 6.93 MB | 73.3 KB | 13.64 s |
  | `5_objetivo2` | 6 | 6 | 0.35 MB | 0.0 KB | 0.12 s |
  | `6_periodos` | 1 | 1 | 0.06 MB | 0.0 KB | 0.09 s |
  | `7_visualizaciones` | 1 | 1 | 0.06 MB | 0.0 KB | 0.01 s |

### Capturas de la Spark UI

Tomadas del History Server sobre la ejecución de `python -m src.pipeline` registrada en `spark-events/` (carpeta [`reports/spark_ui/`](reports/spark_ui/)):

| Captura | Qué muestra |
| :--- | :--- |
| [01 · Jobs y línea de tiempo](reports/spark_ui/01_jobs_linea_de_tiempo.jpg) | Los jobs etiquetados por capa (`bronze`, `silver`, `gold`) y su secuencia en el tiempo; el job largo de Bronze es la lectura del CSV. |
| [02 · Stage de Bronze](reports/spark_ui/02_bronze_stage_12_tareas_en_paralelo.jpg) | Lectura del CSV (215.6 MiB, 746,676 filas) en **12 tareas simultáneas**, una por núcleo, en una sola ola; escribe 4.1 MiB de Parquet. |
| [03](reports/spark_ui/03_silver_plan_1_scan_filter.jpg) · [04](reports/spark_ui/04_silver_plan_2_dedup_exchange_4_particiones.jpg) · [05](reports/spark_ui/05_silver_plan_3_aqe_coalesce_escritura.jpg) · Plan de Silver | `Scan parquet` → `Filter` (de 736,633 a 8,526 filas) → `HashAggregate` parcial → `Exchange` de **4 particiones** (137.6 KiB) → `AQEShuffleRead` que **fusiona las 4 en 1** → `HashAggregate` final (5,481 filas) → escritura. |
| [06](reports/spark_ui/06_gold_plan_1_pivot_broadcast.jpg) · [07](reports/spark_ui/07_gold_plan_2_broadcast_hash_join.jpg) · [08](reports/spark_ui/08_gold_plan_3_window_una_particion.jpg) · Plan de Gold | Pivot en dos agregaciones, tres `BroadcastExchange` + `BroadcastHashJoin` (123 filas), y `Exchange` de **1 partición** → `Sort` → `Window` para los `lag(12)`. |
| [09 · Executors](reports/spark_ui/09_executors.jpg) | **1 executor (driver) con 12 núcleos**; 171 tareas, 0 fallidas, GC de 0.3 s en 35 s de tareas. |
| [10 · Environment](reports/spark_ui/10_environment_configuracion.jpg) | `spark.master = local[*]`, `spark.sql.shuffle.partitions = 4`, *event log* activado. |

Las gráficas del análisis están exportadas en [`reports/figuras/`](reports/figuras/) para usarlas en la presentación.

## Estado del desarrollo

| Etapa | Estado | Detalle |
| :--- | :--- | :--- |
| Definición del proyecto y fuentes | Completa | Pregunta, objetivos, fuentes y arquitectura documentados en `docs/`. |
| Ingesta y capa Bronze | Completa | [notebooks/01_ingesta.ipynb](notebooks/01_ingesta.ipynb) descarga SHCP, SAT, IGAE e INPC a `data/raw/` y los guarda en `data/bronze/`. |
| Periodo de estudio | Ajustado | 2015-01 a 2025-03: el IGAE de datos abiertos del INEGI no tiene meses posteriores; se inicia en 2015 para excluir el cambio de régimen de la Reforma Hacendaria de 2014. |
| Capa Silver (limpieza y validación) | Completa | Llave `fecha` mensual, deduplicación y homologación de SHCP, IGAE de ancho a largo; nulos, duplicados por llave y 123 meses continuos. |
| Capa Gold (integración) | Completa | Pivot de SHCP, joins broadcast, variables reales, ratios y variaciones anuales en `data/gold/dataset_maestro`. |
| Consumo analítico y modelado | Completa | [notebooks/02_analisis_dataset_maestro_final.ipynb](notebooks/02_analisis_dataset_maestro_final.ipynb): validación Gold, consultas Spark SQL, significancia, desglose ISR/IVA, Granger, robustez con gasto programable, regresión PySpark ML + Newey-West HAC, comparativo por periodos, 5 gráficas ejecutivas, arquitectura aplicada, conclusiones y limitaciones. |
| Optimización de Spark | Completa | Configuración medida y justificada en [docs/guia_optimizacion.md](docs/guia_optimizacion.md#3-resultados-medidos-en-este-proyecto); métricas por capa y etapa en cada ejecución. |

## Documentación

- [docs/fuentes.md](docs/fuentes.md): fuentes investigadas y justificación.
- [docs/arquitectura.md](docs/arquitectura.md): diagrama del pipeline y operaciones PySpark.
- [docs/guia_optimizacion.md](docs/guia_optimizacion.md): ciclo de optimización y resultados medidos que justifican la configuración de Spark.
- [docs/roles.md](docs/roles.md): organización, responsabilidades y decisiones.

