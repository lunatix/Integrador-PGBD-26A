# Proyecto Integrador — Pipeline Modular en PySpark (Actividad 11)

Pipeline de procesamiento de datos macroeconómicos y fiscales de México desarrollado en **PySpark** bajo una arquitectura modular reproducible. Integra series mensuales de gasto público (**SHCP**), recaudación tributaria (**SAT**), actividad económica (**INEGI - IGAE**) e inflación (**INEGI - INPC**) para el periodo **enero de 2015 a marzo de 2025** (123 meses).

---

## Arquitectura del flujo de datos

```mermaid
flowchart TD
    A1[SHCP<br/>Ingresos y gastos GF<br/>CSV] --> B
    A2[SAT<br/>Ingresos tributarios<br/>XLS → CSV] --> B
    A3[INEGI IGAE<br/>Actividad económica<br/>CSV] --> B
    A4[INEGI INPC<br/>Precios al consumidor<br/>CSV] --> B
    B[1. Ingesta / Extracción<br/>src/ingestion/loader.py] --> C[2. Limpieza y homologación<br/>src/transformations/clean.py]
    C --> D[3. Validación de calidad<br/>src/quality/validators.py]
    D --> E[4. Integración y transformación<br/>src/transformations/integrate.py]
    E --> F[5. Almacenamiento y relectura<br/>Parquet particionado por año]
```

---

## Estructura del repositorio

```text
Integrador-PGBD-26A/
├── data/
│   ├── raw/                          # Fuentes crudas descargadas (ignorado en Git)
│   └── processed/                    # Dataset maestro en Parquet particionado por año (ignorado en Git)
├── docs/                             # Documentación de fuentes y diseño del proyecto
├── notebooks/
│   └── 01_ingesta.ipynb              # Descarga automatizada, descompresión y conversión SAT XLS → CSV
├── reports/
│   └── evidencia_ejecucion_act11.txt   # Registro de conteos, validaciones y métricas de ejecución
├── src/
│   ├── __init__.py
│   ├── pipeline.py                   # Orquestador ejecutable de principio a fin
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── loader.py                 # Sesión de Spark, validación de rutas y esquemas explícitos
│   ├── quality/
│   │   ├── __init__.py
│   │   └── validators.py             # Controles de nulos, duplicados por llave y continuidad temporal
│   └── transformations/
│       ├── __init__.py
│       ├── clean.py                  # Llave común `fecha`, deduplicación SHCP y homologación monetaria
│       └── integrate.py              # Pivot SHCP, joins broadcast, ventanas YoY y escritura Parquet
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Fuentes de datos y ventana de estudio

Todas las fuentes se homologan a una granularidad mensual común (`1 fila = 1 mes`) dentro de la ventana **2015-01-01 a 2025-03-01** (acotada al último dato disponible en la serie abierta del IGAE).

| Fuente | Institución | Archivo en `data/raw/` | Filas crudas | Filas limpias (ventana) | Unidad / Escala final |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **Gasto Presupuestario (`XBC*`)** | SHCP | `ingreso_gasto_finan.csv` | 746,676 | 3,567 *(29 claves × 123 meses)* | Millones de pesos corrientes |
| **Ingresos Tributarios** | SAT | `ingresos_tributarios_sat.csv` | 201 | 123 | Millones de pesos corrientes |
| **IGAE (Serie Total)** | INEGI | `igae_mensual_csv/.../conjunto_de_datos_igae_*.csv` | 120 | 123 | Índice de volumen físico (Base 2018 = 100) |
| **INPC (Índice General)** | INEGI | `inpc_mensual_csv/.../conjunto_de_datos_inpc_mensual.csv` | 2,002 | 123 | Índice de precios (Base 2Q jul 2018 = 100) |

---

## Operaciones PySpark por etapa

| Etapa | Módulo | Operaciones PySpark | Decisiones técnicas |
| :--- | :--- | :--- | :--- |
| **1. Ingesta / Extracción** | `notebooks/01_ingesta.ipynb`<br>`src/ingestion/loader.py` | `SparkSession.builder`, `spark.read.csv(header=True, schema=...)` | El archivo `.xls` del SAT se convierte previamente a CSV en el notebook mediante `pandas` + `xlrd`. Se definen esquemas explícitos (`StructType`) para SHCP e INPC con el fin de evitar doble lectura y errores de inferencia. Se asigna `sys.executable` a `PYSPARK_PYTHON` para compatibilidad en Windows. |
| **2. Limpieza y homologación** | `src/transformations/clean.py` | `to_date`, `trunc`, `create_map`, `regexp_extract`, `regexp_replace`, `filter`, `stack`, `dropna`, `dropDuplicates` | Se construye la llave común `fecha` (primer día de cada mes, tipo `date`). En **SHCP** se filtran claves `XBC*` mensuales en pesos, se eliminan **2,523 registros exactamente duplicados** publicados desde 2018 y se divide entre `1,000` para convertir de miles a millones de pesos. En **SAT** se limpian saltos de línea en encabezados y marcas preliminares (`*`). En **IGAE** se aplica `F.stack` para pasar de formato ancho a largo. |
| **3. Validación de calidad** | `src/quality/validators.py` | `count`, `filter(col.isNull())`, `agg(min, max)`, `groupBy(llave).count().filter(count > 1)` | Verifica conteos positivos en la extracción, ausencia total de nulos en llaves y métricas, cero duplicados por `(fecha, CLAVE_DE_CONCEPTO)` en SHCP y continuidad exacta de **123 meses únicos** por fuente antes de integrar. |
| **4. Integración y transformación** | `src/transformations/integrate.py` | `groupBy("fecha").pivot("CLAVE_DE_CONCEPTO")`, `join(broadcast(...), on="fecha", how="inner")`, `Window.orderBy("fecha")`, `lag(..., 12)` | Se pivotean las 29 claves presupuestarias de SHCP y se unen las 4 fuentes mediante `broadcast()` para evitar *shuffle* innecesario. Se calculan cifras reales deflactadas con INPC, ratios fiscales y variaciones porcentuales interanuales (`YoY`) para neutralizar la estacionalidad mensual. |
| **5. Almacenamiento y verificación** | `src/transformations/integrate.py` | `write.mode("overwrite").partitionBy("anio").parquet(...)`, `spark.read.parquet(...)` | Persiste el dataset maestro en formato columnar Parquet particionado por año (`anio`) y ejecuta una relectura inmediata para validar que el conteo de filas (123) y el esquema (43 columnas) coincidan exactamente. |

---

## Variables principales del dataset maestro

El archivo de salida (`data/processed/dataset_maestro.parquet`) contiene **123 filas × 43 columnas**:

* **Llave y partición:** `fecha` (`date`), `anio` (`int`).
* **Gasto del Gobierno Federal (`SHCP`, millones de pesos):** `shcp_xbc` (Gasto Total del Gobierno Federal) y sus 28 desgloses programables, no programables, corrientes, de capital y costo financiero (`shcp_xbc10` a `shcp_xbc23`).
* **Recaudación Tributaria (`SAT`, millones de pesos):** `sat_total_tributarios`, `sat_isr`, `sat_iva`, `sat_ieps`.
* **Indicadores Macroeconómicos (`INEGI`):** `igae_total`, `inpc`.
* **Variables Analíticas Derivadas:**
  * `sat_total_real`: recaudación tributaria total deflactada con el INPC (`sat_total_tributarios / (inpc / 100)`).
  * `ratio_tributario_gasto`: proporción del gasto total del Gobierno Federal cubierta con ingresos tributarios (`sat_total_tributarios / shcp_xbc`).
  * `ratio_isr_tributario`: participación del ISR dentro del total de ingresos tributarios (`sat_isr / sat_total_tributarios`).
  * `igae_var_anual_pct`, `inflacion_anual_pct`, `sat_real_var_anual_pct`: variaciones porcentuales anuales (`lag` de 12 meses; `NULL` en 2015 al corresponder al primer año de la ventana).

---

## Instrucciones de ejecución

### 1. Instalar dependencias
Desde la raíz del repositorio:

```bash
pip install -r requirements.txt
```

### 2. Obtener las fuentes crudas (`data/raw/`)
Abrir y ejecutar todas las celdas de `notebooks/01_ingesta.ipynb` para descargar las cuatro fuentes oficiales, descomprimir los archivos ZIP de INEGI y convertir el archivo `.xls` del SAT a CSV.

### 3. Ejecutar el pipeline modular completo
Desde la raíz del repositorio (`Integrador-PGBD-26A/`):

```bash
python -m src.pipeline
```

> **Nota en Windows con Anaconda:** si `python` no está registrado en el `PATH` global de la terminal, ejecutar especificando la ruta del intérprete del entorno, por ejemplo:
> ```powershell
> & "C:\Users\<usuario>\anaconda3\python.exe" -m src.pipeline
> ```

---

## Evidencia de ejecución (Actividad 11)

El registro completo generado automáticamente por el pipeline se encuentra en [`reports/evidencia_ejecucion_act11.txt`](reports/evidencia_ejecucion_act11.txt).

* **Extracción cruda:**
  * `shcp`: 746,676 filas leídas
  * `sat`: 201 filas leídas
  * `igae`: 120 filas leídas
  * `inpc`: 2,002 filas leídas
* **Validación de calidad en ventana (`2015-01-01` a `2025-03-01`):**
  * `shcp`: 3,567 filas | 123 meses únicos | 0 duplicados
  * `sat`: 123 filas | 123 meses únicos | 0 duplicados
  * `igae`: 123 filas | 123 meses únicos | 0 duplicados
  * `inpc`: 123 filas | 123 meses únicos | 0 duplicados
* **Salida verificada:**
  * **Ruta:** `data/processed/dataset_maestro.parquet` (particionado por `anio`)
  * **Dimensiones:** 123 filas × 43 columnas
  * **Integridad:** Relectura Parquet validada sin discrepancias de conteo ni de esquema.
  
=======
# INTEGRANTES

* Díaz Aguila Mauricio Adrián
* Dimas Salgado Aline Yaremys
* Romero Gálvez Roberto Alejandro
* Ruesga Rodriguez Gloria Viridiana

# Impacto del gasto del gobierno y el cobro de impuestos en la economía y la inflación en México (2015–2025)

## Pregunta de investigación
¿De qué manera influye la relación entre el dinero que gasta el gobierno (Secretaría de Hacienda) y lo que cobra en impuestos (SAT) en el crecimiento económico y en la subida de precios (inflación) en México, durante el periodo de enero de 2015 a marzo de 2025?

## Objetivo
- **Objetivo General:**
Evaluar cómo afectan de forma conjunta el gasto del gobierno y la recaudación de impuestos al crecimiento de la economía y al aumento del costo de vida en México, analizando grandes volúmenes de información mediante la herramienta PySpark.
- **Objetivos Específicos:** 
  1. Identificar qué tan conectados están el gasto público y los impuestos principales (como ISR e IVA) con el crecimiento económico del país, y cuánto tiempo tarda en verse su impacto.
  2. Descubrir si aumentar el cobro de impuestos ayuda a frenar la subida de precios provocada por el gasto del gobierno o si, por el contrario, la empeora.
 
## Variables por instancia a considerar 

| Variable | Tipo de Variable | Fuente | Indicador / Definición | Periodicidad | Cobertura Temporal |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Gasto Programable** | Explicativa (Fiscal - Egresos) | **SHCP** | Gasto ejercido del sector público presupuestario. | Mensual | 2015-01 – 2025-03 |
| **Recaudación Tributaria** | Explicativa (Fiscal - Ingresos) | **SAT** | Ingresos tributarios netos (desglose por ISR e IVA). | Mensual | 2015-01 – 2025-03 |
| **IGAE** | Dependiente (Producto) | **INEGI** | Índice Global de la Actividad Económica (Base 2018=100) | Mensual | 2015-01 – 2025-03 |
| **INPC** | Dependiente (Precios) | **INEGI** | Índice Nacional de Precios al Consumidor (Variación interanual). | Mensual | 2015-01 – 2025-03 |

## Resultados esperados
- DataFrame maestro mensual (SHCP + SAT + IGAE + INPC) almacenado en Parquet.
- Indicador de equilibrio fiscal mensual: gasto del Gobierno Federal / recaudación tributaria.
- Correlaciones contemporáneas y rezagadas (1, 3, 6 y 12 meses) del indicador fiscal contra la variación anual del IGAE y del INPC.
- Comparativo por periodos: prepandemia (2015–2019), pandemia (2020–2021) y pospandemia (2022–2025-03).

## Estado actual del desarrollo
| Etapa | Estado | Detalle |
| :--- | :--- | :--- |
| Definición del proyecto y fuentes | Completa | Pregunta, objetivos, fuentes y arquitectura documentados en `docs/`. |
| Ingesta | Completa | [notebooks/01_ingesta.ipynb](notebooks/01_ingesta.ipynb) descarga SHCP, SAT, IGAE e INPC a `data/raw/` y verifica con PySpark esquema, volumen y cobertura. |
| Periodo de estudio | Ajustado | 2015-01 a 2025-03: el IGAE de datos abiertos del INEGI no tiene meses posteriores. |
| Transformación e integración | Pendiente | Llave `fecha` mensual, pivot de SHCP, homologación de unidades, IGAE de ancho a largo y join en el DataFrame maestro. |
| Validaciones de calidad | Pendiente | Nulos, duplicados por fecha y meses faltantes. |
| Análisis | Pendiente | Rezagos, correlaciones y comparativo por periodos. |
| Guía de optimización | Documento de consulta | [docs/guia_optimizacion.md](docs/guia_optimizacion.md) creado, sirve para mejorar los pipelines del proyecto y se actualizará conforme a la implementación |

## Documentación
- [docs/fuentes.md](docs/fuentes.md): fuentes investigadas y justificación.
- [docs/arquitectura.md](docs/arquitectura.md): diagrama del pipeline y operaciones PySpark.
- [docs/roles.md](docs/roles.md): organización, responsabilidades y decisiones.

