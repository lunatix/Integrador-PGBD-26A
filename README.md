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
