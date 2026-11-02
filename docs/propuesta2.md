# Propuesta: Impacto de la política y el equilibrio fiscal sobre el crecimiento económico y la inflación en México (2015–2025)

## Pregunta de investigación
¿De qué manera el equilibrio fiscal —medido a través de la relación entre el gasto presupuestario (SHCP) y la recaudación tributaria (SAT)— incide sobre el crecimiento de la actividad económica (IGAE) y la dinámica de precios (INPC) en México durante el periodo 2015–2025?

## Objetivo
- **Objetivo General:**
Evaluar el impacto coordinado del gasto de la SHCP y la recaudación del SAT sobre la actividad económica (IGAE) y la inflación (INPC) mediante el procesamiento e integración de macrodatos con PySpark.
- **Objetivos Específicos:** 
Medir la correlación y el efecto rezagado del gasto público y la recaudación del ISR/IVA sobre el crecimiento del IGAE.Identificar si incrementos en la recaudación tributaria amortiguan o exacerban presiones inflacionarias derivadas del gasto del sector público.

## Fuente Adicional: Servicio de Administración Tributaria (SAT)
- **Institución y Enlace oficial:** Servicio de Administración Tributaria (SAT) / Datos Abiertos https://www.sat.gob.mx/minisitio/DatosAbiertos/recaudacion.html
- **Variables:** Recaudación tributaria bruta y neta (desagregada por ISR, IVA, IEPS) e ingresos por actos de fiscalización.
- **Periodicidad:** Mensual.   
- **Cobertura temporal:** 2015 – 2025.
- **Formato de acceso:** Archivos CSV, Excel y Datasets descargables del portal gubernamental.   
- **Relación con las fuentes base:** Cierra la ecuación de las finanzas públicas al agregar el lado de los ingresos (SAT) frente a la fuente base de egresos (SHCP). Permite contrastar si los aumentos en IGAE e INPC responden al dinamismo de la recaudación del IVA (consumo) y del ISR (ingresos/empleo).

## Relación de la fuente adicional con las vistas en clase

| Variable | Tipo de Variable | Fuente | Indicador / Definición | Periodicidad | Cobertura Temporal |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Gasto Programable** | Explicativa (Fiscal - Egresos) | **SHCP** | Gasto ejercido del sector público presupuestario. | Mensual | 2015 – 2025 |
| **Recaudación Tributaria** | Explicativa (Fiscal - Ingresos) | **SAT** | Ingresos tributarios netos (desglose por ISR e IVA). | Mensual | 2015 – 2025|
| **IGAE** | Dependiente (Producto) | **INEGI** | Índice Global de la Actividad Económica (Base 2018=100) | Mensual | 2015 – 2025 |
| **INPC** | Dependiente (Precios) | **INEGI** | Índice Nacional de Precios al Consumidor (Variación interanual). | Mensual | 2015 – 2025 |

## Diagrama del flujo de datos

```text
[ Fuentes ]      SHCP (Egresos)  │  SAT (Ingresos)  │  IGAE (Producto)  │  INPC (Precios)
                        ↓               ↓                   ↓                 ↓
[ Ingesta ]     ─────────────────────── spark.read ──────────────────────────────────
                        ↓
[ Limpieza ]    Estandarización de fechas (YYYY-MM), tipos de datos, limpieza de nulos
                        ↓
[ Transform ]   Cálculo de variaciones interanuales y ratio fiscal (Egresos SHCP / Recaudación SAT)
                        ↓
[ Integración ] Join por clave temporal ('anio_mes') para consolidar DataFrame Maestro
                        ↓
[ Almacén ]     Guardado del DataFrame final en formato Parquet (particionado por 'anio')
                        ↓
[ Análisis ]    Matriz de correlación y agregaciones estadísticas con PySpark SQL
