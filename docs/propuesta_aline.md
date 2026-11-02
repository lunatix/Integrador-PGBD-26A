# Propuesta: Eficiencia recaudatoria y sostenibilidad fiscal en México frente al ciclo económico (COVID-19)

## Pregunta analítica
¿Cómo respondió la recaudación tributaria del SAT (ISR, IVA, IEPS) a los ciclos de la actividad económica (IGAE), en particular durante la caída y recuperación del COVID-19 (2020-2021), y qué implica esto para la sostenibilidad fiscal (SHCP)?

### Objetivo
Medir la eficiencia recaudatoria (recaudación real en relación con la actividad económica) y su sensibilidad al ciclo, comparando el periodo previo al COVID, el choque de 2020 y la recuperación posterior.

### Variables necesarias
- Recaudación tributaria por impuesto y mes (SAT)
- Actividad económica mensual (IGAE)
- Precios (INPC), para expresar la recaudación en términos reales
- Ingresos, gasto y balance público (SHCP), para evaluar la sostenibilidad fiscal

### Resultados esperados
- Comparativa de la recaudación real vs. el IGAE por periodo (pre-COVID, choque, recuperación)
- Indicador de eficiencia recaudatoria (recaudación real / IGAE) y su evolución mensual
- Identificación de qué impuesto fue más sensible al ciclo (ISR vs. IVA vs. IEPS)
- Rezagos entre la caída de la actividad y la caída de la recaudación
- Lectura de sostenibilidad fiscal: cuánto del gasto público (SHCP) se cubrió con ingresos tributarios

## Fuente adicional: Recaudación de ingresos tributarios (SAT, Datos Abiertos)
- **Institución:** Servicio de Administración Tributaria (SAT), publicada también en datos.gob.mx.
- **Página oficial:** https://www.sat.gob.mx/minisitio/DatosAbiertos/index.html
- **Conjunto de datos:** "Recaudación de ingresos tributarios del Gobierno Federal" en https://datos.gob.mx/busca/dataset/85368508-8f2e-4a55-9dcb-cf536742e498
- **Contenido:** recaudación por tipo de impuesto (ISR, IVA, IEPS, entre otros). [VERIFICAR columnas exactas en el diccionario de datos]
- **Cobertura temporal:** [VERIFICAR años disponibles; se necesita al menos 2018-2025 para cubrir antes, durante y después del COVID]
- **Periodicidad:** [VERIFICAR: mensual o anual]
- **Formato de acceso:** archivo descargable (CSV/Excel) [VERIFICAR tamaño]
- **Claves de unión:** mes/año (y entidad, si el conjunto la incluye)

## Cómo encaja con las fuentes base
| Fuente base | Unión |
|---|---|
| IGAE | mes (serie nacional de actividad económica) |
| INPC | mes, índice general para deflactar la recaudación |
| SHCP | mes, ingresos, gasto y balance público |

Nota: la SHCP también publica ingresos tributarios, por lo que la fuente del SAT se justifica por su mayor detalle (por impuesto y, si existe, por entidad o tipo de contribuyente). Hay que confirmarlo al revisar el diccionario de datos.

## Pipeline en PySpark (resumen)
1. **Ingesta:** lectura de CSV con esquema explícito (Excel a CSV si hace falta).
2. **Validación:** descartar registros sin fecha o con montos nulos o negativos inesperados; revisar duplicados.
3. **Transformación:** estandarizar fechas a mes; convertir montos a numérico; deflactar con INPC (pesos constantes); crear variable de periodo (pre-COVID, choque, recuperación).
4. **Agregación:** recaudación por impuesto y mes; razón recaudación real / IGAE.
5. **Integración:** `join` por mes con IGAE, INPC y SHCP; rezagos y variaciones anuales con `Window` y `lag`.
6. **Almacenamiento y análisis:** tabla final en Parquet; gráficas y comparativa por periodo.
