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
| Guía de optimización | Pendiente | [docs/guia_optimizacion.md](docs/guia_optimizacion.md) creado, sin contenido. |

## Documentación
- [docs/fuentes.md](docs/fuentes.md): fuentes investigadas y justificación.
- [docs/arquitectura.md](docs/arquitectura.md): diagrama del pipeline y operaciones PySpark.
- [docs/roles.md](docs/roles.md): organización, responsabilidades y decisiones.
