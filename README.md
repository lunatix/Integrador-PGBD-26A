# INTEGRANTES

* Dimas Salgado Aline Yaremys
* Romero Gálvez Roberto Alejandro
* Díaz Aguila Mauricio Adrián 
* Ruesga Rodriguez Gloria Viridiana

# Impacto de la política y el equilibrio fiscal sobre el crecimiento económico y la inflación en México (2015–2025-03)

## Pregunta de investigación
¿De qué manera el equilibrio fiscal —medido a través de la relación entre el gasto presupuestario (SHCP) y la recaudación tributaria (SAT)— incide sobre el crecimiento de la actividad económica (IGAE) y la dinámica de precios (INPC) en México durante el periodo de enero de 2015 a marzo de 2025?

## Objetivo
- **Objetivo General:**
Evaluar el impacto coordinado del gasto de la SHCP y la recaudación del SAT sobre la actividad económica (IGAE) y la inflación (INPC) mediante el procesamiento e integración de macrodatos con PySpark.
- **Objetivos Específicos:** 
  1. Medir la correlación y el efecto rezagado del gasto público y la recaudación del ISR/IVA sobre el crecimiento del IGAE.
  2. Identificar si incrementos en la recaudación tributaria amortiguan o exacerban presiones inflacionarias derivadas del gasto del sector público.

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

## Roles (sesión inicial, rotan cada sesión)
| Integrante | Rol |
| :--- | :--- |
| Ruesga Rodriguez Gloria Viridiana | Developer |
| Díaz Aguila Mauricio Adrián | Developer |
| Romero Gálvez Roberto Alejandro | Technical Reviewer |
| Dimas Salgado Aline Yaremys | Documentation Lead |

Detalle en [docs/roles.md](docs/roles.md).

## Documentación
- [docs/fuentes.md](docs/fuentes.md): fuentes investigadas y justificación.
- [docs/arquitectura.md](docs/arquitectura.md): diagrama del pipeline y operaciones PySpark.
- [docs/roles.md](docs/roles.md): organización, responsabilidades y decisiones.
