# INTEGRANTES

* Dimas Salgado Aline Yaremys
* Romero Gálvez Roberto Alejandro
* Díaz Aguila Mauricio Adrián 
* Ruesga Rodriguez Gloria Viridana

# Impacto de la política y el equilibrio fiscal sobre el crecimiento económico y la inflación en México (2015–2025)

## Pregunta de investigación
¿De qué manera el equilibrio fiscal —medido a través de la relación entre el gasto presupuestario (SHCP) y la recaudación tributaria (SAT)— incide sobre el crecimiento de la actividad económica (IGAE) y la dinámica de precios (INPC) en México durante el periodo 2015–2025?

## Objetivo
- **Objetivo General:**
Evaluar el impacto coordinado del gasto de la SHCP y la recaudación del SAT sobre la actividad económica (IGAE) y la inflación (INPC) mediante el procesamiento e integración de macrodatos con PySpark.
- **Objetivos Específicos:** 
  1. Medir la correlación y el efecto rezagado del gasto público y la recaudación del ISR/IVA sobre el crecimiento del IGAE.
  2. Identificar si incrementos en la recaudación tributaria amortiguan o exacerban presiones inflacionarias derivadas del gasto del sector público.

## Variables por instancia a considerar 

| Variable | Tipo de Variable | Fuente | Indicador / Definición | Periodicidad | Cobertura Temporal |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Gasto Programable** | Explicativa (Fiscal - Egresos) | **SHCP** | Gasto ejercido del sector público presupuestario. | Mensual | 2015 – 2025 |
| **Recaudación Tributaria** | Explicativa (Fiscal - Ingresos) | **SAT** | Ingresos tributarios netos (desglose por ISR e IVA). | Mensual | 2015 – 2025|
| **IGAE** | Dependiente (Producto) | **INEGI** | Índice Global de la Actividad Económica (Base 2018=100) | Mensual | 2015 – 2025 |
| **INPC** | Dependiente (Precios) | **INEGI** | Índice Nacional de Precios al Consumidor (Variación interanual). | Mensual | 2015 – 2025 |
