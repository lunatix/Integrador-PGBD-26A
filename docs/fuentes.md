# Fuentes investigadas

Fuentes base del curso: SHCP (ingresos y gastos del Gobierno Federal), INEGI INPC e INEGI IGAE. Abajo, las dos fuentes adicionales investigadas.

## Fuente 1 (seleccionada): Recaudación de ingresos tributarios (SAT, Datos Abiertos)
- **Institución:** Servicio de Administración Tributaria (SAT). También listada en datos.gob.mx.
- **Página oficial:** https://www.sat.gob.mx/minisitio/DatosAbiertos/recaudacion.html
- **Archivo:** [`IngresosTributarios.xls`](https://wu1agsprosta001.blob.core.windows.net/agsc-publicaciones/Datos_abiertos/Cifras_SAT/Documents/IngresosTributarios.xls)
- **Variables** (millones de pesos, verificadas en el archivo): Año, Mes, Total de ingresos tributarios, ISR, IVA, IEPS (desglosado en gasolinas y diésel, tabacos, alcohol, cerveza, juegos y sorteos, telecomunicaciones, bebidas energetizantes, bebidas saborizadas, alimentos de alta densidad calórica, plaguicidas, carbono), Importaciones, Exportaciones, ISAN, Impuesto por exploración y extracción de hidrocarburos y Otros.
- **Periodicidad:** mensual.
- **Cobertura temporal:** enero de 2010 a agosto de 2026 (200 meses; las cifras más recientes son preliminares). Cubre el periodo de estudio 2015–2025.
- **Formato de acceso:** descarga directa en Excel 97 (`.xls`, alrededor de 150 KB). Sin API ni registro.
- **Calidad observada:** encabezados en dos filas (filas 5 y 6), notas al pie al final del archivo, meses escritos como texto ("Enero") y algunos valores negativos de IEPS en gasolinas (reflejan estímulos fiscales, no son errores).
- **Relación con las fuentes base:** se une por mes (`fecha` = primer día del mes) con SHCP, IGAE e INPC.

## Fuente 2: Puestos de trabajo afiliados al IMSS
- **Institución:** Instituto Mexicano del Seguro Social (IMSS), Datos Abiertos.
- **Página oficial:** http://datos.imss.gob.mx/
- **Variables:** puestos de trabajo afiliados, salario base de cotización promedio, entidad federativa.
- **Periodicidad:** mensual.
- **Cobertura temporal:** no disponible al 2026-11-04; el portal de datos abiertos del IMSS no responde (error SSL 29 / HTTP 503). Pendiente para la siguiente sesión.
- **Formato de acceso:** archivos descargables.
- **Relación con las fuentes base:** se une por mes con el IGAE (empleo y actividad) y con la recaudación del SAT, porque el ISR depende en buena medida del empleo formal.

## Justificación de la fuente seleccionada
**Utilidad:** el SAT mide directamente la variable de la pregunta (recaudación por impuesto). El IMSS solo explica un componente del ISR, el ligado al empleo formal. El SAT permite construir el indicador de equilibrio fiscal (gasto SHCP / recaudación SAT).

**Viabilidad (verificada):** el archivo se descargó y se integró en una prueba con PySpark junto con las fuentes base de los laboratorios 6 y 7.

| Fuente | Desde | Hasta | Meses |
| :--- | :--- | :--- | :--- |
| SHCP (gasto total, clave XBC) | 2011-01 | 2026-07 | 187 |
| SAT (recaudación) | 2010-01 | 2026-08 | 200 |
| IGAE (serie "Total") | 1993-01 | 2026-07 | 403 |
| INPC | 2003-01 | 2026-08 | 284 |
| **Maestro (inner join por mes)** | **2011-01** | **2026-07** | **187** |

La intersección cubre por completo el periodo 2015–2025. La correlación preliminar del ratio fiscal contra la variación anual del IGAE es de 0.08 (0.11 con rezago de 3 meses). Es baja, lo que indica que habrá que trabajar con rezagos y con desestacionalización.

El empleo del IMSS queda como variable explicativa para una posible ampliación del análisis.

Nota: la SHCP también publica ingresos tributarios. El SAT se justifica por su desglose por impuesto (ISR, IVA y 11 rubros de IEPS).
