# Fuentes investigadas

## Fuente 1 (seleccionada): Recaudación de ingresos tributarios (SAT, Datos Abiertos)
- **Institución:** Servicio de Administración Tributaria (SAT), publicada también en datos.gob.mx.
- **Página oficial:** https://www.sat.gob.mx/minisitio/DatosAbiertos/index.html
- **Conjunto de datos:** "Recaudación de ingresos tributarios del Gobierno Federal" en https://datos.gob.mx/busca/dataset/85368508-8f2e-4a55-9dcb-cf536742e498
- **Variables:** recaudación por tipo de impuesto (ISR, IVA, IEPS, entre otros). [VERIFICAR columnas exactas en el diccionario de datos]
- **Cobertura temporal:** [VERIFICAR años disponibles; se necesita al menos 2018-2025 para cubrir antes, durante y después del COVID]
- **Periodicidad:** [VERIFICAR: mensual o anual]
- **Formato de acceso:** archivo descargable (CSV/Excel) [VERIFICAR tamaño]
- **Relación con las fuentes base:** se une por mes con IGAE, INPC y SHCP.

## Fuente 2: Puestos de trabajo afiliados al IMSS
- **Institución:** Instituto Mexicano del Seguro Social (IMSS), Datos Abiertos.
- **Página oficial:** http://datos.imss.gob.mx/
- **Variables:** puestos de trabajo afiliados, salario base de cotización promedio, entidad federativa. [VERIFICAR columnas exactas]
- **Cobertura temporal:** [VERIFICAR años disponibles]
- **Periodicidad:** mensual.
- **Formato de acceso:** archivos descargables [VERIFICAR formato y tamaño]
- **Relación con las fuentes base:** se une por mes con el IGAE (empleo y actividad) y con la recaudación del SAT, porque el ISR depende en buena medida del empleo formal.

## Justificación de la fuente seleccionada
Se selecciona la del SAT porque mide directamente la variable de interés (recaudación tributaria por impuesto), mientras que la del IMSS explicaría solo uno de sus componentes (el ISR ligado al empleo formal). Además, el SAT permite construir el indicador de eficiencia recaudatoria frente al IGAE, que es el centro de la pregunta analítica. El empleo del IMSS queda como variable explicativa para una posible ampliación del análisis.

Nota: la SHCP también publica ingresos tributarios, por lo que la fuente del SAT se justifica por su mayor detalle (por impuesto y, si existe, por entidad o tipo de contribuyente). Hay que confirmarlo al revisar el diccionario de datos.
