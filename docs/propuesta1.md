# Propuesta: Sequía y economía en México

## Pregunta analítica
¿La sequía en un estado se asocia con menor actividad agropecuaria (IGAE), mayor inflación de alimentos (INPC) y cambios en el gasto público (SHCP)?

## Fuente adicional: Monitor de Sequía de México (SMN-CONAGUA)
- **Institución:** Servicio Meteorológico Nacional (SMN), CONAGUA.
- **Página oficial:** https://smn.conagua.gob.mx/es/climatologia/monitor-de-sequia/monitor-de-sequia-en-mexico
- **Descarga directa (Excel, 2.1 MB):** https://smn.conagua.gob.mx/tools/RESOURCES/Monitor%20de%20Sequia%20en%20Mexico/MunicipiosSequia.xlsx
- **Contenido:** categoría de sequía por municipio (D0 anormalmente seco a D4 excepcional).
- **Cobertura:** 2,478 municipios, enero 2003 a octubre 2026 (~1.1 millones de celdas municipio-fecha).
- **Periodicidad:** mensual al inicio, quincenal después.
- **Claves:** estado, municipio (5 dígitos), organismo y consejo de cuenca.

## Cómo encaja con las fuentes base
| Fuente base | Unión |
|---|---|
| IGAE | estado + mes, sector agropecuario |
| INPC | mes, rubro alimentos (por ciudad, mapeada a estado) |
| SHCP | estado + mes, gasto y apoyos al campo |

## Pipeline en PySpark (resumen)
1. **Ingesta:** Excel a CSV, lectura con esquema explícito.
2. **Validación:** descartar valores que no sean D0 a D4 (el archivo trae números sueltos).
3. **Transformación:** pasar de formato ancho (437 columnas de fecha) a largo con `stack`; convertir D0 a D4 en número.
4. **Agregación:** municipio a estado y mes (% de municipios en sequía, severidad media).
5. **Integración:** `join` por estado y mes con IGAE, INPC y SHCP; rezagos con `Window` y `lag`.
