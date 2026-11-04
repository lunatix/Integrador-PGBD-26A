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

