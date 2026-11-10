# Guía de optimización

Documento de consulta para mejorar los pipelines del proyecto. Se actualizará con resultados y evidencias conforme avance la implementación.

## 1. Ciclo de optimización

| Etapa | Pregunta orientadora |
|---|---|
| 1. Definir objetivo | ¿Qué queremos mejorar y qué resultado esperamos? |
| 2. Medir | ¿Qué evidencia o métrica describe la situación inicial? |
| 3. Identificar | ¿Dónde está el cuello de botella o problema? |
| 4. Ajustar | ¿Qué cambio técnico podría ayudar? |
| 5. Validar | ¿Qué cambió y se conservaron los resultados correctos? |

## 2. Recomendaciones prácticas

| Práctica | Qué hacer | Cuándo conviene | Cómo verificarlo |
|---|---|---|---|
| Calidad de datos y esquemas | Definir el esquema explícito y revisar nulos y duplicados antes de transformar | Al ingerir datos nuevos | Conteos antes y después en `src/quality`; tipos correctos en `printSchema()` |
| Selección de columnas | Leer solo las columnas necesarias con `select` | Archivos Parquet con muchas columnas | `ReadSchema` en `explain()`; el resultado no cambia |
| Filtros tempranos | Aplicar `filter` antes de joins y agregaciones | Cuando el filtro reduce muchas filas | `PushedFilters` en el Scan y menor volumen en los `Exchange`; mismo resultado |
| Joins | Evaluar broadcast cuando una tabla es pequeña | Una tabla chica unida con una grande | El plan muestra `BroadcastHashJoin` en lugar de `SortMergeJoin` |
| Shuffle y particionamiento | Evitar `repartition` innecesarios y ajustar `spark.sql.shuffle.partitions` | Pocas filas o muchas particiones vacías | Spark UI, pestaña Stages: Shuffle Read/Write y número de tareas |
| Caché | Usar `persist` solo en DataFrames reutilizados en varias acciones y liberar con `unpersist` | Un mismo DataFrame alimenta varias acciones | Spark UI, pestaña Storage: aparece al persistir y desaparece al liberar |
| AQE | Mantener activo `spark.sql.adaptive.enabled` | Cargas con tamaños de datos variables | `AdaptiveSparkPlan` en `explain()` |
| Monitoreo | Revisar Spark UI después de cada cambio importante | Siempre que se optimiza | Jobs, Stages y SQL comparados antes y después |
