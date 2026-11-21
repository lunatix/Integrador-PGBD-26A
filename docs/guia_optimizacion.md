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

## 3. Resultados medidos en este proyecto

Mediciones con los datos reales del proyecto (CSV de SHCP de 225 MB, 746,676 filas), en una máquina de 12 núcleos y 16 GB de RAM, con Spark 3.5.9 en `local[*]` y Java 17. Las métricas provienen de la API REST de la Spark UI. Los tiempos son la mediana de 3 corridas después de una de calentamiento. El plan físico y las métricas de cada capa se ven en vivo en `notebooks/01_ingesta.ipynb`, y cada ejecución de `python -m src.pipeline` las registra en `reports/evidencia_ejecucion_act11.txt`.

### 3.1 Lecturas del CSV: sin caché, con caché y medallion

Las compuertas de calidad lanzan decenas de acciones (`count`, `agg`, `first`). Spark es perezoso: sin datos materializados, cada acción vuelve a leer el CSV.

| Versión del pipeline | MB leídos de disco | Jobs | Tareas | Tiempo de pared (mediana) |
| :--- | ---: | ---: | ---: | ---: |
| Sin capas ni caché | 2,730 (≈ 12 lecturas del CSV) | 103 | 245 | 14.7 s |
| Sin capas, con `persist()` de tablas limpias y maestro | 457 (2 lecturas) | 84 | 140 | 8.2 s |
| **Medallion** (Bronze → Silver → Gold en Parquet) | **236 (1 lectura)** | 121 | 164 | ~28 s de proceso, incluido el arranque de la JVM¹ |

¹ El medallion escribe tres capas a disco y Bronze interpreta las 16 columnas del CSV (la versión anterior solo leía 6). A cambio, Silver y Gold se reprocesan desde Parquet sin tocar el CSV: Silver lee 6.4 MB y Gold 0.5 MB. **Decisión:** medallion sin `persist()`, porque la materialización en Parquet sustituye a la caché.

### 3.2 Particiones de lectura (`spark.sql.files.maxPartitionBytes`)

Regla de Spark: `maxSplitBytes = min(maxPartitionBytes, max(openCostInBytes, bytes / defaultParallelism))`. Con 12 núcleos, el CSV se divide en *splits* de ~19 MB, uno por núcleo.

| maxPartitionBytes | Particiones | Lectura completa (mediana) |
| :--- | ---: | ---: |
| 256 MB | 12 | 0.27 s |
| **128 MB (por defecto)** | **12** | **0.27 s** |
| 32 MB | 12 | 0.26 s |
| 8 MB | 27 | 0.33 s |
| 2 MB | 108 | 0.33 s |

**Decisión:** valor por defecto. Ya produce una tarea por núcleo en una sola ola; *splits* más pequeños son ~22 % más lentos por el costo fijo de cada tarea.

### 3.3 Particiones de shuffle y AQE

Pipeline completo, variando `spark.sql.shuffle.partitions` con AQE encendido y apagado. Cada shuffle mueve como máximo ~330 KB.

| AQE | shuffle.partitions | Tareas | Mediana (s) | Mín–máx (s) |
| :--- | ---: | ---: | ---: | :--- |
| on | 1 | 104 | 8.03 | 7.36–8.78 |
| on | **4** | 140 | **7.07** | 6.41–8.10 |
| on | 12 | 204 | 8.65 | 8.00–8.79 |
| on | 200 (por defecto) | 1,708 | 9.91 | 9.03–10.76 |
| off | 1 | 99 | 6.58 | 6.46–6.94 |
| off | 4 | 164 | 7.41 | 7.08–8.02 |
| off | 12 | 308 | 7.02 | 6.83–8.04 |
| off | 200 | 3,692 | 11.76 | 11.56–11.88 |

**Decisión:** `spark.sql.shuffle.partitions = 4` con AQE activado.
- El valor por defecto (200) es entre 40 % y 70 % más lento: crea miles de tareas casi vacías.
- Entre 1 y 12 particiones los rangos se traslapan; la diferencia es ruido.
- AQE se mantiene porque fusiona particiones pequeñas y protege si el volumen crece. En la Spark UI se ve en Silver y Gold: `AQEShuffleRead` reduce las 4 particiones de shuffle a 1 cuando el volumen es de KB (`reports/spark_ui/05_silver_plan_3_aqe_coalesce_escritura.jpg`).

### 3.4 Joins: `broadcast()` frente a `SortMergeJoin`

| Estrategia | Joins en el plan | Exchanges | Integración (mediana) |
| :--- | :--- | ---: | ---: |
| `broadcast()` (actual) | 3 × BroadcastHashJoin | 7 | 1.06 s |
| Sin broadcast (`autoBroadcastJoinThreshold = -1`) | 3 × SortMergeJoin | 6 | 0.76 s |

**Decisión:** se conserva `broadcast()`.
- Con 123 filas por lado no hay ganancia medible.
- Spark lo elegiría de todos modos, porque cada tabla pesa menos de 40 KB y el umbral es de 10 MB.
- El hint deja explícita la intención para cuando crezca el lado de SHCP (más claves o desglose por entidad), donde evita repartir por red la tabla grande.

### 3.5 Memoria del driver

| Indicador | Valor |
| :--- | ---: |
| Memoria unificada de Spark (ejecución + caché) | 456 MB |
| Pico de memoria de ejecución / de caché | 28 MB / 28 MB |
| GC como % del tiempo de tareas | 1.9 % |
| Bytes derramados a disco (*spill*) | 0 |

**Decisión:** `spark.driver.memory` en su valor por defecto (1 GB). Convendría aumentarla si el GC superara el 10 % o apareciera *spill*.

### 3.6 Análisis (notebook 02)

En el análisis, la carga de Gold hacía `.orderBy('fecha')`. Eso agregaba un `Exchange` redundante, porque la ventana de `lag(12)` vuelve a ordenar. **Al quitarlo, el análisis pasó de 123 a 77 jobs y de 603 a 327 tareas**, con resultados idénticos.

### 3.7 Resumen de decisiones

| Decisión | Valor | Evidencia |
| :--- | :--- | :--- |
| Modo de ejecución / instancias | `local[*]`: 1 JVM (driver = executor), 12 slots | ~226 MB de insumo; un cluster solo agregaría red y serialización |
| Arquitectura | Medallion: Bronze / Silver / Gold en Parquet | El CSV se lee 1 vez; Bronze es ≈ 50 veces más pequeño que el CSV |
| Esquemas explícitos | `StructType` en SHCP e INPC | Evita la pasada de `inferSchema`; Silver lee 6 de 18 columnas con filtro empujado a Parquet |
| Particiones de lectura | 12 (por defecto) | Una por núcleo; *splits* más pequeños son ~22 % más lentos (§3.2) |
| Particiones de shuffle | 4, con AQE | 200 es entre 40 % y 70 % más lento; de 1 a 12 es equivalente (§3.3) |
| Joins | `broadcast()` explícito | Sin costo relevante; Spark lo elegiría solo (§3.4) |
| Ventana sin `partitionBy` | `Window.orderBy("fecha")` | `lag(12)` necesita la serie completa ordenada; 123 filas |
| Memoria | 1 GB (por defecto) | GC de 1.9 %, sin *spill* (§3.5) |
| Observabilidad | *event log* en `spark-events/` | Permite reabrir cualquier ejecución en el History Server |

> Las tablas 3.2 a 3.5 se midieron antes de introducir las capas medallion, con el mismo CSV y la misma configuración. El volumen que procesan las lecturas, los shuffles y los joins es el mismo en ambas versiones.
