# Organización del equipo

## Roles iniciales (sesión 1)

| Rol | Integrante(s) | Responsabilidades |
| :--- | :--- | :--- |
| Developer | Ruesga Rodriguez Gloria Viridiana; Díaz Aguila Mauricio Adrián | Implementar notebooks y scripts PySpark, abrir Pull Requests con cambios pequeños. |
| Technical Reviewer | Romero Gálvez Roberto Alejandro | Revisar y aprobar PRs, validar resultados (conteos, rangos de fechas, nulos), verificar que no se suban datos ni credenciales. |
| Documentation Lead | Dimas Salgado Aline Yaremys | Mantener README y `docs/`, actualizar diagramas, registrar decisiones. |

**Rotación:** los roles rotan en cada sesión. Todos participan en las decisiones y hacen al menos un commit por sesión.

## Flujo de trabajo en GitHub
1. Rama por contribución a partir de `main`: `tipo/descripcion` (por ejemplo, `docs/fuentes`, `feat/ingesta-sat`).
2. Commit y push de la rama, y apertura de un Pull Request hacia `main`.
3. Otro integrante revisa y aprueba. Nadie integra su propio PR.
4. Merge a `main` y borrado de la rama.

## Decisiones iniciales
| Decisión | Justificación |
| :--- | :--- |
| Fuente adicional: SAT (recaudación) | Mide directamente la variable de la pregunta y su viabilidad está verificada (ver `fuentes.md`). |
| Llave de integración: `fecha` mensual | Las cuatro fuentes son mensuales. |
| Periodo 2015-01 a 2025-03 | El IGAE de datos abiertos (INEGI) solo llega a 2025-03; las demás fuentes llegan a 2026. Se inicia en 2015 para excluir el cambio de régimen fiscal de la Reforma Hacendaria de 2014 (ver `arquitectura.md`). |
| Formato de almacenamiento: Parquet | Es columnar y comprimido, y se integra nativamente con Spark. |
| No versionar datos | Lo pide la actividad; el origen de cada archivo queda documentado en `fuentes.md`. |

## Decisiones de la entrega final
| Decisión | Justificación |
| :--- | :--- |
| Solo las cuatro fuentes (SHCP, IGAE, INPC y SAT) | Se descartó agregar controles externos (tipo de cambio, tasas, petróleo) para no ampliar el alcance. Queda como limitación del modelo de inflación. |
| Ventana desde 2015 | Los datos existen desde 2011-01 (límite de SHCP), pero la Reforma Hacendaria de 2014 cambió el régimen de recaudación (IEPS negativo en 2011–2013; +31 % de recaudación en 2015). Desde 2015 todas las variaciones anuales comparan meses del mismo régimen. |
| Arquitectura medallion (Bronze / Silver / Gold) | Cada capa queda materializada en Parquet: el CSV de SHCP (225 MB) se lee una sola vez y Silver y Gold se reprocesan desde Parquet. |
| Spark explicado dentro de los notebooks | El plan físico y las métricas de la Spark UI se muestran en la capa o sección donde se procesan los datos (`01_ingesta`, `02_analisis`), no en un archivo aparte. Cada ejecución del pipeline registra sus métricas en `reports/`. |
| Configuración de Spark medida | `local[*]` (1 JVM, 12 núcleos), 4 particiones de shuffle, AQE, broadcast y 1 GB de driver, justificados con mediciones en `guia_optimizacion.md`. |
| Gasto total (XBC) como indicador | Es la clave usada desde el inicio; con gasto programable (XBC10) las conclusiones no cambian (prueba de robustez en el notebook 02). |
| Valores p y errores robustos en el análisis | Con 105 meses, correlaciones menores a ±0.19 no son significativas; la autocorrelación (Durbin-Watson ≈ 0.2) exige errores HAC. Varias conclusiones preliminares se corrigieron por esta razón. |

## Bitácora de dificultades
- Sesión 1: los primeros cambios se hicieron con commits directos a `main` desde la interfaz web, sin PR. Desde esta rama, todo cambio entra por PR revisado.
- Entrega final: Spark 3.5 no arranca con Java 21 o superior (`getSubject is not supported`); se documentó el requisito de Java 17 en el README.
- Entrega final: el CSV del INPC que se tenía localmente había sido guardado de nuevo con otro formato de fecha (`yyyy-MM-dd` en lugar del oficial `dd/MM/yyyy`) y la capa Silver quedaba vacía. Se reemplazó por la descarga oficial; los valores en la ventana eran idénticos. Los archivos de `data/raw/` no deben abrirse ni guardarse con Excel.
