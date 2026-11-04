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
| Periodo 2015–2025 | La ventana común real es 2011–2026, así que el periodo cabe completo. |
| Formato de almacenamiento: Parquet | Es columnar y comprimido, y se integra nativamente con Spark. |
| No versionar datos | Lo pide la actividad; el origen de cada archivo queda documentado en `fuentes.md`. |

## Bitácora de dificultades
- Sesión 1: los primeros cambios se hicieron con commits directos a `main` desde la interfaz web, sin PR. Desde esta rama, todo cambio entra por PR revisado.
