# ============================================================
# MÉTRICAS DE LA SPARK UI (src/monitoring.py)
# ============================================================
#
# Lee la API REST de la Spark UI (/api/v1), la misma información
# que muestran las pestañas Jobs, Stages y Executors. Cada etapa
# del pipeline se etiqueta con `etapa()` (setJobGroup) y
# `metricas()` suma lo que Spark hizo dentro de esa etiqueta.
# ============================================================

import json
import urllib.request
from pyspark.sql import SparkSession


def _rest(spark: SparkSession, ruta: str):
    sc = spark.sparkContext
    return json.load(urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}{ruta}"))


def etapa(spark: SparkSession, nombre: str) -> None:
    """Etiqueta los jobs siguientes; en la Spark UI aparecen con esta descripción."""
    spark.sparkContext.setJobGroup(nombre, nombre)


def metricas(spark: SparkSession, nombre: str) -> dict:
    jobs = [j for j in _rest(spark, "/jobs") if j.get("jobGroup") == nombre]
    ids = {s for j in jobs for s in j["stageIds"]}
    stages = [s for s in _rest(spark, "/stages") if s["stageId"] in ids and s["status"] == "COMPLETE"]
    suma = lambda campo: sum(s[campo] for s in stages)
    return {
        "etapa": nombre,
        "jobs": len(jobs),
        "stages": len(stages),
        "tareas": suma("numTasks"),
        "lectura_MB": round(suma("inputBytes") / 1e6, 2),
        "escritura_MB": round(suma("outputBytes") / 1e6, 2),
        "shuffle_KB": round(suma("shuffleWriteBytes") / 1e3, 1),
        "tiempo_tareas_s": round(suma("executorRunTime") / 1e3, 2),
    }


def entorno(spark: SparkSession) -> dict:
    sc = spark.sparkContext
    ex = _rest(spark, "/executors")
    return {
        "master": sc.master,
        "executors (procesos JVM)": len(ex),
        "slots de tarea (núcleos)": ex[0]["totalCores"],
        "memoria unificada Spark (MB)": round(ex[0]["maxMemory"] / 1e6),
        "spark.driver.memory": sc.getConf().get("spark.driver.memory", "1g (por defecto)"),
        "spark.sql.shuffle.partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "spark.sql.adaptive.enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        "spark.sql.files.maxPartitionBytes": spark.conf.get("spark.sql.files.maxPartitionBytes"),
        "Spark UI": sc.uiWebUrl,
    }
