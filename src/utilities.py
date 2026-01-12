# Databricks notebook source
import sys
import os
import time
import threading
import shutil
import gc
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd 
from loguru import logger
import fastexcel 
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, StringType, StructType, StructField, LongType
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
try:
    from databricks.sdk.runtime import dbutils
except ImportError:
    pass

spark = SparkSession.builder.getOrCreate()

# os.environ['TZ'] = 'America/Lima'
# time.tzset()

# print(datetime.now())
# print(f"Hora de ejecución (UTC): {datetime.now(tz = timezone.utc)}")

spark.conf.set("spark.sql.session.timeZone", "America/Lima")
spark.conf.set("spark.sql.shuffle.partitions", "16")

HORA_INICIAL, HORA_FINAL = datetime.now(), datetime.now()
PERIODO = HORA_INICIAL.date()

DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MM-yyyy", "yyyy/MM/dd", "yyyyMMdd", "dd/MM/yy"]
BASE_VOLUME_PATH = "/Volumes/landing_dev/sctr_emision/inputs_volumen"

BASE_NAME = None
LOG_NAME = None
LOCAL_LOG_PATH = None
REMOTE_LOG_PATH = None
_STOP_SYNC = False
_SYNC_THREAD = None

def custom_format_log(type_process: int):
    def formatter(record: dict):
        levelname = record['level'].name
        if levelname == 'INFO':
            text = 'AVISO'
            level_str = f'<cyan>{text:<7}</cyan>'
            message_color = '<cyan>'
        elif levelname == 'WARNING':
            text = 'ALERTA'
            level_str = f'<level>{text:<7}</level>'
            message_color = '<level>'
        elif levelname == 'SUCCESS':
            text = 'ÉXITO'
            level_str = f'<level>{text:<7}</level>'
            message_color = '<level>'
        else:
            level_str = f'<level>{levelname:<7}</level>'
            message_color = '<level>'
        
        original_message = str(record['message'])
        
        safe_message = (original_message
                .replace("{", "{{")
                .replace("}", "}}")
                .replace("<", "\\<")
                .replace(">", "\\>")
               )
        custom_message = f"{message_color}{safe_message}</{message_color.strip('<>')}>\n"
        
        if type_process == 0:
            level_str = f'{level_str} | '
        else:
            level_str = f"{level_str} | {record['name']}:{record['function']}:{record['line']} - "
            if record["exception"] is not None:
                custom_message += f"{record['exception']}\n"

        return (
            f"<cyan><bold>{record['time']:DD/MM/YYYY HH:mm:ss}</bold></cyan> | "
            f"{level_str}"
            f"{custom_message}"
        )
    return formatter

def _sync_worker():
    while not _STOP_SYNC:
        try:
            if LOCAL_LOG_PATH and os.path.exists(LOCAL_LOG_PATH):
                shutil.copy2(LOCAL_LOG_PATH, REMOTE_LOG_PATH)
        except Exception as e:
            pass
        time.sleep(5) 

def clear_local_log():
    global LOCAL_LOG_PATH,LOG_NAME
    try:
        if Path(LOCAL_LOG_PATH).exists():
            Path(LOCAL_LOG_PATH).unlink(missing_ok=True)
    except OSError as e:
        timestamp = int(time.time())
        LOG_NAME = f"{BASE_NAME}_{timestamp}.log"
        LOCAL_LOG_PATH = f"/tmp/{LOG_NAME}"
        print(f"⚠️ El log anterior estaba bloqueado. Usando nuevo nombre: {LOG_NAME}")

def open_log(layer_name : str):
    global LOG_NAME, LOCAL_LOG_PATH, REMOTE_LOG_PATH, _STOP_SYNC, _SYNC_THREAD

    logger.remove()

    _STOP_SYNC = True 
    if _SYNC_THREAD and _SYNC_THREAD.is_alive():
        _SYNC_THREAD.join(timeout=2)

    BASE_NAME = f"ETL_Emision_SCTR_{layer_name}_{PERIODO}"
    LOG_NAME = f"{BASE_NAME}.log"
    LOCAL_LOG_PATH = f"/tmp/{LOG_NAME}"
    REMOTE_LOG_PATH = f"{BASE_VOLUME_PATH}/Logs/{LOG_NAME}"

    if Path(REMOTE_LOG_PATH).exists():
        print(f"🔄 Historial detectado en Volumen. Restaurando '{LOG_NAME}' a local...")
        try:
            shutil.copy2(REMOTE_LOG_PATH, LOCAL_LOG_PATH)
        except Exception as e:
            print(f"⚠️ No se pudo restaurar log previo: {e}")
            clear_local_log()
    else:
        clear_local_log()

    REMOTE_LOG_PATH = f"{BASE_VOLUME_PATH}/Logs/{LOG_NAME}"

    logger.add(sys.stdout, 
            backtrace=False, diagnose=False, level='DEBUG',
            colorize=True,
            format=custom_format_log(0))
    
    logger.add(LOCAL_LOG_PATH, 
        backtrace=True, diagnose=True, level='DEBUG',
        format='{time:DD/MM/YYYY HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}',
        enqueue=True)

    logger.info(f"📝 Log local iniciado en: {LOCAL_LOG_PATH}")
    logger.info(f"🔄 Sincronización automática a: {REMOTE_LOG_PATH}")

    _STOP_SYNC = False
    _SYNC_THREAD = threading.Thread(target=_sync_worker, daemon=True)
    _SYNC_THREAD.start()

def close_log():
    global _STOP_SYNC
    
    logger.info("🏁 Finalizando log y forzando última sincronización...")
    logger.complete() # Asegurar que Loguru bajó todo a disco local
    
    # Detener hilo
    _STOP_SYNC = True
    
    # Última copia manual para asegurar que no falte nada
    try:
        if LOCAL_LOG_PATH and os.path.exists(LOCAL_LOG_PATH):
            shutil.copy2(LOCAL_LOG_PATH, REMOTE_LOG_PATH)
            print(f"✅ Log guardado exitosamente en: {REMOTE_LOG_PATH}")

            Path(LOCAL_LOG_PATH).unlink(missing_ok=True)
            print("🧹 Archivo temporal local eliminado.")
    except Exception as e:
        print(f"❌ Error guardando log final: {e}")

def validate_table_delta(table_name: str, show_message : bool = True) -> bool:
    # is_delta = DeltaTable.isDeltaTable(spark, table_name)
    is_exist = spark.catalog.tableExists(table_name)
    if show_message:
        if is_exist:
            logger.info(f"   ✅ La tabla Delta existe en {table_name}.")
        else:
            logger.info(f"   ❌ La tabla Delta NO existe en {table_name}.")
    return is_exist

def save_to_table_delta(df: DataFrame, table_name: str, mode="overwrite", mergeSchema="false") -> bool:
    try:
        (df.write.format("delta")
            .mode(mode)
            .option("mergeSchema", mergeSchema)
            .saveAsTable(table_name))
        
        return True
    except Exception as e:
        logger.error(f"   ❌ Error en Guardar Tabla Delta {table_name}: {e}")
        return False