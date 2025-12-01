import yaml
import sys
import os

if os.name == 'nt':
    # Actualizar de acuerdo a donde se coloco winutils y hadoop.dll
    HADOOP_HOME_PATH = r"C:\hadoop" 
    
    os.environ['HADOOP_HOME'] = HADOOP_HOME_PATH
    os.environ['hadoop.home.dir'] = HADOOP_HOME_PATH
    
    bin_path = os.path.join(HADOOP_HOME_PATH, "bin")
    os.environ['PATH'] = bin_path + ";" + os.environ['PATH']
    
    print(f"DEBUG: HADOOP_HOME configurado en: {os.environ['HADOOP_HOME']}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, collect_set, first, lit, concat_ws, row_number, desc
from pyspark.sql.window import Window

# Configuracion de Logs
def get_logger(spark):
    log4jLogger = spark._jvm.org.apache.log4j
    return log4jLogger.LogManager.getLogger(__name__)
    
def load_config():
    with open("config/etl_config.yaml","r") as f:
        return yaml.safe_load(f)
        
def create_spark_session():
    warehouse_path = os.path.join(os.getcwd(), "spark-warehouse")
    return SparkSession.builder \
        .appName("KashioChallengeMedallion") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", f"file:///{warehouse_path}") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.hadoop.winutils.exe.libs.ignore", "true") \
        .getOrCreate()
    
def run_pipeline():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR") 
    logger = get_logger(spark)
    config = load_config()
    
    logger.info("Inicializando Pipeline con Medallion architecture")
    
    try:
        # BRONZE: Ingesta Raw Data
        logger.info("Leyendo Bronze")
        
        # Lectura JSON
        df_events = spark.read.json(config["data_paths"]["raw_events"])
        
        # Lectura csv para transacciones y usuarios
        df_transactions = spark.read.option("header",True).csv(config["data_paths"]["raw_transactions"])
        df_users = spark.read.option("header",True).csv(config["data_paths"]["raw_users"])
        
        # SILVER: cleaning & standarization
        logger.info("Procesando capa Silver")
        
        # Events cleaning (duplicated and casting)
        df_events_casted = df_events.withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))

        window_spec = Window.partitionBy("event_id").orderBy(col("event_timestamp").desc())

        df_events_silver = df_events_casted.withColumn("row_number",row_number().over(window_spec)).filter(col("row_number")==1).drop("row_number")
        
        # Transactions cleaning
        df_transactions_silver = df_transactions.withColumn("amount",col("amount").cast("double")).withColumn("transaction_timestamp",col("transaction_timestamp").cast("timestamp"))
        
        # Guardamos los resultados en Silver, en caso de AWS seria en S3 en un bucket usado para Silver
        df_events_silver.write.mode("overwrite").parquet(config["data_paths"]["silver_output"] + "events")
        
        df_transactions_silver.write.mode("overwrite").parquet(config["data_paths"]["silver_output"] + "transactions")
        
        logger.info("Capa Silver en formato Parquet")
        
        # GOLD: Aggregation & join
        logger.info("Generando Golden")
        
        # Eventos por sesion
        df_session_metrics = df_events_silver.groupBy("session_id","user_id").agg(
            first("event_timestamp").alias("session_start"),
            count("event_id").alias("total_events"),
            collect_set("event_type").alias("event_type_list")
        )
        
        # Join con Users - Dimensional
        df_session_metrics_users = df_session_metrics.join(df_users,"user_id","left").select(
            df_session_metrics["*"],
            df_users["country"],
            df_users["device_type"]
        )
        
        # Join con Transactions - Final
        df_gold_final = df_session_metrics_users.join(df_transactions_silver,["session_id","user_id"],"left").select(
            col("session_id"),
            col("user_id"),
            col("country").alias("user_country"),
            col("device_type").alias("user_device"),
            col("session_start").alias("session_start_time"),
            col("total_events"),
            concat_ws(",",col("event_type_list")).alias("event_type"),
            col("transaction_id"),
            col("amount"),
            col("currency")
        ).withColumn("is_conversion",col("transaction_id").isNotNull())
        
        # OUTPUT: En producción esto iria en Redshift, para fines practicos se va guardar en CSV
        output_path = config["data_paths"]["gold_output"] + "user_session_analysis"
        df_gold_final.write.mode("overwrite").option("header",True).csv(output_path)
        
        logger.info(f"Pipeline exitoso. Data guardada en: {output_path}")
        df_gold_final.show(5, truncate=False)
        
    except Exception as e:
        logger.error(f"Error critico en el pipeline: {str(e)}")
        print(f"DEBUG ERROR: {e}")
        sys.exit(1)
        
if __name__ == "__main__":
    run_pipeline()
       
