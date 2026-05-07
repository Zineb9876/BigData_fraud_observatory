"""
Real-Time Fraud Observatory — Spark Streaming Processor
========================================================
Rôle : Consommer le topic Kafka 'transactions-stream' en micro-batches,
       appliquer des règles de détection avancées basées sur des fenêtres
       temporelles et des agrégations, puis stocker les résultats dans MongoDB.

Différence avec fraud_detector.py :
  - fraud_detector.py  → traite 1 transaction à la fois (latence ~0ms)
  - spark_processor.py → traite des fenêtres de 30s (détection de patterns)
"""

import json
import os
from datetime import datetime

import os
os.environ["JAVA_HOME"] = "C:/Program Files/Eclipse Adoptium/jdk-11.0.31.11-hotspot"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:/hadoop/bin"
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as spark_sum,
    avg, max as spark_max, when, lit, current_timestamp,
    collect_list, size, array_distinct
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType, TimestampType
)
from pymongo import MongoClient

# ─────────────────────────────────────────────
# 0. Configuration
# ─────────────────────────────────────────────
KAFKA_BROKER      = "localhost:9092"
KAFKA_TOPIC       = "transactions-stream"
MONGO_URI         = "mongodb://localhost:27017"
MONGO_DB          = "fraud_observatory"
MONGO_COLLECTION  = "spark_aggregations"
BATCH_DURATION    = "30 seconds"   # taille de la fenêtre temporelle
SLIDE_DURATION    = "10 seconds"   # glissement de la fenêtre

# Seuils de détection
SEUIL_MONTANT_ELEVE     = 3000.0   # MAD
SEUIL_TRANSACTIONS      = 5        # nb transactions / fenêtre
SEUIL_VILLES_DISTINCTES = 2        # nb villes différentes / fenêtre

# ─────────────────────────────────────────────
# 1. Schéma des transactions Kafka
# ─────────────────────────────────────────────
transaction_schema = StructType([
    StructField("transaction_id", StringType(),  True),
    StructField("user_id",        StringType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("currency",       StringType(),  True),
    StructField("merchant",       StringType(),  True),
    StructField("city",           StringType(),  True),
    StructField("timestamp",      StringType(),  True),
    StructField("device",         StringType(),  True),
    StructField("is_fraud",       BooleanType(), True),
    StructField("fraud_type",     StringType(),  True),
])

# ─────────────────────────────────────────────
# 2. Initialisation de la SparkSession
# ─────────────────────────────────────────────
def create_spark_session():
    print("Initialisation de la SparkSession...")

    spark = (
        SparkSession.builder
        .appName("FraudObservatory-SparkStreaming")
        .master("local[*]")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
        .config("spark.sql.streaming.checkpointLocation", "C:/Users/dell/fraud-observatory/checkpoint")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession créée avec succès !")
    return spark

# ─────────────────────────────────────────────
# 3. Lecture du flux Kafka
# ─────────────────────────────────────────────
def read_kafka_stream(spark):
    print(f"Connexion à Kafka : {KAFKA_BROKER} | Topic : {KAFKA_TOPIC}")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Désérialisation JSON
    parsed_stream = (
        raw_stream
        .select(
            from_json(col("value").cast("string"), transaction_schema)
            .alias("data"),
            col("timestamp").alias("kafka_timestamp")
        )
        .select("data.*", "kafka_timestamp")
    )

    return parsed_stream

# ─────────────────────────────────────────────
# 4. Règles de détection avancées (sur fenêtres)
# ─────────────────────────────────────────────
def apply_window_detection(stream):
    """
    Règles impossibles transaction par transaction :
    - Fréquence élevée sur 30s
    - Montant total suspect sur 30s
    - Présence dans plusieurs villes sur 30s
    """

    windowed = (
        stream
        .withWatermark("kafka_timestamp", "1 minute")
        .groupBy(
            col("user_id"),
            window(col("kafka_timestamp"), BATCH_DURATION, SLIDE_DURATION)
        )
        .agg(
            count("*")                          .alias("nb_transactions"),
            spark_sum("amount")                 .alias("montant_total"),
            avg("amount")                       .alias("montant_moyen"),
            spark_max("amount")                 .alias("montant_max"),
            collect_list("city")                .alias("villes"),
            collect_list("device")              .alias("devices"),
            spark_sum(when(col("is_fraud") == True, 1).otherwise(0))
                                                .alias("nb_fraudes_simulateur"),
        )
    )

    # Ajout des flags de détection Spark
    result = (
        windowed
        .withColumn("nb_villes_distinctes",
                    size(array_distinct(col("villes"))))
        .withColumn("flag_frequence_elevee",
                    col("nb_transactions") >= SEUIL_TRANSACTIONS)
        .withColumn("flag_montant_total_suspect",
                    col("montant_total") >= SEUIL_MONTANT_ELEVE)
        .withColumn("flag_multi_villes",
                    col("nb_villes_distinctes") >= SEUIL_VILLES_DISTINCTES)
        .withColumn("spark_is_fraud",
                    col("flag_frequence_elevee") |
                    col("flag_montant_total_suspect") |
                    col("flag_multi_villes"))
        .withColumn("window_start",
                    col("window.start").cast("string"))
        .withColumn("window_end",
                    col("window.end").cast("string"))
        .withColumn("processed_at",
                    current_timestamp().cast("string"))
        .drop("window")
    )

    return result

# ─────────────────────────────────────────────
# 5. Sauvegarde dans MongoDB
# ─────────────────────────────────────────────
def save_batch_to_mongo(batch_df, batch_id):
    """Appelée pour chaque micro-batch Spark."""
    rows = batch_df.collect()

    if not rows:
        return

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db     = client[MONGO_DB]
        col_agg   = db[MONGO_COLLECTION]
        col_alerts = db["spark_alerts"]

        docs    = []
        alerts  = []

        for row in rows:
            doc = {
                "batch_id":               batch_id,
                "user_id":                row["user_id"],
                "window_start":           row["window_start"],
                "window_end":             row["window_end"],
                "nb_transactions":        row["nb_transactions"],
                "montant_total":          round(float(row["montant_total"] or 0), 2),
                "montant_moyen":          round(float(row["montant_moyen"] or 0), 2),
                "montant_max":            round(float(row["montant_max"] or 0), 2),
                "nb_villes_distinctes":   row["nb_villes_distinctes"],
                "villes":                 list(row["villes"]),
                "flag_frequence_elevee":  bool(row["flag_frequence_elevee"]),
                "flag_montant_total":     bool(row["flag_montant_total_suspect"]),
                "flag_multi_villes":      bool(row["flag_multi_villes"]),
                "spark_is_fraud":         bool(row["spark_is_fraud"]),
                "nb_fraudes_simulateur":  int(row["nb_fraudes_simulateur"] or 0),
                "processed_at":          row["processed_at"],
            }
            docs.append(doc)

            # Générer une alerte si fraude détectée
            if row["spark_is_fraud"]:
                reasons = []
                if row["flag_frequence_elevee"]:
                    reasons.append(f"Frequence elevee ({row['nb_transactions']} tx/30s)")
                if row["flag_montant_total_suspect"]:
                    reasons.append(f"Montant total suspect ({round(float(row['montant_total'] or 0), 2)} MAD)")
                if row["flag_multi_villes"]:
                    reasons.append(f"Multi-villes ({row['nb_villes_distinctes']} villes)")

                alerts.append({
                    "source":       "spark_streaming",
                    "user_id":      row["user_id"],
                    "window_start": row["window_start"],
                    "window_end":   row["window_end"],
                    "reasons":      reasons,
                    "montant_total": round(float(row["montant_total"] or 0), 2),
                    "villes":       list(row["villes"]),
                    "timestamp":    datetime.now().isoformat(),
                })

                # Affichage console
                print(f"\n{'='*60}")
                print(f"[SPARK ALERTE] {row['user_id']}")
                print(f"  Fenêtre   : {row['window_start']} → {row['window_end']}")
                print(f"  Raisons   : {' | '.join(reasons)}")
                print(f"  Villes    : {list(row['villes'])}")
                print(f"  Montant   : {round(float(row['montant_total'] or 0), 2)} MAD")
                print(f"{'='*60}")
            else:
                print(f"[SPARK NORMAL] {row['user_id']} | "
                      f"{row['nb_transactions']} tx | "
                      f"{round(float(row['montant_total'] or 0), 2)} MAD | "
                      f"{row['nb_villes_distinctes']} ville(s)")

        if docs:
            col_agg.insert_many(docs)
        if alerts:
            col_alerts.insert_many(alerts)

        print(f"\n[Batch {batch_id}] {len(docs)} agrégats | {len(alerts)} alertes Spark → MongoDB")
        client.close()

    except Exception as e:
        print(f"[ERREUR MongoDB] Batch {batch_id} : {e}")

# ─────────────────────────────────────────────
# 6. Lancement du streaming
# ─────────────────────────────────────────────
def start_spark_streaming():
    print("=" * 60)
    print("  Real-Time Fraud Observatory — Spark Streaming")
    print("=" * 60)
    print(f"  Kafka Broker  : {KAFKA_BROKER}")
    print(f"  Topic         : {KAFKA_TOPIC}")
    print(f"  Fenêtre       : {BATCH_DURATION} / glissement {SLIDE_DURATION}")
    print(f"  MongoDB       : {MONGO_URI}/{MONGO_DB}")
    print("=" * 60)

    spark  = create_spark_session()
    stream = read_kafka_stream(spark)
    result = apply_window_detection(stream)

    query = (
        result.writeStream
        .outputMode("update")
        .foreachBatch(save_batch_to_mongo)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("\nSpark Streaming démarré — en attente de transactions...\n")
    query.awaitTermination()

# ─────────────────────────────────────────────
# 7. Point d'entrée
# ─────────────────────────────────────────────
if __name__ == "__main__":
    start_spark_streaming()
