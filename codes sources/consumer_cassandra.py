from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lower, regexp_replace
from pyspark.sql.types import *
from cassandra.cluster import Cluster
import time

#Schémas
CLICKSTREAM_SCHEMA = StructType([
    StructField("session_id",    StringType(),  True),
    StructField("user_id",       IntegerType(), True),
    StructField("product_id",    IntegerType(), True),
    StructField("product_title", StringType(),  True),
    StructField("category",      StringType(),  True),
    StructField("price",         FloatType(),   True),
    StructField("rating",        FloatType(),   True),
    StructField("action",        StringType(),  True),
    StructField("page_visitee",  StringType(),  True),
    StructField("duree_visite",  IntegerType(), True),
    StructField("source",        StringType(),  True),
    StructField("review_text",   StringType(),  True),
    StructField("timestamp",     DoubleType(),  True),
])

TRANSACTION_SCHEMA = StructType([
    StructField("session_id",       StringType(),  True),
    StructField("user_id",          IntegerType(), True),
    StructField("product_id",       IntegerType(), True),
    StructField("product_title",    StringType(),  True),
    StructField("category",         StringType(),  True),
    StructField("montant_paye",     FloatType(),   True),
    StructField("methode_paiement", StringType(),  True),
    StructField("statut_paiement",  StringType(),  True),
    StructField("timestamp",        DoubleType(),  True),
])

SUPPORT_SCHEMA = StructType([
    StructField("session_id",    StringType(),  True),
    StructField("user_id",       IntegerType(), True),
    StructField("motif_contact", StringType(),  True),
    StructField("canal",         StringType(),  True),
    StructField("resolu",        BooleanType(), True),
    StructField("timestamp",     DoubleType(),  True),
])

#Fonctions sauvegarde 
def save_clicks_to_cassandra(df, epoch_id):
    if df.count() == 0:
        return
    start = time.time()
    cluster = Cluster(['172.18.0.2'], port=9042, connect_timeout=30)
    session = cluster.connect('bigdata_projet')
    session.default_timeout = 60
    rows = df.collect()
    for row in rows:
        session.execute("""
            INSERT INTO clickstream (
                session_id, user_id, product_id, product_title,
                category, price, rating, action, page_visitee,
                duree_visite, source, review_text, review_clean,
                action_score, recommandation, timestamp
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row.session_id, row.user_id, row.product_id, row.product_title,
            row.category, row.price, row.rating, row.action, row.page_visitee,
            row.duree_visite, row.source, row.review_text, row.review_clean,
            row.action_score, row.recommandation, row.timestamp
        ))
    elapsed = time.time() - start
    print(f"[Cassandra]  {len(rows)} clicks | temps: {elapsed:.2f}s")
    cluster.shutdown()

def save_transactions_to_cassandra(df, epoch_id):
    if df.count() == 0:
        return
    start = time.time()
    cluster = Cluster(['172.18.0.2'], port=9042, connect_timeout=30)
    session = cluster.connect('bigdata_projet')
    session.default_timeout = 60
    rows = df.collect()
    for row in rows:
        session.execute("""
            INSERT INTO transactions (
                session_id, user_id, product_id, product_title,
                category, montant_paye, methode_paiement,
                statut_paiement, timestamp
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row.session_id, row.user_id, row.product_id, row.product_title,
            row.category, row.montant_paye, row.methode_paiement,
            row.statut_paiement, row.timestamp
        ))
    elapsed = time.time() - start
    print(f"[Cassandra]  {len(rows)} transactions | temps: {elapsed:.2f}s")
    cluster.shutdown()

def save_support_to_cassandra(df, epoch_id):
    if df.count() == 0:
        return
    start = time.time()
    cluster = Cluster(['172.18.0.2'], port=9042, connect_timeout=30)
    session = cluster.connect('bigdata_projet')
    session.default_timeout = 60
    rows = df.collect()
    for row in rows:
        session.execute("""
            INSERT INTO service_client (
                session_id, user_id, motif_contact,
                canal, resolu, timestamp
            ) VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            row.session_id, row.user_id, row.motif_contact,
            row.canal, row.resolu, row.timestamp
        ))
    elapsed = time.time() - start
    print(f"[Cassandra]  {len(rows)} tickets | temps: {elapsed:.2f}s")
    cluster.shutdown()
#spark
spark = SparkSession.builder \
    .appName("Consumer_Cassandra") \
    .master("local[2]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#kafka
def read_kafka(topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

#Parser les données
clicks = read_kafka("clickstream") \
    .select(from_json(col("value").cast("string"), CLICKSTREAM_SCHEMA).alias("data")) \
    .select("data.*") \
    .withColumn("review_clean",
        lower(regexp_replace(col("review_text"), "[^a-zA-Z\\s]", ""))) \
    .withColumn("action_score",
        when(col("action") == "purchase",     5)
        .when(col("action") == "add_to_cart", 3)
        .when(col("action") == "view",        1)
        .when(col("action") == "search",      1)
        .when(col("action") == "abandon",    -1)
        .otherwise(0)) \
    .withColumn("recommandation",
        when(col("action") == "abandon",      "Envoyer coupon -20%")
        .when(col("action") == "add_to_cart", "Proposer livraison gratuite")
        .when(col("action") == "purchase",    "Suggerer produits similaires")
        .otherwise("Continuer navigation"))

transactions = read_kafka("transactions") \
    .select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data")) \
    .select("data.*")

support = read_kafka("service_client") \
    .select(from_json(col("value").cast("string"), SUPPORT_SCHEMA).alias("data")) \
    .select("data.*")

#Écrire dans Cassandra 
query_clicks = clicks.writeStream \
    .foreachBatch(save_clicks_to_cassandra) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_cass_clicks") \
    .trigger(processingTime="10 seconds") \
    .queryName("clicks_cassandra") \
    .start()

query_tx = transactions.writeStream \
    .foreachBatch(save_transactions_to_cassandra) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_cass_tx") \
    .trigger(processingTime="10 seconds") \
    .queryName("transactions_cassandra") \
    .start()

query_support = support.writeStream \
    .foreachBatch(save_support_to_cassandra) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_cass_support") \
    .trigger(processingTime="10 seconds") \
    .queryName("support_cassandra") \
    .start()

print("\nConsumer Cassandra démarré !")
print("Sauvegarde : clickstream | transactions | service_client\n")

spark.streams.awaitAnyTermination()
