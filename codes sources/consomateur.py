from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lower, regexp_replace, udf
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, DoubleType, BooleanType
)
from textblob import TextBlob
import joblib
import nltk
import warnings
warnings.filterwarnings('ignore')

#Télécharger ressources NLTK
nltk.download('punkt',     quiet=True)
nltk.download('stopwords', quiet=True)
nltk.download('wordnet',   quiet=True)
nltk.download('punkt_tab', quiet=True)

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

#Charger le modèle IA dejà entraîné et les encoders
print("Chargement du modèle IA:")
modele      = joblib.load('/home/khaoula/projet_bigdata/meilleur_modele.pkl')
le_category = joblib.load('/home/khaoula/projet_bigdata/le_category.pkl')
le_source   = joblib.load('/home/khaoula/projet_bigdata/le_source.pkl')
le_page     = joblib.load('/home/khaoula/projet_bigdata/le_page.pkl')
le_sentiment= joblib.load('/home/khaoula/projet_bigdata/le_sentiment.pkl')
le_profil   = joblib.load('/home/khaoula/projet_bigdata/le_profil.pkl')
features    = joblib.load('/home/khaoula/projet_bigdata/features.pkl')
print(f"Modèle chargé — {len(features)} features")

# NLP 
stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

positive_words = ['great', 'excellent', 'good', 'happy', 'satisfied',
                  'awesome', 'love', 'perfect', 'recommended', 'pleased',
                  'impressed', 'wonderful', 'fantastic', 'amazing']
negative_words = ['poor', 'bad', 'disappointed', 'unhappy', 'terrible',
                  'worst', 'dissatisfied', 'waste', 'awful', 'horrible',
                  'useless', 'broken', 'cheap', 'slow', 'expensive']

def analyser_sentiment_udf(text):
    try:
        score = TextBlob(str(text)).sentiment.polarity
        if score > 0.2:  return "positif"
        if score < -0.2: return "negatif"
        return "neutre"
    except:
        return "neutre"

def sentiment_score_udf(text):
    try:
        return round(TextBlob(str(text)).sentiment.polarity, 3)
    except:
        return 0.0

def subjectivite_udf(text):
    try:
        return round(TextBlob(str(text)).sentiment.subjectivity, 3)
    except:
        return 0.0

def nb_tokens_udf(text):
    try:
        tokens = word_tokenize(str(text).lower())
        tokens = [t for t in tokens if t.isalpha() and t not in stop_words]
        return len(tokens)
    except:
        return 0

def mots_positifs_udf(text):
    try:
        tokens = word_tokenize(str(text).lower())
        return sum(1 for t in tokens if t in positive_words)
    except:
        return 0

def mots_negatifs_udf(text):
    try:
        tokens = word_tokenize(str(text).lower())
        return sum(1 for t in tokens if t in negative_words)
    except:
        return 0

def encode_safe(encoder, value, default=0):
    try:
        classes = list(encoder.classes_)
        if value in classes:
            return int(encoder.transform([value])[0])
        return default
    except:
        return default

def predire_intention(row):
    """
    Prédit l'intention d'achat et génère une recommandation
    intelligente basée sur le modèle IA
    """
    try:
        # NLP
        text           = str(row.get('review_clean', ''))
        sentiment      = analyser_sentiment_udf(text)
        sent_score     = sentiment_score_udf(text)
        subj           = subjectivite_udf(text)
        nb_tok         = nb_tokens_udf(text)
        nb_pos         = mots_positifs_udf(text)
        nb_neg         = mots_negatifs_udf(text)
        score_nlp      = nb_pos - nb_neg

        # Profil comportemental
        taux     = 0.2
        abandons = 0
        achats   = 1 if row.get('action') == 'purchase' else 0
        actions  = 1
        score_eng= row.get('action_score', 0)

        if taux > 0.4 and score_eng > 10:
            profil = "client_fidele"
        elif abandons > achats and sentiment == 'negatif':
            profil = "chasseur_promos"
        elif achats > 0 and actions < 3:
            profil = "acheteur_impulsif"
        elif actions > 10 and taux < 0.1:
            profil = "comparateur"
        else:
            profil = "visiteur_standard"

        # Encodage
        cat_enc  = encode_safe(le_category, str(row.get('category', '')))
        src_enc  = encode_safe(le_source,   str(row.get('source', '')))
        pag_enc  = encode_safe(le_page,     str(row.get('page_visitee', '')))
        sent_enc = encode_safe(le_sentiment, sentiment)
        prof_enc = encode_safe(le_profil,   profil)

        # Vecteur de features
        X = [[
            float(row.get('price', 0)),
            float(row.get('rating', 0)),
            float(row.get('duree_visite', 0)),
            cat_enc, src_enc, pag_enc,
            sent_enc, sent_score, subj,
            nb_tok, nb_pos, nb_neg, score_nlp,
            prof_enc,
            0, 0, 0, 0, 0, 0,  
            0, 0, 0, 0, 0, 0,
            1 if (row.get('action') == 'abandon' and sentiment == 'negatif') else 0
        ]]

        # Ajuster la taille si nécessaire
        while len(X[0]) < len(features):
            X[0].append(0)
        X[0] = X[0][:len(features)]

        
        prediction   = int(modele.predict(X)[0])
        proba        = modele.predict_proba(X)[0]
        proba_achat  = round(float(proba[1]) * 100, 1)

        return prediction, proba_achat, sentiment, profil

    except Exception as e:
        return 0, 0.0, "neutre", "visiteur_standard"

#shemas
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
    StructField("session_id",       StringType(), True),
    StructField("user_id",          IntegerType(),True),
    StructField("product_id",       IntegerType(),True),
    StructField("product_title",    StringType(), True),
    StructField("category",         StringType(), True),
    StructField("montant_paye",     FloatType(),  True),
    StructField("methode_paiement", StringType(), True),
    StructField("statut_paiement",  StringType(), True),
    StructField("timestamp",        DoubleType(), True),
])

SUPPORT_SCHEMA = StructType([
    StructField("session_id",    StringType(),  True),
    StructField("user_id",       IntegerType(), True),
    StructField("motif_contact", StringType(),  True),
    StructField("canal",         StringType(),  True),
    StructField("resolu",        BooleanType(), True),
    StructField("timestamp",     DoubleType(),  True),
])

#spark
spark = SparkSession.builder \
    .appName("Clickstream_Consumer_IA") \
    .master("local[2]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# kafka
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
    .select(from_json(col("value").cast("string"),
            CLICKSTREAM_SCHEMA).alias("data")) \
    .select("data.*") \
    .withColumn("review_clean",
        lower(regexp_replace(col("review_text"), r"[^a-zA-Z\s]", ""))) \
    .withColumn("action_score",
        when(col("action") == "purchase",     5)
        .when(col("action") == "add_to_cart", 3)
        .when(col("action") == "view",        1)
        .when(col("action") == "search",      1)
        .when(col("action") == "abandon",    -1)
        .otherwise(0))

transactions = read_kafka("transactions") \
    .select(from_json(col("value").cast("string"),
            TRANSACTION_SCHEMA).alias("data")) \
    .select("data.*")

support = read_kafka("service_client") \
    .select(from_json(col("value").cast("string"),
            SUPPORT_SCHEMA).alias("data")) \
    .select("data.*")

# Traitement avec IA par batch
def process_clicks_batch(df, epoch_id):
    if df.count() == 0:
        return

    rows = df.collect()
    print(f"\n{'='*65}")
    print(f"  BATCH {epoch_id} — {len(rows)} événements traités")
    print(f"{'='*65}")

    for row in rows[:5]:  
        row_dict = row.asDict()

        # Prédiction IA + NLP
        prediction, proba, sentiment, profil = predire_intention(row_dict)

        # Recommandation
        if prediction == 1:
            if proba > 80:
                recommandation = f" ACHAT PROBABLE ({proba}%) — Suggérer produits similaires"
            else:
                recommandation = f" ACHAT POSSIBLE ({proba}%) — Proposer livraison gratuite"
        else:
            if sentiment == 'negatif':
                recommandation = f"  RISQUE ABANDON ({100-proba}%) — Envoyer coupon -20%"
            else:
                recommandation = f"  NAVIGATION ({100-proba}%) — Continuer navigation"

        print(f"\n  👤 user_{row_dict.get('user_id')} | "
              f"session_{row_dict.get('session_id', '')[:8]}")
        print(f"   {row_dict.get('product_title', '')[:40]}")
        print(f"   Action    : {row_dict.get('action')}")
        print(f"   Sentiment : {sentiment}")
        print(f"   Profil    : {profil}")
        print(f"   {recommandation}")

def process_tx_batch(df, epoch_id):
    if df.count() == 0:
        return
    rows = df.collect()
    print(f"\n TRANSACTIONS — {len(rows)} paiements")
    for row in rows[:3]:
        r = row.asDict()
        print(f"   user_{r.get('user_id')} | "
              f"{r.get('statut_paiement')} | "
              f"{r.get('montant_paye', 0):.2f} MAD")

def process_support_batch(df, epoch_id):
    if df.count() == 0:
        return
    rows = df.collect()
    print(f"\n SUPPORT — {len(rows)} tickets")
    for row in rows[:3]:
        r = row.asDict()
        print(f"   user_{r.get('user_id')} | "
              f"{r.get('motif_contact')} | "
              f"résolu: {r.get('resolu')}")

#Streams
query_clicks = clicks.writeStream \
    .foreachBatch(process_clicks_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_consumer_clicks") \
    .trigger(processingTime="10 seconds") \
    .queryName("clicks_ia") \
    .start()

query_tx = transactions.writeStream \
    .foreachBatch(process_tx_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_consumer_tx") \
    .trigger(processingTime="10 seconds") \
    .queryName("transactions_ia") \
    .start()

query_support = support.writeStream \
    .foreachBatch(process_support_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_consumer_support") \
    .trigger(processingTime="10 seconds") \
    .queryName("support_ia") \
    .start()

print("\n Consumer IA démarré !")
print("   NLP + Modèle IA + Recommandations intelligentes\n")

spark.streams.awaitAnyTermination()