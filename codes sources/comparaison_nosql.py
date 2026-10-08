from pymongo import MongoClient
from cassandra.cluster import Cluster
import time
import json

print(" COMPARAISON MongoDB vs Cassandra")

#Données de test
test_data = {
    "session_id": "test_session_001",
    "user_id": 1,
    "product_title": "Test Product",
    "category": "electronics",
    "price": 99.99,
    "action": "purchase",
    "action_score": 5,
    "recommandation": "Suggerer produits similaires",
    "review_clean": "great product",
    "timestamp": time.time()
}

N = 1000  
# MONGODB
print("\nTEST MONGODB")

mongo_client = MongoClient("localhost", 27017)
mongo_db = mongo_client["benchmark_test"]
mongo_col = mongo_db["test_collection"]
mongo_col.drop()  # nettoyer

# Test écriture MongoDB
start = time.time()
for i in range(N):
    doc = test_data.copy()
    doc["session_id"] = f"session_{i}"
    doc["timestamp"] = time.time()
    mongo_col.insert_one(doc)
mongo_write = time.time() - start
print(f" Écriture {N} docs    : {mongo_write:.3f}s")
print(f"   Vitesse             : {N/mongo_write:.0f} docs/sec")

# Test lecture MongoDB
start = time.time()
count = mongo_col.count_documents({})
mongo_read_count = time.time() - start
print(f" Count ({count} docs)  : {mongo_read_count:.3f}s")

start = time.time()
results = list(mongo_col.find({"action": "purchase"}))
mongo_read_query = time.time() - start
print(f" Query par action     : {mongo_read_query:.3f}s ({len(results)} résultats)")

start = time.time()
results = list(mongo_col.find({"category": "electronics"}))
mongo_read_cat = time.time() - start
print(f" Query par catégorie  : {mongo_read_cat:.3f}s ({len(results)} résultats)")

mongo_client.close()

# TEST CASSANDRA
print("\n TEST CASSANDRA")
cass_cluster = Cluster(['127.0.0.1'], port=9042, connect_timeout=60)
cass_session = cass_cluster.connect()
cass_session.default_timeout = 60

# Créer keyspace de test
cass_session.execute("""
    CREATE KEYSPACE IF NOT EXISTS benchmark_test
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
cass_session.set_keyspace('benchmark_test')

# Créer table de test
cass_session.execute("DROP TABLE IF EXISTS test_collection")
cass_session.execute("""
    CREATE TABLE test_collection (
        session_id      TEXT,
        user_id         INT,
        product_title   TEXT,
        category        TEXT,
        price           FLOAT,
        action          TEXT,
        action_score    INT,
        recommandation  TEXT,
        review_clean    TEXT,
        timestamp       DOUBLE,
        PRIMARY KEY (session_id, timestamp)
    )
""")

# Préparer la requête
insert_stmt = cass_session.prepare("""
    INSERT INTO test_collection (
        session_id, user_id, product_title, category,
        price, action, action_score, recommandation,
        review_clean, timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Test écriture Cassandra
start = time.time()
for i in range(N):
    cass_session.execute(insert_stmt, (
        f"session_{i}", 1, "Test Product", "electronics",
        99.99, "purchase", 5, "Suggerer produits similaires",
        "great product", time.time()
    ))
cass_write = time.time() - start
print(f" Écriture {N} rows    : {cass_write:.3f}s")
print(f"   Vitesse             : {N/cass_write:.0f} rows/sec")

# Test lecture Cassandra
start = time.time()
count = cass_session.execute("SELECT COUNT(*) FROM test_collection").one()[0]
cass_read_count = time.time() - start
print(f" Count ({count} rows)  : {cass_read_count:.3f}s")

cass_cluster.shutdown()

print(" Bilan de Comparaison")
print(f"\n{'Métrique':<30} {'MongoDB':>10} {'Cassandra':>10}")
print(f"{'Écriture ' + str(N) + ' docs (sec)':<30} {mongo_write:>10.3f} {cass_write:>10.3f}")
print(f"{'Vitesse écriture (docs/sec)':<30} {N/mongo_write:>10.0f} {N/cass_write:>10.0f}")
print(f"{'Lecture COUNT (sec)':<30} {mongo_read_count:>10.3f} {cass_read_count:>10.3f}")
print(f"{'Query filtrée (sec)':<30} {mongo_read_query:>10.3f} {'N/A':>10}")

print("\nANALYSE :")
if mongo_write < cass_write:
    print(f"  MongoDB plus rapide en écriture ({mongo_write:.3f}s vs {cass_write:.3f}s)")
else:
    print(f"  Cassandra plus rapide en écriture ({cass_write:.3f}s vs {mongo_write:.3f}s)")

if mongo_read_query < cass_read_count:
    print(f"  MongoDB plus rapide en lecture filtrée")
else:
    print(f"  Cassandra plus rapide en lecture simple")

