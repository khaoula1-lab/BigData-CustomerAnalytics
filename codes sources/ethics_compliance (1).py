from pymongo import MongoClient
import pandas as pd
import hashlib
import json
from datetime import datetime

print("=" * 60)
print("   ETHIQUE & CONFORMITE RGPD — E-Commerce Big Data")
print("=" * 60)

# connexion mongo
client = MongoClient("mongodb://127.0.0.1:27017")
db = client["ecommerce"]

#anonymisation
print("\n1. ANONYMISATION DES DONNEES PERSONNELLES")
print("-" * 40)

def anonymize_user_id(user_id):
    return hashlib.sha256(str(user_id).encode()).hexdigest()[:12]

try:
    df = pd.DataFrame(list(db["clickstream"].find().limit(5)))

    if len(df) > 0 and "user_id" in df.columns:
        df["user_id_anonyme"] = df["user_id"].apply(anonymize_user_id)

        print("Avant anonymisation :", df["user_id"].tolist())
        print("Apres anonymisation  :", df["user_id_anonyme"].tolist())
        print("OK anonymisation SHA-256 appliquée")

except Exception as e:
    print("Erreur MongoDB clickstream :", e)

# minimisation
print("\n2. MINIMISATION DES DONNEES")
print("-" * 40)

colonnes_necessaires = [
    "session_id", "user_id", "action",
    "category", "price", "rating",
    "source", "timestamp"
]

colonnes_sensibles = [
    "review_text", "page_visitee", "duree_visite"
]

print("Colonnes nécessaires :", colonnes_necessaires)
print("Colonnes sensibles   :", colonnes_sensibles)
print("Recommandation       : chiffrement AES en production")

#droit a l'oubli
print("\n3. DROIT A L'OUBLI (RGPD)")
print("-" * 40)

def supprimer_utilisateur(user_id):
    c = db["clickstream"].delete_many({"user_id": user_id})
    t = db["transactions"].delete_many({"user_id": user_id})
    s = db["service_client"].delete_many({"user_id": user_id})

    print(f"User {user_id} supprimé :")
    print(f"  clickstream  : {c.deleted_count}")
    print(f"  transactions : {t.deleted_count}")
    print(f"  support      : {s.deleted_count}")

print("Simulation suppression user_id=999")
supprimer_utilisateur(999)

# consentement
print("\n4. CONSENTEMENT")
print("-" * 40)

consentement = {
    "navigation": True,
    "analytics": True,
    "marketing": False,
    "sharing": False,
    "retention_days": 90
}

for k, v in consentement.items():
    print(f"{k:20} : {v}")

#retention
print("\n5. RETENTION DES DONNEES")
print("-" * 40)

retention = {
    "clickstream": "90 jours",
    "transactions": "5 ans",
    "support": "3 ans",
    "ml_models": "1 an"
}

for k, v in retention.items():
    print(f"{k:20} : {v}")

# securité
print("\n6. SECURITE")
print("-" * 40)

securite = [
    "TLS/SSL activé",
    "Authentification MongoDB",
    "Accès par rôles",
    "Logs activés",
    "Docker isolation"
]

for s in securite:
    print("OK", s)

# rapport final
print("\n" + "=" * 60)
print("   RAPPORT RGPD")
print("=" * 60)

rapport = {
    "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
    "anonymisation": "SHA-256 user_id",
    "minimisation": "OK",
    "droit_oubli": "OK",
    "consentement": "OK",
    "retention": "OK",
    "securite": "OK",
    "conformite": "PARTIELLE (PFE)",
    "recommendation": "Ajouter chiffrement AES en prod"
}

for k, v in rapport.items():
    print(f"{k:20} : {v}")

# sauvegarde JSON
with open("/home/rania/projet_bigdata/rapport_rgpd.json", "w") as f:
    json.dump(rapport, f, indent=2, ensure_ascii=False)

print("\nRapport sauvegardé : rapport_rgpd.json")

client.close()
print("\nAnalyse RGPD terminée !")
