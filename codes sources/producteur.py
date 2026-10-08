import requests
import json
import time
import random
import uuid
from kafka import KafkaProducer

#Connexion Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092,localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#Récupérer données depuis les 2 APIs 
def fetch_fakestore():
    res = requests.get("https://fakestoreapi.com/products")
    return res.json()

def fetch_dummyjson():
    res = requests.get("https://dummyjson.com/products?limit=30&select=title,reviews,rating")
    return res.json()['products']

#Créer les 3 types d'événements 

def create_clickstream_event(product, review_text, session_id):
    """Topic clickstream — navigation utilisateur"""
    action = random.choice(['view', 'add_to_cart', 'purchase', 'abandon', 'search'])
    rating = product.get('rating', {}).get('rate', 0) \
             if isinstance(product.get('rating'), dict) \
             else product.get('rating', 0)
    return {
        "session_id"   : session_id,
        "user_id"      : random.randint(1, 200),
        "product_id"   : product.get('id'),
        "product_title": product.get('title'),
        "category"     : product.get('category', 'unknown'),
        "price"        : product.get('price', 0),
        "rating"       : rating,
        "action"       : action,
        "page_visitee" : random.choice(['/accueil', '/produit', '/panier', '/checkout', '/recherche']),
        "duree_visite" : random.randint(5, 300),
        "source"       : random.choice(['google', 'direct', 'email', 'pub', 'reseaux_sociaux']),
        "review_text"  : review_text,
        "timestamp"    : time.time()
    }

def create_transaction_event(product, session_id, user_id):
    """Topic transactions — paiements"""
    return {
        "session_id"       : session_id,
        "user_id"          : user_id,
        "product_id"       : product.get('id'),
        "product_title"    : product.get('title'),
        "category"         : product.get('category', 'unknown'),
        "montant_paye"     : round(product.get('price', 0) * random.uniform(0.8, 1.0), 2),
        "methode_paiement" : random.choice(['carte', 'paypal', 'virement']),
        "statut_paiement"  : random.choice(['succès', 'succès', 'succès', 'échoué', 'en_attente']),
        "timestamp"        : time.time()
    }

def create_support_event(session_id, user_id):
    """Topic service_client — contacts support"""
    return {
        "session_id"   : session_id,
        "user_id"      : user_id,
        "motif_contact": random.choice(['remboursement', 'livraison', 'produit_défectueux', 'question']),
        "canal"        : random.choice(['chat', 'email', 'telephone']),
        "resolu"       : random.choice([True, False]),
        "timestamp"    : time.time()
    }

#Charger les données 
fakestore_products = fetch_fakestore()
dummyjson_products = fetch_dummyjson()

all_reviews = []
for p in dummyjson_products:
    for review in p.get('reviews', []):
        all_reviews.append(review.get('comment', ''))

print(f"{len(fakestore_products)} produits | {len(all_reviews)} reviews chargés")
print(" Envoi vers 3 topics Kafka...\n")

#Boucle principale
while True:
    for product in fakestore_products:
        # Générer une session unique par produit
        session_id = str(uuid.uuid4())[:8]
        user_id    = random.randint(1, 200)
        review     = random.choice(all_reviews) if all_reviews else ""

        #Topic 1 : clickstream 
        click_event = create_clickstream_event(product, review, session_id)
        producer.send('clickstream', value=click_event)
        review_preview = review[:30]
        print(" [clickstream]    session_" + session_id + " | user_" + str(user_id) + " | " + click_event['action'] + " | " + product['title'][:25] + " | review: " + review_preview)
        #Topic 2 : transaction
        if click_event['action'] == 'purchase':
            tx_event = create_transaction_event(product, session_id, user_id)
            producer.send('transactions', value=tx_event)
            print(f" [transactions]   user_{user_id} | {tx_event['statut_paiement']:10} | {tx_event['montant_paye']} MAD")

        #Topic 3 : service_client 
        if random.random() < 0.2:
            sup_event = create_support_event(session_id, user_id)
            producer.send('service_client', value=sup_event)
            print(f" [service_client] user_{user_id} | {sup_event['motif_contact']}")
        producer.flush()
        time.sleep(0.5)
