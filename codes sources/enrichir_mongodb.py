from pymongo import MongoClient
import pandas as pd
import numpy as np

print("Enrichissement des données MongoDB...")

client = MongoClient('localhost', 27017)
db = client['ecommerce']

clicks_df = pd.DataFrame(list(db['clickstream'].find()))
tx_df     = pd.DataFrame(list(db['transactions'].find()))
sup_df    = pd.DataFrame(list(db['service_client'].find()))

print(f" Chargé : {len(clicks_df)} clicks | {len(tx_df)} transactions | {len(sup_df)} tickets support")

# FEATURE 1 : Comportement utilisateur

# Nombre total d'actions par user
nb_actions_user = clicks_df.groupby('user_id').size().rename('nb_actions_user')

# Nombre d'achats par user
nb_achats_user = clicks_df[clicks_df['action'] == 'purchase'] \
    .groupby('user_id').size().rename('nb_achats_user')

# Nombre d'abandons par user
nb_abandons_user = clicks_df[clicks_df['action'] == 'abandon'] \
    .groupby('user_id').size().rename('nb_abandons_user')

# Durée moyenne de visite par user
duree_moy_user = clicks_df.groupby('user_id')['duree_visite'] \
    .mean().rename('duree_moy_user')

# Taux d'achat par user (achats / total actions)
taux_achat_user = (nb_achats_user / nb_actions_user).fillna(0).rename('taux_achat_user')

# FEATURE 2 : Comportement par catégorie
# Taux d'achat par catégorie
cat_actions = clicks_df.groupby('category').size()
cat_achats  = clicks_df[clicks_df['action'] == 'purchase'].groupby('category').size()
taux_achat_cat = (cat_achats / cat_actions).fillna(0).rename('taux_achat_categorie')

# Prix moyen par catégorie
prix_moy_cat = clicks_df.groupby('category')['price'].mean().rename('prix_moy_categorie')

# FEATURE 3 : Données transactions 
if len(tx_df) > 0:
    tx_succes = tx_df[tx_df['statut_paiement'] == 'succès']
    montant_total_user = tx_succes.groupby('user_id')['montant_paye'] \
        .sum().rename('montant_total_depense')
    nb_tx_user = tx_succes.groupby('user_id').size().rename('nb_transactions_reussies')
    panier_moyen_user = tx_succes.groupby('user_id')['montant_paye'] \
        .mean().rename('panier_moyen')
else:
    montant_total_user = pd.Series(dtype=float, name='montant_total_depense')
    nb_tx_user         = pd.Series(dtype=float, name='nb_transactions_reussies')
    panier_moyen_user  = pd.Series(dtype=float, name='panier_moyen')
# FEATURE 4 : Support client par user
if len(sup_df) > 0:
    nb_tickets_user   = sup_df.groupby('user_id').size().rename('nb_tickets_support')
    nb_remboursements = sup_df[sup_df['motif_contact'] == 'remboursement'] \
        .groupby('user_id').size().rename('nb_remboursements')
else:
    nb_tickets_user   = pd.Series(dtype=float, name='nb_tickets_support')
    nb_remboursements = pd.Series(dtype=float, name='nb_remboursements')

# FEATURE 5 : Score d'engagement combiné

score_engagement = clicks_df.groupby('user_id')['action_score'] \
    .sum().rename('score_engagement_total')

# ASSEMBLER TOUTES LES FEATURES
clicks_df = clicks_df.join(nb_actions_user,    on='user_id')
clicks_df = clicks_df.join(nb_achats_user,     on='user_id')
clicks_df = clicks_df.join(nb_abandons_user,   on='user_id')
clicks_df = clicks_df.join(duree_moy_user,     on='user_id')
clicks_df = clicks_df.join(taux_achat_user,    on='user_id')
clicks_df = clicks_df.join(score_engagement,   on='user_id')
clicks_df = clicks_df.join(montant_total_user, on='user_id')
clicks_df = clicks_df.join(nb_tx_user,         on='user_id')
clicks_df = clicks_df.join(panier_moyen_user,  on='user_id')
clicks_df = clicks_df.join(nb_tickets_user,    on='user_id')
clicks_df = clicks_df.join(nb_remboursements,  on='user_id')
clicks_df = clicks_df.join(taux_achat_cat,     on='category')
clicks_df = clicks_df.join(prix_moy_cat,       on='category')

# Remplir les valeurs manquantes
fill_zeros = [
    'nb_achats_user', 'nb_abandons_user', 'nb_transactions_reussies',
    'nb_tickets_support', 'nb_remboursements',
    'montant_total_depense', 'panier_moyen'
]
for col in fill_zeros:
    if col in clicks_df.columns:
        clicks_df[col] = clicks_df[col].fillna(0)

clicks_df['taux_achat_user']      = clicks_df['taux_achat_user'].fillna(0)
clicks_df['taux_achat_categorie'] = clicks_df['taux_achat_categorie'].fillna(0)

# SAUVEGARDER dans MongoDB 
new_cols = [
    'nb_actions_user', 'nb_achats_user', 'nb_abandons_user',
    'duree_moy_user', 'taux_achat_user', 'score_engagement_total',
    'montant_total_depense', 'nb_transactions_reussies', 'panier_moyen',
    'nb_tickets_support', 'nb_remboursements',
    'taux_achat_categorie', 'prix_moy_categorie'
]

print("\n Aperçu des nouvelles features :")
print(clicks_df[new_cols].describe().round(2))

# Supprimer l'ancienne collection enrichie si elle existe
db['clickstream_enrichi'].drop()

# Insérer les données enrichies
records = clicks_df.drop(columns=['_id']).to_dict('records')
db['clickstream_enrichi'].insert_many(records)

clicks_df.drop(columns=['_id']).to_csv('clickstream_enrichi.csv', index=False)

print(f"\n {len(records)} documents enrichis sauvegardés !")
print("   → MongoDB  : collection 'clickstream_enrichi'")
print("   → CSV      : clickstream_enrichi.csv")
print("\n Nouvelles features ajoutées :")
for col in new_cols:
    print(f"   + {col}")

client.close()