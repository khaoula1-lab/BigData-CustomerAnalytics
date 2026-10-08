from pymongo import MongoClient
from textblob import TextBlob
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, f1_score, accuracy_score, recall_score, precision_score
from sklearn.preprocessing import LabelEncoder
from imblearn.over_sampling import SMOTE
import nltk
import time
import joblib
import warnings
warnings.filterwarnings('ignore')

# Télécharger les ressources NLTK
nltk.download('punkt',     quiet=True)
nltk.download('stopwords', quiet=True)
nltk.download('wordnet',   quiet=True)
nltk.download('punkt_tab', quiet=True)

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

print("=" * 62)
print("   MODÉLISATION IA — Prédiction Intention d'Achat")
print("=" * 62)


df = pd.read_csv('/home/khaoula/projet_bigdata/clickstream_enrichi.csv')

#NLP COMPLET
print("\n🔍 NLP COMPLET — Tokenisation + Sentiment + Profil...")

stop_words  = set(stopwords.words('english'))
lemmatizer  = WordNetLemmatizer()

positive_words = ['great', 'excellent', 'good', 'happy', 'satisfied',
                  'awesome', 'love', 'perfect', 'recommended', 'pleased',
                  'impressed', 'wonderful', 'fantastic', 'amazing',
                  'quality', 'fast', 'shipping', 'value']
negative_words = ['poor', 'bad', 'disappointed', 'unhappy', 'terrible',
                  'worst', 'dissatisfied', 'waste', 'awful', 'horrible',
                  'useless', 'broken', 'cheap', 'slow', 'expensive']

#tokenisation
def tokeniser_nltk(text):
    """
    Tokenisation complète :
    1. Découpage en tokens
    2. Suppression stopwords
    3. Lemmatisation
    """
    try:
        tokens     = word_tokenize(str(text).lower())
        tokens     = [t for t in tokens if t.isalpha()]
        tokens     = [t for t in tokens if t not in stop_words]
        tokens     = [lemmatizer.lemmatize(t) for t in tokens]
        return tokens
    except:
        return []

#analyse de sentiment (textblob)
def analyser_sentiment(text):
    """
    Analyse de sentiment :
     polarity > 0.2  = positif
     polarity < -0.2 = negatif
     sinon           = neutre
    """
    try:
        score = TextBlob(str(text)).sentiment.polarity
        if score > 0.2:  return "positif", round(score, 3)
        if score < -0.2: return "negatif", round(score, 3)
        return "neutre", round(score, 3)
    except:
        return "neutre", 0.0

# Subjectivité TextBlob 
def analyser_subjectivite(text):
    """
    Subjectivité :
    → 0 = objectif (fait)
    → 1 = subjectif (opinion)
    """
    try:
        return round(TextBlob(str(text)).sentiment.subjectivity, 3)
    except:
        return 0.0

#2.4 Appliquer NLP 
print(" Tokenisation NLTK...")
df['tokens']          = df['review_clean'].fillna('').apply(tokeniser_nltk)
df['nb_tokens']       = df['tokens'].apply(len)

print("  Analyse sentiment TextBlob...")
sentiments            = df['review_clean'].fillna('').apply(analyser_sentiment)
df['sentiment']       = sentiments.apply(lambda x: x[0])
df['sentiment_score'] = sentiments.apply(lambda x: x[1])
df['subjectivite']    = df['review_clean'].fillna('').apply(analyser_subjectivite)

print("  Comptage mots positifs/négatifs...")
df['nb_mots_positifs'] = df['tokens'].apply(
    lambda tokens: sum(1 for t in tokens if t in positive_words))
df['nb_mots_negatifs'] = df['tokens'].apply(
    lambda tokens: sum(1 for t in tokens if t in negative_words))

# Score NLP combiné
df['score_nlp'] = df['nb_mots_positifs'] - df['nb_mots_negatifs']

print(f"\n Résultats NLP :")
print(f"   Sentiment positif : {(df['sentiment']=='positif').sum()}")
print(f"   Sentiment négatif : {(df['sentiment']=='negatif').sum()}")
print(f"   Sentiment neutre  : {(df['sentiment']=='neutre').sum()}")
print(f"   Tokens moyens/review : {df['nb_tokens'].mean():.1f}")
print(f"   Subjectivité moyenne : {df['subjectivite'].mean():.3f}")

# Profil comportemental 
print("\n   → Classification profil comportemental...")

def classifier_profil(row):
    """
    Détecte le profil de l'utilisateur :
    - client_fidele      : achète souvent, bon score
    - chasseur_promos    : abandonne souvent, sentiment négatif
    - acheteur_impulsif  : achète vite sans beaucoup naviguer
    - comparateur        : navigue beaucoup sans acheter
    - visiteur_standard  : comportement neutre
    """
    taux     = row.get('taux_achat_user', 0)
    abandons = row.get('nb_abandons_user', 0)
    achats   = row.get('nb_achats_user', 0)
    actions  = max(row.get('nb_actions_user', 1), 1)
    score    = row.get('score_engagement_total', 0)
    sentiment= row.get('sentiment', 'neutre')

    if taux > 0.4 and score > 10:
        return "client_fidele"
    elif abandons > achats and sentiment == 'negatif':
        return "chasseur_promos"
    elif achats > 0 and (actions / max(achats, 1)) < 3:
        return "acheteur_impulsif"
    elif actions > 10 and taux < 0.1:
        return "comparateur"
    else:
        return "visiteur_standard"

df['profil_comportemental'] = df.apply(classifier_profil, axis=1)

print(f"\n Profils comportementaux :")
print(df['profil_comportemental'].value_counts().to_string())

# PRÉPARER LES DONNÉES
df = df[df['action'].isin(['purchase', 'abandon'])].copy()
df['intention_achat'] = (df['action'] == 'purchase').astype(int)

df['risque_abandon'] = (
    (df['action'] == 'abandon') &
    (df['sentiment'] == 'negatif')
).astype(int)

print(f"\n Distribution cible :")
print(f"   Achat (1)     : {df['intention_achat'].sum()} ({df['intention_achat'].mean()*100:.1f}%)")
print(f"   Abandon (0)   : {(df['intention_achat']==0).sum()} ({(1-df['intention_achat'].mean())*100:.1f}%)")
print(f"   Risque abandon: {df['risque_abandon'].sum()}")

# Encodage
le_category  = LabelEncoder()
le_source    = LabelEncoder()
le_page      = LabelEncoder()
le_sentiment = LabelEncoder()
le_profil    = LabelEncoder()

df['category_enc']  = le_category.fit_transform(df['category'].astype(str))
df['source_enc']    = le_source.fit_transform(df['source'].astype(str))
df['page_enc']      = le_page.fit_transform(df['page_visitee'].fillna('unknown').astype(str))
df['sentiment_enc'] = le_sentiment.fit_transform(df['sentiment'].astype(str))
df['profil_enc']    = le_profil.fit_transform(df['profil_comportemental'].astype(str))

#FEATURES
features_base = [
    'price', 'rating', 'duree_visite',
    'category_enc', 'source_enc', 'page_enc'
]

features_nlp = [
    'sentiment_enc', 'sentiment_score', 'subjectivite',
    'nb_tokens', 'nb_mots_positifs', 'nb_mots_negatifs',
    'score_nlp'
]

features_profil = [
    'profil_enc'
]

features_enrichies = [
    'nb_actions_user', 'nb_achats_user', 'nb_abandons_user',
    'duree_moy_user', 'taux_achat_user', 'score_engagement_total',
    'montant_total_depense', 'nb_transactions_reussies', 'panier_moyen',
    'nb_tickets_support', 'nb_remboursements',
    'taux_achat_categorie', 'prix_moy_categorie',
    'risque_abandon'
]

features = features_base + features_nlp + features_profil + \
           [f for f in features_enrichies if f in df.columns]

print(f"\n Features utilisées ({len(features)}) :")
for f in features:
    print(f"   + {f}")

X = df[features].fillna(0)
y = df['intention_achat']

# SPLIT TRAIN / TEST
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"\n Split Train/Test :")
print(f"   Train : {len(X_train)} | Test : {len(X_test)}")

# SMOTE 
smote = SMOTE(random_state=42)
X_train_sm, y_train_sm = smote.fit_resample(X_train, y_train)
print(f" SMOTE : {len(X_train_sm)} lignes équilibrées")

#ENTRAÎNER LES 3 MODÈLES
models = {
    "Logistic Regression": LogisticRegression(max_iter=1000, random_state=42),
    "Random Forest"      : RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1),
    "Gradient Boosting"  : GradientBoostingClassifier(n_estimators=100, random_state=42)
}

print("\n" + "=" * 62)
print("       COMPARAISON DES 3 MODÈLES")
print("=" * 62)

results      = []
meilleur_f1  = 0
meilleur_nom = None
meilleur_mod = None

for name, model in models.items():
    print(f"\n--- {name} ---")

    start      = time.time()
    model.fit(X_train_sm, y_train_sm)
    train_time = time.time() - start

    start      = time.time()
    y_pred     = model.predict(X_test)
    infer_time = time.time() - start

    f1  = f1_score(y_test, y_pred, zero_division=0)
    acc = accuracy_score(y_test, y_pred)
    rec = recall_score(y_test, y_pred, zero_division=0)
    pre = precision_score(y_test, y_pred, zero_division=0)

    print(classification_report(y_test, y_pred,
          target_names=['Abandon', 'Achat'], zero_division=0))
    print(f"  Entraînement : {train_time:.3f}s | Inférence : {infer_time:.4f}s")

    results.append({
        "Modèle"              : name,
        "Précision"           : round(pre, 3),
        "Rappel"              : round(rec, 3),
        "F1-Score"            : round(f1, 3),
        "Accuracy"            : round(acc, 3),
        "Temps train (s)"     : round(train_time, 3),
        "Temps inférence (s)" : round(infer_time, 4)
    })

    if f1 > meilleur_f1:
        meilleur_f1  = f1
        meilleur_nom = name
        meilleur_mod = model


print("\n" + "=" * 62)
print("   TABLEAU RÉCAPITULATIF")
print("=" * 62)
df_results = pd.DataFrame(results)
print(df_results.to_string(index=False))

# meilleur modèle et les sauvegarder
print(f"\n{'=' * 62}")
print(f"  Meilleur modèle : {meilleur_nom}")
print(f"     F1-Score        : {meilleur_f1:.3f}")
print(f"{'=' * 62}")

joblib.dump(meilleur_mod, '/home/khaoula/projet_bigdata/meilleur_modele.pkl')
joblib.dump(le_category,  '/home/khaoula/projet_bigdata/le_category.pkl')
joblib.dump(le_source,    '/home/khaoula/projet_bigdata/le_source.pkl')
joblib.dump(le_page,      '/home/khaoula/projet_bigdata/le_page.pkl')
joblib.dump(le_sentiment, '/home/khaoula/projet_bigdata/le_sentiment.pkl')
joblib.dump(le_profil,    '/home/khaoula/projet_bigdata/le_profil.pkl')
joblib.dump(features,     '/home/khaoula/projet_bigdata/features.pkl')

print(f"\n Modèle sauvegardé     : meilleur_modele.pkl")
print(f" Encodeurs sauvegardés  : category, source, page, sentiment, profil")
print(f" Features sauvegardées  : {len(features)} features")
print("\n ml_model.py terminé avec succès !")