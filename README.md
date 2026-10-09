# BigData-CustomerAnalytics

Analyse prédictive du parcours client et moteur de recommandation en temps réel pour une plateforme e-commerce simulée.

Projet de fin de module « Fondamentaux de Big Data », ENSA Tétouan, filière Sciences des Données, Big Data & IA, année 2025-2026. Encadrant : Pr. Imad Sassi.

**Stack :** Apache Kafka · Spark Structured Streaming · MongoDB · Cassandra · Scikit-learn · NLTK / TextBlob · Streamlit · Docker

## Aperçu

Un producteur génère en continu des événements e-commerce (navigation, paiements, support client) à partir de deux API publiques. Ces événements transitent par Kafka, sont enrichis par Spark (NLP, score d'engagement, recommandation), stockés dans MongoDB, puis affichés dans un dashboard Streamlit mis à jour toutes les 10 secondes. Un modèle de Machine Learning prédit l'intention d'achat (achat ou abandon).


<img width="1920" height="1080" alt="Capture d&#39;écran 2026-10-08 213726" src="https://github.com/user-attachments/assets/298ab1a6-b760-4ac3-8962-8031ae7bcf02" />

## Architecture



<img width="572" height="617" alt="image" src="https://github.com/user-attachments/assets/32218c74-2773-494a-bf75-b2fd4d8591c2" />



| Topic Kafka | Contenu |
|---|---|
| `clickstream` | Navigation : session, action, produit, source, avis |
| `transactions` | Paiements : montant, méthode, statut |
| `service_client` | Support : motif, canal, résolu |

## Structure du dépôt

```
.
├── codes sources/                 Scripts Python (producteur, consommateurs, ML, dashboard)
├── bases de données (résultats)/  Exports finaux (clickstream, transactions, support) et rapport qualité
├── données entrainement modèles/  Jeux de données utilisés pour l'entraînement
├── docker-compose.yml             Mode local : 1 broker Kafka + MongoDB
├── docker-compose-distributed.yml Mode distribué : 2 brokers Kafka, MongoDB Replica Set, Mongo Express
├── *.pkl                          Modèle retenu, encodeurs et liste des features
└── Final_projet_BIG_DATA__Rapport .pdf
```

| Script | Rôle |
|---|---|
| `producteur.py` | Génère les événements et les publie dans Kafka |
| `consomateur.py` | Consommateur Spark : NLP, ML, recommandations |
| `consumer_mongodb.py` | Spark vers MongoDB (micro-batches de 10 s) |
| `consumer_cassandra.py` | Spark vers Cassandra |
| `enrichir_mongodb.py` | Enrichissement des données stockées |
| `data_quality.py` | Contrôle qualité (complétude, doublons, cohérence) |
| `ml_comparaison.py` | Entraînement et comparaison de 3 modèles |
| `comparaison_nosql.py` | Benchmark MongoDB vs Cassandra |
| `ethics_compliance (1).py` | Pseudonymisation des identifiants (RGPD) |
| `dashboard.py` | Dashboard Streamlit |

## Prérequis

- Docker et Docker Compose
- Python 3.12
- Java (JDK 8, 11 ou 17) pour Spark
- Environ 4 Go de RAM disponibles pour les conteneurs et Spark
- Accès internet pour le producteur (appels aux API FakeStore et DummyJSON)

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows : .venv\Scripts\Activate.ps1
pip install requests kafka-python cassandra-driver pymongo pyspark pandas numpy \
            scikit-learn imbalanced-learn joblib nltk textblob wordcloud \
            matplotlib seaborn plotly streamlit
python -c "import nltk; nltk.download('punkt'); nltk.download('punkt_tab'); nltk.download('stopwords'); nltk.download('wordnet')"
```

## Lancement

Chaque commande tourne dans un terminal distinct, depuis le dossier `codes sources`.

1. **Infrastructure** (à la racine du dépôt), puis attendre environ 30 secondes que Kafka démarre :
   ```bash
   docker-compose up -d
   docker ps
   ```
2. **Consommateur Spark vers MongoDB** :
   ```bash
   python3 consumer_mongodb.py
   ```
3. **Producteur** :
   ```bash
   python3 producteur.py
   ```
4. **Dashboard** :
   ```bash
   streamlit run dashboard.py
   ```
   Puis ouvrir http://localhost:8501.

Le mode distribué (2 brokers, Replica Set, Mongo Express sur le port 8081) se lance avec `docker-compose -f docker-compose-distributed.yml up -d`.

Pour arrêter : `docker-compose down`.

## Dashboard

Le dashboard lit MongoDB en direct et se rafraîchit toutes les 10 secondes : KPIs, courbe de conversion, tunnel de conversion, répartition par catégorie et source de trafic, statut des paiements, motifs de support, nuage de mots des avis et comparaison des modèles.


<img width="1920" height="1080" alt="Capture d&#39;écran 2026-10-08 213811" src="https://github.com/user-attachments/assets/a77bf3ea-4060-4eae-a055-556480c85ae2" />




<img width="1120" height="605" alt="image" src="https://github.com/user-attachments/assets/65e4f899-f5d2-45b5-bd94-b5a41cb7a1a9" />



<img width="1112" height="556" alt="image" src="https://github.com/user-attachments/assets/ef8faaff-e4da-4795-a5f2-94327198c608" />




## Résultats

### Modèles de prédiction d'achat (achat vs abandon)

Les trois modèles sont entraînés avec SMOTE sur le jeu d'entraînement et un découpage 80/20 stratifié. Le meilleur, choisi sur le F1-Score, est sauvegardé dans `meilleur_modele.pkl`.

| Modèle | Précision | Rappel | F1-Score | Accuracy |
|---|---|---|---|---|
| Régression logistique | 0.671 | 0.738 | 0.703 | 0.689 |
| Random Forest | 0.662 | 0.617 | 0.639 | 0.652 |
| Gradient Boosting | 0.669 | 0.705 | 0.686 | 0.679 |

### MongoDB vs Cassandra (1000 insertions)

| Métrique | MongoDB | Cassandra |
|---|---|---|
| Écriture (s) | 0.431 | 2.464 |
| Vitesse (docs/s) | 2323 | 406 |
| Lecture COUNT (s) | 0.001 | 0.099 |

### Qualité des données

Complétude de 100 %, aucun doublon, cohérence validée sur six contraintes métier. Détails dans le rapport (chapitre 8) et dans `bases de données (résultats)/data_quality.png`.

## Éthique et RGPD

- Minimisation : aucune donnée nominative ni bancaire n'entre dans Kafka.
- Pseudonymisation des `user_id` par hachage SHA-256 salé (`ethics_compliance (1).py`).
- Les recommandations suivent des règles métier explicites ; la décision finale reste humaine.

## Limites

- Les données sont **entièrement synthétiques** (actions quasi uniformes), donc les métriques ne sont pas représentatives d'un trafic e-commerce réel.
- 
## Équipe

| Membre | Rôle |
|---|---|
| Loubna Souali | Ingestion temps réel et orchestration des flux (Kafka) |
| Rime Khazraoui | Architecture globale, éthique et RGPD, documentation |
| Khaoula Anjroum | Stockage NoSQL, modélisation IA et analytics |
| Khadija El Ouarad | Traitement distribué en temps réel (Spark) |
| Rania Habibi | Data Quality, dashboard, installation hybride (Mongo Express) |

## Rapport

Le rapport complet est disponible dans le dépôt : `Final_projet_BIG_DATA__Rapport .pdf`.
