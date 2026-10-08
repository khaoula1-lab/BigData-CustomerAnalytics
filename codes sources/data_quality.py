import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#Charger données depuis CSV
df_clicks = pd.read_csv("/home/khaoula/projet_bigdata/final_clickstream.csv")
df_tx      = pd.read_csv("/home/khaoula/projet_bigdata/final_transactions.csv")
df_support = pd.read_csv("/home/khaoula/projet_bigdata/final_support.csv")

print("=" * 60)
print("       DATA QUALITY FRAMEWORK")
print("=" * 60)
print(f"\nClickstream : {len(df_clicks)} lignes | {len(df_clicks.columns)} colonnes")
print(f"Transactions: {len(df_tx)} lignes | {len(df_tx.columns)} colonnes")
print(f"Support     : {len(df_support)} lignes | {len(df_support.columns)} colonnes")

#valeurs manquantes
print("\n COMPLETUDE — Valeurs manquantes :")
for name, df in [("Clickstream", df_clicks), ("Transactions", df_tx), ("Support", df_support)]:
    total = len(df)
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    print(f"\n  [{name}] — {total} lignes")
    if len(nulls) == 0:
        print("  OK Aucune valeur manquante")
    else:
        for col, n in nulls.items():
            print(f"  ATTENTION {col}: {n} manquants ({round(n/total*100,1)}%)")

#doublons
print("\n DOUBLONS :")
for name, df, key in [
    ("Clickstream", df_clicks, "session_id"),
    ("Transactions", df_tx, "session_id"),
    ("Support", df_support, "session_id")
]:
    if key in df.columns:
        dups = df.duplicated(subset=[key]).sum()
        print(f"  [{name}] — {dups} doublons sur '{key}'")
    else:
        dups = df.duplicated().sum()
        print(f"  [{name}] — {dups} doublons (toutes colonnes)")

#coherence des valeurs
print("\n COHERENCE des valeurs :")
if "action" in df_clicks.columns:
    valid_actions = ["view", "add_to_cart", "purchase", "abandon", "search"]
    invalid = df_clicks[~df_clicks["action"].isin(valid_actions)]
    print(f"  Actions invalides      : {len(invalid)}")

if "price" in df_clicks.columns:
    neg_prices = df_clicks[df_clicks["price"] < 0]
    print(f"  Prix negatifs          : {len(neg_prices)}")

if "rating" in df_clicks.columns:
    bad_ratings = df_clicks[(df_clicks["rating"] < 0) | (df_clicks["rating"] > 5)]
    print(f"  Ratings hors [0-5]     : {len(bad_ratings)}")

if "duree_visite" in df_clicks.columns:
    bad_duree = df_clicks[df_clicks["duree_visite"] < 0]
    print(f"  Durees negatives       : {len(bad_duree)}")

if "statut_paiement" in df_tx.columns:
    valid_statuts = ["succes", "echoue", "en_attente", "succès", "échoué"]
    invalid_tx = df_tx[~df_tx["statut_paiement"].isin(valid_statuts)]
    print(f"  Statuts invalides      : {len(invalid_tx)}")

if "montant_paye" in df_tx.columns:
    neg_montant = df_tx[df_tx["montant_paye"] < 0]
    print(f"  Montants negatifs      : {len(neg_montant)}")

#statistiques descriptives
print("\n STATISTIQUES DESCRIPTIVES :")
print("\n  --- Clickstream ---")
for col in ["price", "rating", "duree_visite"]:
    if col in df_clicks.columns:
        print(f"  {col:15} | Min: {df_clicks[col].min():.2f} | Max: {df_clicks[col].max():.2f} | Moy: {df_clicks[col].mean():.2f} | Std: {df_clicks[col].std():.2f}")

print("\n  --- Transactions ---")
for col in ["montant_paye"]:
    if col in df_tx.columns:
        print(f"  {col:15} | Min: {df_tx[col].min():.2f} | Max: {df_tx[col].max():.2f} | Moy: {df_tx[col].mean():.2f} | Std: {df_tx[col].std():.2f}")

#biais
print("\n BIAIS ALGORITHMIQUES :")
if "action" in df_clicks.columns:
    dist = df_clicks["action"].value_counts(normalize=True) * 100
    print("\n  Distribution des actions :")
    for action, pct in dist.items():
        print(f"    {action:15} : {pct:.1f}%")

if "category" in df_clicks.columns:
    cat_dist = df_clicks["category"].value_counts(normalize=True) * 100
    print("\n  Distribution des categories :")
    for cat, pct in cat_dist.items():
        print(f"    {cat:30} : {pct:.1f}%")

if "source" in df_clicks.columns:
    src_dist = df_clicks["source"].value_counts(normalize=True) * 100
    print("\n  Distribution des sources :")
    for src, pct in src_dist.items():
        print(f"    {src:20} : {pct:.1f}%")

#graphiques
fig, axes = plt.subplots(2, 3, figsize=(18, 10))

# Valeurs manquantes
nulls_pct = df_clicks.isnull().mean() * 100
nulls_pct = nulls_pct[nulls_pct > 0]
if len(nulls_pct) > 0:
    nulls_pct.plot(kind="bar", ax=axes[0,0], color="red")
    axes[0,0].set_title("Valeurs Manquantes (%)")
else:
    axes[0,0].text(0.5, 0.5, "Aucune valeur manquante",
                   ha="center", va="center", fontsize=12, color="green")
    axes[0,0].set_title("Valeurs Manquantes")

# Distribution actions
if "action" in df_clicks.columns:
    df_clicks["action"].value_counts().plot(
        kind="pie", ax=axes[0,1], autopct="%1.1f%%",
        colors=sns.color_palette("Set2")
    )
    axes[0,1].set_title("Distribution des Actions")
    axes[0,1].set_ylabel("")

# Distribution categories
if "category" in df_clicks.columns:
    df_clicks["category"].value_counts().plot(
        kind="bar", ax=axes[0,2],
        color=sns.color_palette("Set3")
    )
    axes[0,2].set_title("Distribution des Categories")
    axes[0,2].tick_params(axis="x", rotation=30)

# Distribution prix
if "price" in df_clicks.columns:
    df_clicks["price"].plot(kind="hist", bins=30, ax=axes[1,0], color="#3498db")
    axes[1,0].set_title("Distribution des Prix")
    axes[1,0].set_xlabel("Prix")

# Distribution rating
if "rating" in df_clicks.columns:
    df_clicks["rating"].plot(kind="hist", bins=20, ax=axes[1,1], color="#2ecc71")
    axes[1,1].set_title("Distribution des Ratings")
    axes[1,1].set_xlabel("Rating")

# Distribution montant transactions
if "montant_paye" in df_tx.columns:
    df_tx["montant_paye"].plot(kind="hist", bins=30, ax=axes[1,2], color="#e74c3c")
    axes[1,2].set_title("Distribution des Montants")
    axes[1,2].set_xlabel("Montant (MAD)")

plt.suptitle("Data Quality Framework — E-Commerce Big Data", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig("/home/khaoula/projet_bigdata/data_quality.png", dpi=150)
print("\nGraphique sauvegarde : data_quality.png")
print("\nAnalyse Data Quality terminee !")