import os

import pandas as pd
from sqlalchemy import create_engine, text

# Connexion PostgreSQL
user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
db = os.environ["POSTGRES_DB"]

engine = create_engine(f"postgresql://{user}:{password}@db:5432/{db}")

# Création de la table
with engine.connect() as conn:
    conn.execute(
        text("""
        CREATE TABLE IF NOT EXISTS ventes (
            id SERIAL PRIMARY KEY,
            produit TEXT,
            quantite INTEGER,
            prix FLOAT
        )
    """)
    )
    conn.commit()

# Données simulées
df = pd.DataFrame(
    {
        "produit": ["A", "B", "C", "D"],
        "quantite": [10, 25, 5, 30],
        "prix": [9.99, 4.49, 19.99, 2.99],
    }
)

# Chargement dans PostgreSQL
df.to_sql("ventes", engine, if_exists="append", index=False)
print(f"✅ {len(df)} lignes chargées dans PostgreSQL")

# Vérification
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM ventes"))
    for row in result:
        print(row)
