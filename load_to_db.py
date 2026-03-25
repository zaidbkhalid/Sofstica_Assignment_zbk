import pandas as pd
from sqlalchemy import create_engine

# DB connection
DATABASE_URL = "postgresql+psycopg2://zaid:password@localhost:5432/github_data"

engine = create_engine(DATABASE_URL)

# Load CSV
df = pd.read_csv("github_stars.csv")

# Optional: clean column names (recommended)
df.columns = [col.lower() for col in df.columns]

# Push to DB
df.to_sql("repositories", engine, if_exists="replace", index=False)

print("✅ Data inserted into PostgreSQL successfully!")