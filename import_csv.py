import pandas as pd
from dateutil import parser as date_parser
from sqlalchemy import delete

from db import SessionLocal, engine, Base
from models import RepositoryCore, RepositoryMeta


def normalize_bool(value):
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "t", "1", "yes"}


def main():
    Base.metadata.create_all(bind=engine)

    df = pd.read_csv("github_stars.csv")
    df.columns = [col.strip().lower() for col in df.columns]

    required = {
        "rank",
        "namewithowner",
        "stars",
        "language",
        "isarchived",
        "isfork",
        "createdat",
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in github_stars.csv: {sorted(missing)}")

    session = SessionLocal()

    try:
        session.execute(delete(RepositoryMeta))
        session.execute(delete(RepositoryCore))

        for _, row in df.iterrows():
            core = RepositoryCore(
                namewithowner=str(row["namewithowner"]).strip(),
                stars=int(row["stars"]),
                createdat=date_parser.isoparse(str(row["createdat"]))
            )

            meta = RepositoryMeta(
                rank=int(row["rank"]),
                language=None if pd.isna(row["language"]) or str(row["language"]).strip() == "" else str(row["language"]).strip(),
                isarchived=normalize_bool(row["isarchived"]),
                isfork=normalize_bool(row["isfork"])
            )

            core.meta = meta
            session.add(core)

        session.commit()
        print(f"✅ Imported {len(df)} repositories into PostgreSQL (2-table schema).")

    except Exception as e:
        session.rollback()
        raise RuntimeError(f"Import failed: {e}") from e

    finally:
        session.close()


if __name__ == "__main__":
    main()