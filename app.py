from flask import Flask, render_template
from sqlalchemy import select

from db import SessionLocal
from models import RepositoryCore, RepositoryMeta

app = Flask(__name__)


@app.route("/")
def index():
    session = SessionLocal()

    try:
        stmt = (
            select(RepositoryCore)
            .join(RepositoryMeta)
            .order_by(RepositoryCore.stars.desc())
            .limit(10000)
        )

        repos = session.execute(stmt).scalars().all()

        return render_template("index.html", repos=repos)

    finally:
        session.close()


if __name__ == "__main__":
    app.run(debug=True)