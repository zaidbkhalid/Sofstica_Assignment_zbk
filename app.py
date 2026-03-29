from flask import Flask, render_template, request
from sqlalchemy import select, func

from db import SessionLocal
from models import RepositoryCore, RepositoryMeta

app = Flask(__name__)

PER_PAGE = 100


@app.route("/")
def index():
    page = request.args.get("page", default=1, type=int)
    query_text = request.args.get("q", default="", type=str).strip()
    language = request.args.get("language", default="", type=str).strip()

    if page < 1:
        page = 1

    session = SessionLocal()

    try:
        base_query = (
            select(
                RepositoryCore.id,
                RepositoryMeta.rank,
                RepositoryCore.namewithowner,
                RepositoryCore.stars,
                RepositoryCore.createdat,
                RepositoryMeta.language,
                RepositoryMeta.isarchived,
                RepositoryMeta.isfork,
            )
            .join(RepositoryMeta, RepositoryCore.id == RepositoryMeta.id)
        )

        count_query = (
            select(func.count())
            .select_from(RepositoryCore)
            .join(RepositoryMeta, RepositoryCore.id == RepositoryMeta.id)
        )

        if query_text:
            pattern = f"%{query_text}%"
            base_query = base_query.where(RepositoryCore.namewithowner.ilike(pattern))
            count_query = count_query.where(RepositoryCore.namewithowner.ilike(pattern))

        if language:
            base_query = base_query.where(RepositoryMeta.language == language)
            count_query = count_query.where(RepositoryMeta.language == language)

        total_rows = session.scalar(count_query) or 0
        total_pages = max(1, (total_rows + PER_PAGE - 1) // PER_PAGE)
        if page > total_pages:
            page = total_pages

        offset = (page - 1) * PER_PAGE

        rows = session.execute(
            base_query.order_by(RepositoryCore.stars.desc())
            .limit(PER_PAGE)
            .offset(offset)
        ).all()

        language_rows = session.execute(
            select(RepositoryMeta.language)
            .where(RepositoryMeta.language.is_not(None))
            .distinct()
            .order_by(RepositoryMeta.language.asc())
        ).all()
        languages = [row[0] for row in language_rows if row[0]]

        total_repos = session.scalar(select(func.count()).select_from(RepositoryCore)) or 0
        max_stars = session.scalar(select(func.max(RepositoryCore.stars))) or 0

        return render_template(
            "index.html",
            rows=rows,
            page=page,
            per_page=PER_PAGE,
            total_rows=total_rows,
            total_pages=total_pages,
            query_text=query_text,
            selected_language=language,
            languages=languages,
            total_repos=total_repos,
            max_stars=max_stars,
        )
    finally:
        session.close()


if __name__ == "__main__":
    app.run(debug=True)