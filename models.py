from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class RepositoryCore(Base):
    __tablename__ = "repositories_core"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    namewithowner: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    stars: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    createdat: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    meta: Mapped["RepositoryMeta"] = relationship(
        "RepositoryMeta",
        back_populates="core",
        uselist=False,
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_core_stars_desc", "stars"),
    )


class RepositoryMeta(Base):
    __tablename__ = "repositories_meta"

    id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("repositories_core.id"),
        primary_key=True,
    )

    rank: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    language: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    isarchived: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    isfork: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    core: Mapped["RepositoryCore"] = relationship(
        "RepositoryCore",
        back_populates="meta",
    )

    __table_args__ = (
        Index("ix_meta_language", "language"),
        Index("ix_meta_archived_fork", "isarchived", "isfork"),
    )


class CrawlShardProgress(Base):
    __tablename__ = "crawl_shard_progress"

    shard_key: Mapped[str] = mapped_column(String(128), primary_key=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", index=True)
    cursor: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    discovered_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )