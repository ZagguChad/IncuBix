from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from incubrix.config import get_settings


class Base(DeclarativeBase):
    pass


def make_engine(url: str | None = None):
    database_url = url or get_settings().database_url
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, pool_pre_ping=True, connect_args=connect_args)


engine = make_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def initialize_database() -> None:
    from incubrix import models  # noqa: F401

    database_url = get_settings().database_url
    if database_url.startswith("sqlite:"):
        Path(database_url.removeprefix("sqlite:///" )).parent.mkdir(parents=True, exist_ok=True)

    Base.metadata.create_all(bind=engine)
    _upgrade_legacy_schema()


def _upgrade_legacy_schema() -> None:
    """Apply the small, backwards-compatible changes needed by pre-audit DBs.

    This is deliberately limited to additive columns/indexes so local users of
    the original assessment build do not silently lose data. Production teams
    should replace it with a versioned migration tool before introducing
    destructive schema evolution.
    """

    inspector = inspect(engine)
    if "destinations" in inspector.get_table_names():
        destination_columns = {column["name"] for column in inspector.get_columns("destinations")}
        if "normalized_url" not in destination_columns:
            with engine.begin() as connection:
                connection.exec_driver_sql("ALTER TABLE destinations ADD COLUMN normalized_url VARCHAR(2048)")
        from incubrix.util import normalize_destination_url

        with engine.begin() as connection:
            rows = connection.execute(
                text("SELECT id, url, normalized_url FROM destinations")
            ).mappings()
            for row in rows:
                if not row["normalized_url"]:
                    connection.execute(
                        text("UPDATE destinations SET normalized_url = :normalized_url WHERE id = :id"),
                        {"normalized_url": normalize_destination_url(row["url"]), "id": row["id"]},
                    )
            duplicates = connection.execute(
                text("SELECT platform_key, normalized_url, COUNT(*) AS count "
                     "FROM destinations GROUP BY platform_key, normalized_url HAVING COUNT(*) > 1")
            ).mappings().all()
        # Earlier assessment builds permitted duplicate rows that differ only
        # in URL spelling. Merge those historical records into the oldest row
        # before installing the new uniqueness index. Jobs, posts, evidence,
        # rule proposals, tickets, and recommendation feedback retain their
        # relationship to the canonical destination.
        if duplicates:
            dependent_tables = {
                "posts": "destination_id",
                "certification_runs": "destination_id",
                "rule_proposals": "destination_id",
                "recertification_tickets": "destination_id",
                "recommendation_feedback": "destination_id",
            }
            table_names = set(inspector.get_table_names())
            with engine.begin() as connection:
                for duplicate in duplicates:
                    rows = connection.execute(text(
                        "SELECT id FROM destinations WHERE platform_key = :platform_key "
                        "AND normalized_url = :normalized_url ORDER BY created_at, id"
                    ), dict(duplicate)).mappings().all()
                    canonical_id = rows[0]["id"]
                    for row in rows[1:]:
                        duplicate_id = row["id"]
                        post_conflict = connection.execute(text(
                            "SELECT 1 FROM posts duplicate "
                            "JOIN posts canonical ON canonical.destination_id = :canonical_id "
                            "AND canonical.idempotency_key = duplicate.idempotency_key "
                            "WHERE duplicate.destination_id = :duplicate_id LIMIT 1"
                        ), {"canonical_id": canonical_id, "duplicate_id": duplicate_id}).first()
                        if post_conflict:
                            raise RuntimeError(
                                "Cannot merge duplicate destination records with conflicting post idempotency keys; "
                                "resolve the duplicate rows manually."
                            )
                        for table, column in dependent_tables.items():
                            if table in table_names:
                                connection.execute(text(
                                    f"UPDATE {table} SET {column} = :canonical_id WHERE {column} = :duplicate_id"
                                ), {"canonical_id": canonical_id, "duplicate_id": duplicate_id})
                        connection.execute(text("DELETE FROM destinations WHERE id = :duplicate_id"), {"duplicate_id": duplicate_id})

    if "jobs" in inspector.get_table_names():
        job_columns = {column["name"] for column in inspector.get_columns("jobs")}
        if "lease_expires_at" not in job_columns:
            with engine.begin() as connection:
                connection.exec_driver_sql("ALTER TABLE jobs ADD COLUMN lease_expires_at TIMESTAMP")

    if "certification_runs" in inspector.get_table_names():
        certification_columns = {column["name"] for column in inspector.get_columns("certification_runs")}
        with engine.begin() as connection:
            if "platform_post_id" not in certification_columns:
                connection.exec_driver_sql("ALTER TABLE certification_runs ADD COLUMN platform_post_id VARCHAR(300)")
            if "cleanup_deadline_at" not in certification_columns:
                connection.exec_driver_sql("ALTER TABLE certification_runs ADD COLUMN cleanup_deadline_at TIMESTAMP")

    required_indexes = {
        "destinations": ("ux_destinations_platform_normalized_url", ("platform_key", "normalized_url")),
        "posts": (
            "ux_posts_destination_idempotency",
            ("destination_id", "idempotency_key"),
        ),
        "jobs": ("ix_jobs_claimable", ("status", "not_before", "created_at")),
        "certification_runs": ("ix_certification_runs_cleanup_deadline_at", ("cleanup_deadline_at",)),
    }
    inspector = inspect(engine)
    for table, (index_name, columns) in required_indexes.items():
        if table not in inspector.get_table_names():
            continue
        existing = [
            tuple(index.get("column_names") or [])
            for index in inspector.get_indexes(table)
        ] + [
            tuple(constraint.get("column_names") or [])
            for constraint in inspector.get_unique_constraints(table)
        ]
        if columns in existing:
            continue
        unique = "UNIQUE " if index_name.startswith("ux_") else ""
        rendered_columns = ", ".join(columns)
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE {unique}INDEX {index_name} ON {table} ({rendered_columns})"
            )
