def build_engine(url: str, *, echo: bool = False, pool_pre_ping: bool = True):
    try:
        from sqlalchemy import create_engine
    except ImportError as exc:
        raise ImportError(
            "SQLAlchemy is required for database connections. "
            "Add it to requirements.txt"
        ) from exc

    return create_engine(
        url,
        echo=echo,
        pool_pre_ping=pool_pre_ping,
        future=True,
    )


def build_sessionmaker(engine):
    try:
        from sqlalchemy.orm import sessionmaker
    except ImportError as exc:
        raise ImportError(
            "SQLAlchemy is required for database sessions. "
            "Add it to requirements.txt"
        ) from exc

    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
