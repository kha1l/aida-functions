def build_engine(url: str, *, echo: bool = False, pool_pre_ping: bool = True):
    """Создаёт async SQLAlchemy engine для указанного URL."""
    try:
        from sqlalchemy.ext.asyncio import create_async_engine
    except ImportError as exc:
        raise ImportError(
            "SQLAlchemy is required for database connections. "
            "Add it to requirements.txt"
        ) from exc

    return create_async_engine(
        _normalize_async_url(url),
        echo=echo,
        pool_pre_ping=pool_pre_ping,
        future=True,
    )


def build_sessionmaker(engine):
    """Создаёт async sessionmaker поверх переданного engine."""
    try:
        from sqlalchemy.ext.asyncio import async_sessionmaker
    except ImportError as exc:
        raise ImportError(
            "SQLAlchemy is required for database sessions. "
            "Add it to requirements.txt"
        ) from exc

    return async_sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def _normalize_async_url(url: str) -> str:
    """Нормализует PostgreSQL URL к asyncpg-совместимому виду."""
    if url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url
