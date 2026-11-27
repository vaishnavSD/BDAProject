"""
db.py — Database connection and session factory.
Uses SQLAlchemy async engine backed by asyncpg.
"""

import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"), override=False)

POSTGRES_URL = (
    f"postgresql+asyncpg://"
    f"{os.getenv('POSTGRES_USER','inventory')}:"
    f"{os.getenv('POSTGRES_PASSWORD','inventory')}@"
    f"{os.getenv('POSTGRES_HOST','localhost')}:"
    f"{os.getenv('POSTGRES_PORT','5432')}/"
    f"{os.getenv('POSTGRES_DB','smart_inventory')}"
)

engine = create_async_engine(POSTGRES_URL, echo=False, pool_size=10, max_overflow=20)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
