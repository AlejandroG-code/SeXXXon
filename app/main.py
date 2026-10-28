from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api.reports import router as reports_router
from app.core.config import settings
from app.core.logging import logger
from app.db.models import Base
from app.db.session import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: ensure tables exist and storage path is prepared
    logger.info("Initializing database tables and storage path...")
    Base.metadata.create_all(bind=engine)
    storage_path = settings.get_storage_path()
    logger.info(f"Storage path ready: {storage_path}")
    yield
    logger.info("Application shutting down...")


app = FastAPI(
    title="Asynchronous Report Generator",
    description="Enterprise Asynchronous Sales Report Generation with RabbitMQ & PostgreSQL",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(reports_router)

# Mount static directory for Frontend Dashboard
static_path = Path(__file__).parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")


@app.get("/", include_in_schema=False)
def get_dashboard():
    """Serve the single page application dashboard."""
    index_file = static_path / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"message": "Async Report Generator API", "docs": "/docs"}


@app.get("/health", tags=["Monitoring"])
def health_check():
    """Liveness probe."""
    return {"status": "ok", "app": "async-report-generator"}


@app.get("/ready", tags=["Monitoring"])
def readiness_check():
    """Readiness probe checking database and broker connectivity."""
    db_ok = True
    broker_ok = True

    # Check DB
    try:
        with engine.connect() as conn:
            conn.execute(Base.metadata.tables["reports"].select().limit(1))
    except Exception:
        db_ok = False

    # Check RabbitMQ
    try:
        from app.broker.rabbitmq import get_rabbitmq_connection
        conn = get_rabbitmq_connection()
        conn.close()
    except Exception:
        broker_ok = False

    status_str = "ready" if (db_ok and broker_ok) else "degraded"
    return {
        "status": status_str,
        "database": "connected" if db_ok else "unreachable",
        "broker": "connected" if broker_ok else "unreachable",
    }
