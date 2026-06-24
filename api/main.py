from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import diseases, vaccination, alerts, time_travel

app = FastAPI(
    title="Health Lakehouse API",
    description="REST API over the public health surveillance lakehouse",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(diseases.router)
app.include_router(vaccination.router)
app.include_router(alerts.router)
app.include_router(time_travel.router)


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "health-lakehouse-api"}


@app.get("/")
async def root():
    return {"message": "Health Lakehouse API", "docs": "/docs", "version": "1.0.0"}
