from fastapi import FastAPI

from .routers.jobs import router as jobs_router

app = FastAPI(title="Migration API", version="0.1.0")
app.include_router(jobs_router)


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}