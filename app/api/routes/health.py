from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/")
async def root():
    return {"app": "Running Tracker", "status": "ok"}


@router.get("/health")
async def health_check():
    return {"status": "ok"}
