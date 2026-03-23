from fastapi import APIRouter

router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("/")
async def list_runs():
    return []


@router.get("/{run_id}")
async def get_run(run_id: int):
    return {"id": run_id}
