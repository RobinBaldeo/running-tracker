from fastapi import APIRouter

router = APIRouter(prefix="/sync", tags=["sync"])


@router.post("/garmin")
async def sync_garmin():
    return {"status": "sync not yet implemented"}
