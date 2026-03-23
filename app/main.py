from contextlib import asynccontextmanager

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_mcp import FastApiMCP

from app.api.routes import health, runs, sync
from app.tools import sleep, weather


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    yield
    # shutdown


app = FastAPI(title="Running Tracker", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(runs.router)
app.include_router(sync.router)
app.include_router(weather.router)
app.include_router(sleep.router)


# MCP Server
mcp = FastApiMCP(
    app,
    name="Running Tracker MCP",
    description="Running tracker with weather and Garmin sync tools",
)
mcp.mount()