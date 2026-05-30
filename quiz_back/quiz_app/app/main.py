"""FastAPI application entrypoint (Quiz service)."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.routers import auth, games, questions, teams, users, ws

app = FastAPI(title="Quiz Service API")

app.include_router(auth.router)
app.include_router(users.router)
app.include_router(teams.router)
app.include_router(questions.router)
app.include_router(games.router)
app.include_router(ws.router)

# Lightweight built-in UI for manual testing (no separate frontend server).
_UI_DIR = Path(__file__).resolve().parent / "ui"
if _UI_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/ui/")
