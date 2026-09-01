from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes.simulator_routes import router as simulator_router

app = FastAPI(
    title="Daemon Supply Chain Simulator API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(simulator_router)


@app.get("/")
def root():
    return {
        "service": "daemon-supply-chain-simulator",
        "status": "UP"
    }