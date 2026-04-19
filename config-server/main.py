from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Set

app = FastAPI()

registry: Dict[str, Set[str]] = {}

class ServiceRegistration(BaseModel):
    service_name: str
    address: str

@app.post("/register")
async def register_service(req: ServiceRegistration):
    if req.service_name not in registry:
        registry[req.service_name] = set()
    registry[req.service_name].add(req.address)
    print(f"Registered: {req.service_name} at {req.address}")
    return {"status": "registered", "service": req.service_name, "address": req.address}

@app.get("/services/{service_name}")
async def get_service_locations(service_name: str):
    if service_name not in registry or not registry[service_name]:
        raise HTTPException(status_code=404, detail="Service not found")
    return list(registry[service_name])