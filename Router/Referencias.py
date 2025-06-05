from fastapi import APIRouter, Request, Depends
from sqlalchemy.orm import Session
from Class.Referencias import Referencias
from Utils.decorator import http_decorator
from Config.db import get_db

referencias_router = APIRouter()

@referencias_router.post('/cargar_archivo', tags=["Referencias"], response_model=dict)
@http_decorator
def cargar_archivo(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Referencias(db).cargar_archivo(data)
    return response

@referencias_router.post('/guardar_referencias', tags=["Referencias"], response_model=dict)
@http_decorator
def guardar_referencias(request: Request, db: Session = Depends(get_db)):
    data = getattr(request.state, "json_data", {})
    response = Referencias(db).guardar_referencias(data)
    return response
