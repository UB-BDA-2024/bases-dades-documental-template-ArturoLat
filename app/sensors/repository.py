from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import json
from . import models, schemas
from app.redis_client import RedisClient
from ..mongodb_client import MongoDBClient


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    db_sensor_data = sensor.dict()

    # Guardem a MONGODB
    mongodb.insertDoc(db_sensor_data)

    return db_sensor

def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData) -> schemas.SensorData:
    db_sensordata = data
    redis.set(sensor_id, json.dumps(data.dict()))
    return db_sensordata

def get_data(redis: RedisClient, sensor_id: int) -> schemas.SensorData:
    db_data = redis.get(sensor_id)

    if db_data is None:
        raise HTTPException(status_code=404, detail="Sensor data not found")

    return json.loads(db_data)

def delete_sensor(db: Session, sensor_id: int, mongodb: MongoDBClient, redis: RedisClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    mongodb.deleteDoc(db_sensor.name)
    redis.delete(sensor_id)
    return db_sensor

def get_sensors_near(mongodb: MongoDBClient, latitude: float, longitude: float, radius: float, redis: RedisClient, db: Session):
    list_near = []
    query = {"latitude": {"$gte": latitude - radius, "$lte": latitude + radius},
     "longitude": {"$gte": longitude - radius, "$lte": longitude + radius}}

    sensors = mongodb.collection.find(query)
    for sensor in sensors:
        db_sensor = get_sensor_by_name(db, sensor['name'])
        db_sensor_data = get_data(redis, db_sensor.id)

        # We make the same that we do it in get_data
        db_data = {
            'id': db_sensor.id,
            'name': db_sensor.name
        }
        db_data.update(db_sensor_data)

        list_near.append(db_data)

    return list_near


