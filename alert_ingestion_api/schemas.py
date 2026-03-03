from pydantic import BaseModel

class Alert(BaseModel):
    incident_id: str
    title: str
    service: str
    description: str