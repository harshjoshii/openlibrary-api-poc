# Using pydantic for validations and modelling purposes
# Custom validations can also be introduced if needed here
from datetime import datetime
from typing import List
from pydantic import BaseModel

class Author(BaseModel):
    key: str = None

class Type(BaseModel):
    key: str = None

class TimeStamp(BaseModel):
    type: str = None
    value: datetime = None

class AuthorType(BaseModel):
    author : Author 
    type: Type

class Excerpt(BaseModel):
    type: str = None
    value: str = None

class Excerpts(BaseModel):
    excerpt: Excerpt
    page: str = None

class Works(BaseModel):
    key: str = None
    title: str = None
    description: str = None
    type: Type = None
    revision: int = None
    latest_revision: int = None
    first_publish_date: str = None
    api_response_status: str = None

    covers: List[str] = None
    subjects: List[str] = None
    subject_places: List[str] = None
    subject_people: List[str] = None
    subject_times: List[str] = None
    dewey_number: List[str] = None
    lc_classifications: List[str] = None

    authors: List[AuthorType] = None
    excerpts: List[Excerpts] = None

    created: TimeStamp = None
    last_modified: TimeStamp = None