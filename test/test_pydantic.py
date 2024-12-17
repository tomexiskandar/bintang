from datetime import datetime
from pydantic import BaseModel, PositiveInt, ValidationError

class User(BaseModel):
    id: int
    name: str = 'John doe'
    signup_ts: datetime | None
    tastes: dict[str, PositiveInt]

external_data = {
    'id': 'not 123',
    'signup_ts': '2019-07-01 12:22',
    'tastes': {
        'wine': 9,
        b'cheese': 7,
        'cabbage':'1'
    }
}

try:
    user = User(**external_data) # note *=unpack list, **=unpack dict
except ValidationError as e:
    print(e.errors())
# print(user.id)


