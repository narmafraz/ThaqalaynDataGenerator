from typing import List

from fastapi.encoders import jsonable_encoder
from pydantic.main import BaseModel


class YourBaseModel(BaseModel):
    bar: int
    baz: int

class MyBaseModel(BaseModel):
    nest: List[YourBaseModel] = None
    foo: int

model = MyBaseModel(nest=[YourBaseModel(bar=2, baz=3), YourBaseModel(bar=4, baz=5)], foo=1)
exclude_pattern={'nest': {'__all__': {'baz'}}}
expected={"nest":[{"bar": 2}, {"bar": 4}], "foo": 1}


def test_pydantic_nested_exclude():
    assert model.dict(exclude=exclude_pattern) == expected # Passed

def test_jsonable_encoder_nested_exclude():
    assert jsonable_encoder(model, exclude=exclude_pattern) == expected # Failed
    # actual={'foo': 1} ## i.e. removes all nested elements!
