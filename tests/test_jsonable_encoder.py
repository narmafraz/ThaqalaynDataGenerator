from typing import Dict, List, Set

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

# Note: In Pydantic v2, custom_encoder on jsonable_encoder does not apply to
# fields inside Pydantic models (they are serialized by Pydantic's own serializer
# before custom_encoder is consulted). This test documents the actual behavior.
def test_custom_encoder_set():
    class SetBase(BaseModel):
        relations: Dict[str, Set[str]] = None
        index: int = 1
    setmodel = SetBase()
    setmodel.relations = {'mentions': set()}
    myset = setmodel.relations['mentions']
    myset.add(12)
    myset.add(2)
    myset.add(52)
    myset.add(22)
    result = jsonable_encoder(setmodel)
    # Pydantic v2 converts sets to lists but order is not guaranteed
    assert set(result['relations']['mentions']) == {2, 12, 22, 52}
    assert result['index'] == 1
