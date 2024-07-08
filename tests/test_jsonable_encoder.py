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

# ideally would like to serialise sets in a sorted way to avoid too many diffs for data in git
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
    print(type(setmodel.relations))
    assert jsonable_encoder(setmodel, custom_encoder={
        Dict[str, Set[str]]: lambda d: "compdict",
        dict: lambda d: "okdict",
        set: lambda v: sorted(v),
        int: lambda x: "bla"+str(x)
    }) == {'relations': {'mentions': [2,12,22,52]}, 'index': 'bla1'}
