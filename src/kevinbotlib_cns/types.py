from typing import Union, Mapping, Sequence

JSONPrimitive = Union[str, int, float, bool, None]
JSONType = Union[JSONPrimitive, Mapping[str, "JSONType"], Sequence["JSONType"]]
