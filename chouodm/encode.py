import datetime
from collections import deque
from decimal import Decimal
from enum import Enum
from ipaddress import (
    IPv4Address,
    IPv4Interface,
    IPv4Network,
    IPv6Address,
    IPv6Interface,
    IPv6Network,
)
from pathlib import Path
from re import Pattern
from types import GeneratorType
from typing import Any, Callable, Dict, Type, Union
from uuid import UUID
from bson import ObjectId, decode as bson_decode
from bson.raw_bson import RawBSONDocument


def decimal_encoder(dec_value: Decimal) -> Union[int, float]:
    """
    Encodes a Decimal as int of there's no exponent, otherwise float

    >>> decimal_encoder(Decimal("1.0"))
    1.0

    >>> decimal_encoder(Decimal("1"))
    1
    """
    if dec_value.as_tuple().exponent >= 0:  # type: ignore
        return int(dec_value)
    else:
        return float(dec_value)


def isoformat(o: Union[datetime.date, datetime.time]) -> str:
    return o.isoformat()


ENCODERS_BY_TYPE: Dict[Type[Any], Callable[[Any], Any]] = {
    bytes: lambda o: o.decode(),
    datetime.date: isoformat,
    datetime.datetime: isoformat,
    datetime.time: isoformat,
    datetime.timedelta: lambda td: td.total_seconds(),
    Decimal: decimal_encoder,
    Enum: lambda o: o.value,
    frozenset: list,
    deque: list,
    GeneratorType: list,
    IPv4Address: str,
    IPv4Interface: str,
    IPv4Network: str,
    IPv6Address: str,
    IPv6Interface: str,
    IPv6Network: str,
    Path: str,
    Pattern: lambda o: o.pattern,
    set: list,
    UUID: str,
    ObjectId: str,
    RawBSONDocument: bson_decode,
}


DECODERS_BY_TYPE: Dict[Type[Any], Callable[[Any], Any]] = {
    Relation: decode_to_relation  # type: ignore
}
