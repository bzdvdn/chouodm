from typing import Type, Any

from .encode import ENCODERS_BY_TYPE, DECODERS_BY_TYPE


def enc_hook(value: Any):
    type_ = type(value)
    if type_ in ENCODERS_BY_TYPE:
        f = ENCODERS_BY_TYPE[type_]
        return f(value)
    else:
        raise TypeError(f"Objects of type {type(value)} are not supported")


def dec_hook(type_: Type, value: Any):
    if type_ in ENCODERS_BY_TYPE:
        return type_(value)
    elif getattr(type_, "__origin__", None) in DECODERS_BY_TYPE:
        f = DECODERS_BY_TYPE[type_.__origin__]  # type: ignore
        return f(value, type_)  # type: ignore
    else:
        raise TypeError(f"Objects of type {type(value)} are not supported")
