from typing import Optional, List, Union, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from pymongo import IndexModel


class BaseConfig(object):
    indexes: Optional[List['IndexModel']] = []
    database_exclude_fields: Optional[Union[List, Tuple]] = tuple()
    colletion_name: Optional[str] = None
