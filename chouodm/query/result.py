from typing import List, Generator, Any, Union, Tuple, TYPE_CHECKING


if TYPE_CHECKING:
    from ..document import Document


class FindResult(object):
    __slots__ = ('_data', 'document_class')

    def __init__(
        self,
        document_class: 'Document',
        data: list,
    ):
        self._data = data
        self.document_class = document_class

    # @handle_and_convert_connection_errors
    def __iter__(self):
        for obj in self._data:
            yield obj

    def __next__(self):
        return next(self.__iter__())

    @property
    def data(self) -> List:
        return [obj.data for obj in self.__iter__()]

    @property
    def generator(self) -> Generator:
        return self.__iter__()

    @property
    def data_generator(self) -> Generator:
        for obj in self.__iter__():
            yield obj.data

    @property
    def list(self) -> List:
        return list(self.__iter__())

    def json(self) -> str:
        return self.document_class.__encoder__.encode(self.data).decode()

    def first(self) -> Any:
        return next(self.__iter__())

    def serialize(
        self, fields: Union[Tuple, List], to_list: bool = True
    ) -> Union[Tuple, List]:
        return (
            [obj.serialize(fields) for obj in self.__iter__()]
            if to_list
            else tuple(obj.serialize(fields) for obj in self.__iter__())
        )

    def serialize_generator(self, fields: Union[Tuple, List]) -> Generator:
        for obj in self.__iter__():
            yield obj.serialize(fields)

    def serialize_json(self, fields: Union[Tuple, List]) -> str:
        return self.document_class.__encoder__.encode(self.serialize(fields)).decode()


class SimpleAggregateResult(object):
    __slots__ = ('_data', 'document_class')

    def __init__(
        self,
        document_class: 'Document',
        data: dict,
    ):
        self._data = data
        self.document_class = document_class

    def json(self) -> str:
        return self.document_class.__encoder__.encode(self._data).decode()

    @property
    def data(self) -> dict:
        return self._data
