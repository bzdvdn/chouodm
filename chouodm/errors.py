class BaseChouODMError(Exception):
    pass


class QueryValidationError(BaseChouODMError):
    pass


class InvalidArgsParams(BaseChouODMError):
    def __str__(self):
        return "Arguments must be Query objects"


class NotDeclaredField(BaseChouODMError):
    def __init__(self, field_name: str, fields: list, *args):
        self.field_name = field_name
        self.fields = fields
        super().__init__(*args)

    def __str__(self):
        return f"This field - {self.field_name} not declared in {self.fields}"


class ODMIndexError(BaseChouODMError):
    pass


class DocumentDoesNotExist(BaseChouODMError):
    pass
