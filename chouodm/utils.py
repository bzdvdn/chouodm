from typing import Union, List, Tuple, Generator


def chunk_by_length(items: Union[List, Tuple], step: int) -> Generator:
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(items), step):
        yield items[i : i + step]
