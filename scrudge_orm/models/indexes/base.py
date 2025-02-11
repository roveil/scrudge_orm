from typing import Dict, Tuple

from pydantic import BaseModel


class Index(BaseModel):
    fields: Tuple[str, ...]
    name: str

    def get_create_index_kwargs(self) -> Dict:
        return {}


class UniqueIndex(Index):
    def get_create_index_kwargs(self) -> Dict:
        kwargs = super().get_create_index_kwargs()
        kwargs["unique"] = True

        return kwargs
