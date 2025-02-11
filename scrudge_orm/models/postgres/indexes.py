from typing import Dict, Optional, Tuple

from pydantic import Field

from scrudge_orm.models.indexes.base import Index, UniqueIndex


class PostgresIndex(Index):
    # max_len is 63, but reserve characters for table name and idx postfix
    name: str = Field(..., min_length=1, max_length=40)
    postgresql_concurrently: bool = False
    postgresql_where: Optional[str] = None
    postgresql_using: Optional[str] = None
    postgresql_include: Optional[Tuple[str, ...]] = None
    postgresql_ops: Optional[Dict[str, str]] = None

    def get_create_index_kwargs(self) -> Dict:
        kwargs = super().get_create_index_kwargs()
        kwargs.update(self.model_dump(exclude={"name", "fields"}, exclude_none=True))

        return kwargs


class PostgresUniqueIndex(PostgresIndex, UniqueIndex):
    pass
