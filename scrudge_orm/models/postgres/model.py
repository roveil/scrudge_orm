from typing import TYPE_CHECKING, ClassVar

from scrudge_orm.managers.postgres import PostgresManager
from scrudge_orm.models.base import DatabaseModel, MetaBase


class PostgresMeta(MetaBase):
    manager_cls = PostgresManager
    is_proxy = False

    __is_inheritance_class: bool = True


class PostgresModel(DatabaseModel):
    if TYPE_CHECKING:
        objects: ClassVar["PostgresManager"]

    class Meta(PostgresMeta):
        is_proxy = True

        __is_inheritance_class: bool = True
