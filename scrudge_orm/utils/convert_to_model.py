from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type, TypeVar

from scrudge_orm.utils.typehint import find_annotation

if TYPE_CHECKING:
    from scrudge_orm.models.base import DatabaseModelTypeVar

T = TypeVar("T")


def convert_to_model(
    src_model: Type["DatabaseModelTypeVar"], dst_model: Type[T], skip_fields: Optional[Iterable[str]] = None
) -> Type[T]:
    skip_fields = set(skip_fields) if skip_fields is not None else set()

    annotations = {}

    for field_name in src_model.scrudge_db_fields:
        if field_name in skip_fields:
            continue
        else:
            annotations[field_name] = find_annotation(field_name, src_model)

    attrs: Dict[str, Any] = {"__annotations__": annotations}

    from scrudge_orm.serialization.base_serializers import BaseModelSerializer

    if issubclass(dst_model, BaseModelSerializer):
        attrs["__model__"] = src_model

    return type(f"{src_model.__name__}{dst_model.__name__}", (dst_model,), attrs)
