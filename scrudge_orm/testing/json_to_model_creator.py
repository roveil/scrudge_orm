import json
from datetime import date, datetime
from typing import TYPE_CHECKING, List, Type

from dateutil import parser

if TYPE_CHECKING:
    from scrudge_orm.models.base import DatabaseModelTypeVar


async def json_to_model_bulk_creator(
    schema: Type["DatabaseModelTypeVar"], fixture_path: str
) -> List["DatabaseModelTypeVar"]:
    with open(fixture_path) as f:
        fixture_objects = json.load(f)

    assert isinstance(fixture_objects, list), "List of objects expected"

    annotations = {field_name: field.annotation for field_name, field in schema.model_fields.items()}

    for obj in fixture_objects:
        for k, v in obj.items():
            if (
                (field_cls := annotations.get(k)) is not None
                and isinstance(field_cls, type)
                and issubclass(field_cls, (datetime, date))
                and v is not None
            ):
                obj[k] = dt if ((dt := parser.parse(v)) and issubclass(field_cls, datetime)) else dt.date()

    return await schema.objects.bulk_insert(schema.to_models_bulk(fixture_objects))
