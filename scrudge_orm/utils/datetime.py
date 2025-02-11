from datetime import datetime
from typing import Union

from dateutil.parser import ParserError, parse
from pydantic import ValidationError


def parse_datetime(value: Union[datetime, str]) -> datetime:
    if isinstance(value, datetime):
        return value.replace(microsecond=0)

    try:
        result = parse(value).replace(microsecond=0)
    except (TypeError, ParserError) as exc:
        raise ValidationError(exc.args[0]) from exc

    return result


def get_timestamp_in_milliseconds() -> int:
    """
    Returns current timestamp in milliseconds
    :return: value of current timestamp in milliseconds
    """
    return int(datetime.utcnow().timestamp() * 1000)
