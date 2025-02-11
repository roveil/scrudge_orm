from enum import Enum
from functools import lru_cache
from typing import Dict, Optional, Set, Tuple


class BaseEnum(Enum):
    @classmethod
    @lru_cache
    def all(cls) -> Set[Enum]:
        return set(cls)

    @classmethod
    def get_description(cls) -> str:
        return ",".join((f"{item.name}-{item.value}" for item in cls))

    @classmethod
    @lru_cache
    def by_lowered_names_dict(cls) -> Dict[str, "BaseEnum"]:
        return {name.lower(): v for name, v in cls.__members__.items()}

    @classmethod
    def get_by_lowered_name(cls, name: str) -> Optional["BaseEnum"]:
        return cls.by_lowered_names_dict().get(name.lower())


class ChoicesFieldEnum(int, BaseEnum):
    @classmethod
    def get_choices(cls) -> Tuple:
        return tuple((v.value, name) for name, v in cls.__members__.items())
