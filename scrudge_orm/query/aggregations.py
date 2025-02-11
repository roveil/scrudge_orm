from typing import Optional


class Aggregation:
    default_label: str
    sql_func: str

    def __init__(self, field: str, label: Optional[str] = None):
        self.field = field
        self.label = label or self.default_label


class Sum(Aggregation):
    default_label = "sum"
    sql_func = "sum"


class Count(Aggregation):
    default_label = "count"
    sql_func = "count"


class Max(Aggregation):
    default_label = "max"
    sql_func = "max"


class ArrayAGG(Aggregation):
    default_label = "array_aggregation"
    sql_func = "array_agg"
