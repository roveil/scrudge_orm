from inspect import getfullargspec
from typing import Any, Callable, Dict


def get_dict_of_function_arguments(
    func: Callable, *args: Any, exclude_self_attributes: bool = True, **kwargs: Any
) -> Dict:
    exclude_attributes = {"self", "cls"} if exclude_self_attributes else set()
    args_as_dict = dict(zip(getfullargspec(func)[0], args, strict=False))
    args_as_dict.update(kwargs)

    return {k: v for k, v in args_as_dict.items() if k not in exclude_attributes}
