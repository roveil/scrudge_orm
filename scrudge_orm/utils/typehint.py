from typing import Any, Dict, ForwardRef, Iterable, Optional, Type, Union, get_args, get_origin


def is_optional(field: Any) -> bool:
    return get_origin(field) is Union and type(None) in get_args(field)


def is_forward_ref(field: Any) -> bool:
    return isinstance(field, ForwardRef) or any(filter(lambda obj: isinstance(obj, ForwardRef), get_args(field)))


def get_cls_from_annotation(field_annotation: Any, cls_parent_to_search: Type) -> Optional[Type]:
    is_union = get_origin(field_annotation) is Union
    result = None

    if is_union:
        for annotation in get_args(field_annotation):
            result = get_cls_from_annotation(annotation, cls_parent_to_search)

            if result is not None:
                break
    elif isinstance(field_annotation, Iterable):
        for element in get_args(field_annotation):
            result = get_cls_from_annotation(element, cls_parent_to_search)

            if result is not None:
                break
    elif isinstance(field_annotation, type) and issubclass(field_annotation, cls_parent_to_search):
        result = field_annotation

    return result


def is_any_of(field_annotation: Any, of_field: Any) -> bool:
    return get_cls_from_annotation(field_annotation, of_field) is not None


def find_annotation(attr_name: str, cls: Type) -> Any:
    annotation = None

    for klass in cls.__mro__:
        if hasattr(klass, "__annotations__"):
            annotation = klass.__annotations__.get(attr_name)

        if annotation is not None:
            break

    return annotation


def find_annotation_on_class_creation(attr_name: str, attrs: Dict, bases: Iterable[Type]) -> Any:
    if "__annotations__" in attrs and (annotation := attrs["__annotations__"].get(attr_name)) is not None:
        return annotation

    for base_cls in bases:
        annotation = find_annotation(attr_name, base_cls)

        if annotation is not None:
            return annotation
