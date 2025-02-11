import pkgutil
import sys
from importlib import import_module
from os import path, sep
from typing import Any, ClassVar, ForwardRef, List, Optional, Type, TypeVar, get_origin

T = TypeVar("T")


def lazy_import(*path_to_imports: str) -> Any:
    result = None

    for import_path in path_to_imports:
        module_name, obj_name = import_path.rsplit(".", 1)
        module = import_module(module_name)

        try:
            return getattr(module, obj_name)
        except (AttributeError, ImportError):
            pass

    if result is None:
        raise ImportError(f"Invalid import paths: {','.join(path_to_imports)} ")


def check_classvar(v: Optional[Type[Any]]) -> bool:
    if v is None:
        return False

    return v.__class__ == ClassVar.__class__ and getattr(v, "_name", None) == "ClassVar"


def is_classvar(ann_type: Type[Any]) -> bool:
    if check_classvar(ann_type) or check_classvar(get_origin(ann_type)):
        return True

    # this is an ugly workaround for class vars that contain forward references and are therefore themselves
    # forward references, see #3679
    if ann_type.__class__ == ForwardRef and ann_type.__forward_arg__.startswith("ClassVar["):
        return True

    return False


def import_by_sources(package_path: str, file_tpl: str, root_prefix: Optional[str] = None) -> None:
    imported_modules_files = {
        file_path for item in sys.modules.values() if (file_path := getattr(item, "__file__", None)) is not None
    }
    full_prefix = f"{root_prefix or ''}{'.' if root_prefix else ''}{path.normpath(package_path).split(sep)[-1]}"

    for package in pkgutil.iter_modules([package_path]):
        current_path = path.join(package_path, package.name)

        if package.ispkg:
            import_by_sources(current_path, file_tpl, root_prefix=full_prefix)
        elif package.name == file_tpl:
            module_name = f"{full_prefix}.{file_tpl}"
            module_file_path = f"{current_path}.py"

            # avoid double import the same file
            if module_name in sys.modules or module_file_path in imported_modules_files:
                continue

            import_module(module_name)


def get_all_subclasses(klass: Type[T]) -> List[Type[T]]:
    all_subclasses = []

    for subclass in klass.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))

    return all_subclasses
