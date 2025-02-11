def get_register_model_name(module_name: str, model_name: str) -> str:
    splitted_module_name = tuple(filter(lambda obj: obj and obj != "models", module_name.split(".")))

    return f"{splitted_module_name[-1]}.{model_name}" if splitted_module_name else model_name


def get_table_name_for_class(class_name: str, module: str) -> str:
    class_name_lowered = class_name.split(".")[-1].lower()
    prefix = f"{spl_name[-2]}_" if (spl_name := module.split(".")) and len(spl_name) >= 2 else ""

    return f"{prefix}{class_name_lowered}"


def get_table_name_for_register_name(register_name: str) -> str:
    result = register_name.lower()

    if (name_splitted := register_name.split(".")) and len(name_splitted) > 1:
        result = f"{name_splitted[0]}_{name_splitted[1:]}"

    return result
