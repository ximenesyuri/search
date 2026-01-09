from search.mods.models import Fields

def _field_specs(fields_model_cls, prefix=(), result=None):
    if result is None:
        result = {}

    attrs = fields_model_cls.attrs

    for name, meta in attrs.items():
        tpe = meta["type"]
        default = meta["default"]

        is_fields_model = getattr(tpe, "is_model", False) and issubclass(tpe, Fields)

        path = prefix + (name,)

        if is_fields_model:
            _field_specs(
                tpe,
                prefix=path,
                result=result,
            )
        else:
            flat_name = ".".join(path)
            result[flat_name] = {
                "path": list(path),
                "type": tpe,
                "default": default,
            }

    return result

