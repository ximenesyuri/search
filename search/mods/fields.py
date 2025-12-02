from typed import Tuple, Maybe, Dict
from search.mods.models import Fields

def _field_specs(fields_model_cls: type, prefix: Tuple=(), result: Maybe(Dict)=None) -> Dict:
    """
    Recursively collect leaf fields from a Fields model class:

      {
        field_name: {
          "path":    ["data", "context", "title"],
          "type":    <TYPE>,
          "default": <default_value>,
        },
        ...
      }
    """
    if result is None:
        result = {}

    attrs = fields_model_cls.attrs  # name -> {'type', 'optional', 'default'}

    for name, meta in attrs.items():
        tpe = meta["type"]
        default = meta["default"]
        path = prefix + (name,)

        is_fields_model = getattr(tpe, "is_model", False) and issubclass(tpe, Fields)

        if is_fields_model:
            _field_specs(
                tpe,
                prefix=path,
                result=result,
            )
        else:
            result[name] = {
                "path": list(path),
                "type": tpe,
                "default": default,
            }

    return result
