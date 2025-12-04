from typed import model, Maybe
from search.mods.helper import _ensure_extends, _ensure_no_defaults
from search.mods.models import Indexes, Fields, Schema, Filters

def indexes(_cls=None):
    def wrap(cls):
        _ensure_extends(cls, Indexes, "Indexes")
        _ensure_no_defaults(cls, "Indexes")
        indexes_model = model(cls)
        indexes_model.is_indexes = True
        return indexes_model

    if _cls is None:
        return wrap
    else:
        return wrap(_cls)

def fields(_cls=None):
    def wrap(cls):
        _ensure_extends(cls, Fields, "Fields")
        _ensure_no_defaults(cls, "Fields")
        fields_model = model(cls)
        fields_model.is_fields = True
        return fields_model

    if _cls is None:
        return wrap
    else:
        return wrap(_cls)

def filters(*, schema: Schema):
    idx_model = schema.indexes
    if not (isinstance(idx_model, type) and hasattr(idx_model, 'attrs')):
        idx_model = idx_model.__class__

    fld_model = schema.fields
    if not (isinstance(fld_model, type) and hasattr(fld_model, 'attrs')):
        fld_model = fld_model.__class__

    schema_attr_types = {}

    for name, meta in getattr(idx_model, 'attrs', {}).items():
        schema_attr_types[name] = meta["type"]

    for name, meta in getattr(fld_model, 'attrs', {}).items():
        if name not in schema_attr_types:
            schema_attr_types[name] = meta["type"]

    def decorator(cls):
        _ensure_extends(cls, Filters, "Filters")
        annotations = getattr(cls, '__annotations__', {})

        for name, ann_type in annotations.items():
            if name not in schema_attr_types:
                raise TypeError(
                    f"Filter model '{cls.__name__}' has attribute '{name}' "
                    f"which is not present in schema indexes or fields."
                )

            base_type = schema_attr_types[name]

            if ann_type == base_type:
                continue

            maybe_of_base = Maybe(base_type)
            if ann_type == maybe_of_base:
                continue

            raise TypeError(
                f"Filter model '{cls.__name__}' attribute '{name}' has type "
                f"'{ann_type}', but expected '{base_type}' or 'Maybe({base_type})'."
            )
        model_filters = model(cls)
        setattr(model_filters, 'is_filters', True)
        return model_filters
    return decorator
