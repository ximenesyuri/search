from typed import model, Maybe
from search.mods.helper import _ensure_extends, _ensure_no_defaults
from search.mods.models import Indexes, Fields, Schema, Filters
from search.mods.fields import _field_specs

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

    index_attr_types = {}
    for name, meta in getattr(idx_model, 'attrs', {}).items():
        index_attr_types[name] = meta["type"]

    field_specs = _field_specs(fld_model)
    flat_field_types = {fname: spec["type"] for fname, spec in field_specs.items()}

    alias_candidates = {}
    for flat_name in flat_field_types.keys():
        short = flat_name.split(".")[-1]
        alias_candidates.setdefault(short, []).append(flat_name)

    alias_unique = {
        short: flats[0]
        for short, flats in alias_candidates.items()
        if len(flats) == 1
    }
    alias_ambiguous = {
        short: flats
        for short, flats in alias_candidates.items()
        if len(flats) > 1
    }

    def decorator(cls):
        _ensure_extends(cls, Filters, "Filters")
        annotations = getattr(cls, '__annotations__', {})

        field_name_map = {}

        for name, ann_type in annotations.items():
            if name in index_attr_types:
                base_type = index_attr_types[name]

                if ann_type == base_type:
                    continue
                maybe_of_base = Maybe(base_type)
                if ann_type == maybe_of_base:
                    continue

                raise TypeError(
                    f"Filter model '{cls.__name__}' attribute '{name}' has type "
                    f"'{ann_type}', but expected '{base_type}' or 'Maybe({base_type})'."
                )
                continue

            if name in flat_field_types:
                flat_name = name
                base_type = flat_field_types[flat_name]
            else:
                if name in alias_ambiguous:
                    choices = ", ".join(sorted(alias_ambiguous[name]))
                    raise TypeError(
                        f"Filter model '{cls.__name__}' attribute '{name}' is ambiguous: "
                        f"it matches multiple schema fields [{choices}]. "
                        f"Use one of the fully-qualified names instead."
                    )

                flat_name = alias_unique.get(name)
                if flat_name is None:
                    raise TypeError(
                        f"Filter model '{cls.__name__}' has attribute '{name}' "
                        f"which is not present in schema indexes or fields."
                    )
                base_type = flat_field_types[flat_name]

            if ann_type != base_type and ann_type != Maybe(base_type):
                raise TypeError(
                    f"Filter model '{cls.__name__}' attribute '{name}' has type "
                    f"'{ann_type}', but expected '{base_type}' or 'Maybe({base_type})'."
                )

            field_name_map[name] = flat_name

        model_filters = model(cls)
        setattr(model_filters, 'is_filters', True)
        setattr(model_filters, '_field_name_map', field_name_map)
        return model_filters

    return decorator
