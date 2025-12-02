from typed import List, Dict, Any, Str, Nat
from search.mods.models import Schema, Filters

def _index_specs(schema: Schema) -> List(Dict):
    """
    Ordered list of index specs from schema.indexes.attrs.
    """
    idx_model = schema.indexes
    idx_cls = idx_model.__class__
    names = list(idx_cls.keys())   # ordered keys
    attrs = idx_cls.attrs          # name -> {'type', 'optional', 'default'}

    specs = []
    for name in names:
        meta = attrs[name]
        specs.append({
            "name":     name,
            "type":     meta["type"],
            "optional": meta["optional"],
            "default":  meta["default"],
        })
    return specs

def _index_filters(schema: Schema, filters: Filters) -> Dict:
    """
    Use the schema's index definitions and a Filters instance to build
    {index_name: value} for non-None index filters.
    """
    idx_model = schema.indexes
    idx_cls = idx_model.__class__
    names = list(idx_cls.keys())  # index names

    # start from defaults from schema.indexes
    base = {}
    for name in names:
        base[name] = getattr(idx_model, name)

    # override from Filters instance (which extends Indexes) if provided
    if filters is not None:
        for name in names:
            if hasattr(filters, name):
                base[name] = getattr(filters, name)

    return {name: val for name, val in base.items() if val is not None}
