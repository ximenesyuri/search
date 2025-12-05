from typed import List, Dict, Maybe
from search.mods.models import Schema, Filters

def _get_indexes_model(schema: Schema):
    idx = schema.indexes
    if isinstance(idx, type) and hasattr(idx, 'attrs'):
        return idx
    return idx.__class__

def _index_specs(schema: Schema) -> List(Dict):
    idx_cls = _get_indexes_model(schema)
    names = list(idx_cls.keys())
    attrs = idx_cls.attrs

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


def _index_filters(schema: Schema, filters: Maybe(Filters)) -> Dict:
    if filters is None:
        return {}

    idx_cls = _get_indexes_model(schema)
    names = list(idx_cls.keys())

    result: Dict = {}
    for name in names:
        if not hasattr(filters, name):
            continue
        val = getattr(filters, name)
        if val is None:
            continue
        result[name] = val

    return result

