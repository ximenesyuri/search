from typed import Dict, List, Str, Any, Maybe
from utils.types import Nat
from search.mods.models import Schema, Filters
from search.mods.fields import _field_specs
from search.mods.indexes import _index_filters, _index_specs


def _get_in(entity: Dict, path: List(Str), default: Any):
    cur = entity
    for p in path:
        if not isinstance(cur, dict):
            return default
        if p not in cur:
            return default
        cur = cur[p]
        if cur is None:
            return default
    return cur


def _iter(root: Dict, index_specs: List(Dict), index_filters: Dict):
    if not index_specs:
        yield {}, root
        return

    def _recurse(node: Any, depth: Nat, acc: Dict):
        if depth == len(index_specs):
            if isinstance(node, dict):
                yield acc, node
            return

        if not isinstance(node, dict):
            return

        spec = index_specs[depth]
        iname = spec["name"]
        fval = index_filters.get(iname)

        for key, child in node.items():
            if fval is not None and str(key) != str(fval):
                continue
            new_acc = dict(acc)
            new_acc[iname] = key
            yield from _recurse(child, depth + 1, new_acc)

    yield from _recurse(root, 0, {})


def _all_entries(schema: Schema, json_data: Dict, filters: Maybe(Filters)) -> List(Dict):
    index_specs = _index_specs(schema)
    index_filters = _index_filters(schema, filters)

    root_key = schema.root
    root_data = json_data.get(root_key, {})

    fields_model_cls = schema.fields
    if not isinstance(fields_model_cls, type) and hasattr(fields_model_cls, '__class__'):
        fields_model_cls = fields_model_cls.__class__

    field_specs = _field_specs(fields_model_cls)

    results = []

    for index_values, entity in _iter(root_data, index_specs, index_filters):
        if not isinstance(entity, dict):
            continue

        record = dict(index_values)

        for fname, spec in field_specs.items():
            value = _get_in(entity, spec["path"], spec["default"])
            record[fname] = value

        results.append(record)

    return results


def _apply_filters(entries: List(Dict), schema: Schema, filters: Maybe(Filters)) -> List(Dict):
    if filters is None:
        return entries

    filter_cls = filters.__class__
    filter_attrs = filter_cls.attrs

    idx_cls = schema.indexes
    if not (isinstance(idx_cls, type) and hasattr(idx_cls, 'keys')):
        idx_cls = idx_cls.__class__
    index_names = set(idx_cls.keys())

    field_name_map = getattr(filter_cls, '_field_name_map', {})

    def _norm(x):
        if x is None:
            return None
        return str(x).strip().lower()

    for name, meta in filter_attrs.items():
        if name in index_names:
            continue

        fval = getattr(filters, name, None)
        if fval is None:
            continue

        target = _norm(fval)
        field_key = field_name_map.get(name, name)

        entries = [e for e in entries if _norm(e.get(field_key)) == target]

    return entries

def _filtered_entries(schema: Schema, json_data: Dict, filters: Maybe(Filters)) -> List(Dict):
    entries = _all_entries(schema=schema, json_data=json_data, filters=filters)
    entries = _apply_filters(entries, schema, filters)
    return entries
