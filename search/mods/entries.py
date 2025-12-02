from typed import Dict, List, Str, Any
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
    """
    Traverse a tree shaped as:

        idx1 -> idx2 -> ... -> idxN -> entity

    `index_specs` is an ordered list [{'name': 'site', ...}, ...].
    `index_filters` is a dict { 'site': value, 'kind': value, ... }.
    """
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

def _all_entries(schema: Schema, json_data: Dict, filters: Filters) -> List(Dict):
    """
    Flatten `json_data` according to `schema`, applying only *index* filters.

    Returns a list of records like:

      {
        <index1>: value,
        <index2>: value,
        ...,
        <field1>: value1,
        <field2>: value2,
        ...,
        "entity": full_original_entity_dict,
      }
    """
    index_specs = _index_specs(schema)
    index_filters = _index_filters(schema, filters)
    field_specs = _field_specs(schema.fields.__class__)

    results = []

    for index_values, entity in _iter(json_data, index_specs, index_filters):
        if not isinstance(entity, dict):
            continue

        record = dict(index_values)

        for fname, spec in field_specs.items():
            value = _get_in(entity, spec["path"], spec["default"])
            record[fname] = value

        record["entity"] = entity
        results.append(record)

    return results

def _apply_filters(entries: List(Dict), schema: Schema, filters: Filters) -> List(Dict):
    if filters is None:
        return entries

    filter_cls = filters.__class__
    filter_attrs = filter_cls.attrs
    index_cls = schema.indexes.__class__
    index_names = set(index_cls.keys())

    def _norm(x):
        if x is None:
            return None
        return str(x).strip().lower()

    for name, meta in filter_attrs.items():
        if name in index_names:
            continue  # those are index filters, already applied in _all_entries
        fval = getattr(filters, name, None)
        if fval is None:
            continue

        target = _norm(fval)
        entries = [e for e in entries if _norm(e.get(name)) == target]

    return entries

def _filtered_entries(schema: Schema, json_data: Dict, filters: Filters) -> List(Dict):
    """
    1. Flatten json_data according to schema (applying index filters).
    2. Apply non-index filters from `filters`.
    """
    entries = _all_entries(schema=schema, json_data=json_data, filters=filters)
    entries = _apply_filters(entries, schema, filters)
    return entries
