# in search/mods/sql.py

from typing import Any, Dict, List, Union

from search.mods.indexes import _index_specs
from search.mods.models import Schema
from search.mods.sql import SCHEMA_REGISTRY

# already defined somewhere in this module:
# SCHEMA_REGISTRY: Dict[str, Schema]


def _unflatten_fields(flat_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Turn {"publisher.city": "London", "title": "1984"} into
    {"publisher": {"city": "London"}, "title": "1984"}.
    """
    result: Dict[str, Any] = {}
    for key, value in flat_fields.items():
        parts = key.split(".")
        cur = result
        for p in parts[:-1]:
            if p not in cur or not isinstance(cur[p], dict):
                cur[p] = {}
            cur = cur[p]
        cur[parts[-1]] = value
    return result


def _deep_merge(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    """
    Deep-merge src into dst in-place.
    """
    for k, v in src.items():
        if (
            k in dst
            and isinstance(dst[k], dict)
            and isinstance(v, dict)
        ):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v


def _unflat_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Core unflatten logic for a list of records:

        {
          "root": "<root>",
          "indexes": { <index1>: ..., <index2>: ... },
          "fields":  { <field1>: ..., <field2>: ... }
        }

    Returns a nested JSON like:

        {
          "<root>": {
            "<index1_value>": {
              "<index2_value>": {
                ... {
                  <field1>: <value1>,
                  <field2>: <value2>,
                  ...
                }
              }
            }
          }
        }
    """
    result: Dict[str, Any] = {}

    for rec in records:
        root = str(rec.get("root"))
        indexes = rec.get("indexes", {}) or {}
        flat_fields = rec.get("fields", {}) or {}

        if not root:
            continue

        schema: Schema | None = SCHEMA_REGISTRY.get(root)
        if schema is not None:
            index_order = [spec["name"] for spec in _index_specs(schema)]
        else:
            # Fallback: use the order found in this record
            index_order = list(indexes.keys())

        root_obj = result.setdefault(root, {})

        # Traverse/create index path
        node = root_obj
        for idx_name in index_order:
            if idx_name not in indexes:
                # Index missing in this record; skip it
                continue
            idx_value = str(indexes[idx_name])
            if idx_value not in node or not isinstance(node[idx_value], dict):
                node[idx_value] = {}
            node = node[idx_value]

        # Unflatten fields ("publisher.city" -> {"publisher": {"city": ...}})
        fields_unflat = _unflatten_fields(flat_fields)

        # Merge into leaf node
        if isinstance(node, dict):
            _deep_merge(node, fields_unflat)
        else:
            # Defensive fallback
            # overwrite with fields_unflat
            node = fields_unflat

    return result


def unflat(
    results: Union[
        List[Dict[str, Any]],                 # from search(..., fields="title") or sql(...)
        Dict[str, List[Dict[str, Any]]],      # from search(..., fields=["title", "author"])
    ]
) -> Union[
    Dict[str, Any],              # unflattened for single-field / sql
    Dict[str, Dict[str, Any]],   # per-field unflattened for multi-field search
]:
    """
    Polymorphic unflatten:

    1) If `results` is a list (from sql() or search() with a single field),
       unflatten it into a nested JSON:

           [
             {
               "root": "books",
               "indexes": { "id": "book_101" },
               "fields":  { "title": "aaaa", "publisher.city": "London" }
             },
             ...
           ]
       =>
           {
             "books": {
               "book_101": {
                 "title": "aaaa",
                 "publisher": { "city": "London" }
               },
               ...
             }
           }

    2) If `results` is a dict mapping field -> list of records
       (from search(..., fields=[...])), unflatten each list separately:

           {
             "title":  [ ...records for title... ],
             "author": [ ...records for author... ],
           }
       =>
           {
             "title":  { ...nested JSON for title results... },
             "author": { ...nested JSON for author results... }
           }
    """
    if isinstance(results, dict):
        # Dict[field_name -> List[records]]
        out: Dict[str, Dict[str, Any]] = {}
        for field_name, recs in results.items():
            if isinstance(recs, list):
                out[field_name] = _unflat_records(recs)
        return out
    else:
        # Assume list[records]
        return _unflat_records(list(results))
