# search/mods/unflat.py

from typing import Any, Dict, List, Union

from search.mods.indexes import _index_specs
from search.mods.models import Schema
from search.mods.sql import SCHEMA_REGISTRY


def _unflatten_fields(flat_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Turn {"publisher.city": "London", "title": "1984"} into
    {"publisher": {"city": "London"}, "title": "1984"}.

    For keys like "<other_root>.indexes.<id>" we skip them here; they are only
    used as index metadata for additional roots, not as nested fields.
    """
    result: Dict[str, Any] = {}
    for key, value in flat_fields.items():
        parts = key.split(".")

        # Skip "<root>.indexes.<id>" as nested fields; these are used separately
        if len(parts) >= 3 and parts[1] == "indexes" and parts[0] in SCHEMA_REGISTRY:
            continue

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
          "fields":  { <field1>: ..., <field2>: ... },
          "_all_fields": { ... }   # optional, for SQL joins
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
          },
          "<other_root>": {
            "<idx_value>": { ... }   # built from joined data (SQL)
          },
          ...
        }
    """
    result: Dict[str, Any] = {}

    for rec in records:
        root = str(rec.get("root"))
        indexes = rec.get("indexes", {}) or {}
        flat_fields_view = rec.get("fields", {}) or {}
        # For SQL join rows we also have full flattened fields:
        all_flat_fields = rec.get("_all_fields") or flat_fields_view

        if not root:
            continue

        # --------------------
        # 1) Main root (search/sql primary root)
        # --------------------
        schema: Schema | None = SCHEMA_REGISTRY.get(root)
        if schema is not None:
            index_order = [spec["name"] for spec in _index_specs(schema)]
        else:
            index_order = list(indexes.keys())

        root_obj = result.setdefault(root, {})

        # Traverse/create index path for main root
        node = root_obj
        for idx_name in index_order:
            if idx_name not in indexes:
                continue
            idx_value = str(indexes[idx_name])
            if idx_value not in node or not isinstance(node[idx_value], dict):
                node[idx_value] = {}
            node = node[idx_value]

        # Filter out fields that belong to *other* roots for the main root.
        # E.g. drop "movies.director" when root == "books".
        primary_fields_flat: Dict[str, Any] = {}
        for k, v in flat_fields_view.items():
            first = k.split(".", 1)[0]
            if first in SCHEMA_REGISTRY and first != root:
                # This is a joined root's field; it should not appear under the main root
                continue
            primary_fields_flat[k] = v

        # Unflatten only the main-root-visible fields
        fields_unflat = _unflatten_fields(primary_fields_flat)

        # Merge into leaf node
        if isinstance(node, dict):
            _deep_merge(node, fields_unflat)

        # --------------------
        # 2) Additional roots from joined data (SQL JOIN/CROSS)
        #    We inspect:
        #      - _all_fields to find "<other_root>.indexes.<id>" for indexes
        #      - fields (selected fields) to find "<other_root>.<field>"
        # --------------------
        for other_root, other_schema in SCHEMA_REGISTRY.items():
            if other_root == root:
                continue  # skip primary root in this pass

            other_indexes: Dict[str, Any] = {}
            other_fields_flat: Dict[str, Any] = {}

            prefix_idx = other_root + ".indexes."
            prefix_fld = other_root + "."

            # 2a) indexes for other_root from all_flat_fields
            for k, v in all_flat_fields.items():
                if k.startswith(prefix_idx):
                    # e.g. "movies.indexes.id" -> idx_name = "id"
                    idx_name = k[len(prefix_idx):]
                    other_indexes[idx_name] = v

            # 2b) fields for other_root from the *selected* fields
            for k, v in flat_fields_view.items():
                if k.startswith(prefix_fld):
                    # e.g. "movies.director" -> "director"
                    sub = k[len(prefix_fld):]
                    # Skip "indexes.<id>" just in case; we only want real fields here
                    if sub.startswith("indexes."):
                        continue
                    other_fields_flat[sub] = v

            if not other_indexes and not other_fields_flat:
                # No data for this other root in this record
                continue

            # Build/merge the other_root tree
            other_root_obj = result.setdefault(other_root, {})

            if isinstance(other_schema, Schema):
                idx_order_other = [spec["name"] for spec in _index_specs(other_schema)]
            else:
                idx_order_other = list(other_indexes.keys())

            node_other = other_root_obj
            for idx_name in idx_order_other:
                if idx_name not in other_indexes:
                    continue
                idx_value = str(other_indexes[idx_name])
                if idx_value not in node_other or not isinstance(node_other[idx_value], dict):
                    node_other[idx_value] = {}
                node_other = node_other[idx_value]

            # Unflatten other root's fields ("title", "studio.city", ...)
            other_unflat = _unflatten_fields(other_fields_flat)
            if isinstance(node_other, dict):
                _deep_merge(node_other, other_unflat)

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
       unflatten it into a nested JSON, possibly with multiple roots.

    2) If `results` is a dict mapping field -> list of records
       (from search(..., fields=[...])), unflatten each list separately.
    """
    if isinstance(results, dict):
        out: Dict[str, Dict[str, Any]] = {}
        for field_name, recs in results.items():
            if isinstance(recs, list):
                out[field_name] = _unflat_records(recs)
        return out
    else:
        return _unflat_records(list(results))

