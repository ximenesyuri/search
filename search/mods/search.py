from typed import List, Dict, Str, Bool, Nat, Union
from search.mods.models import Filters, Schema
from search.mods.helper import _normalize_queries
from search.mods.entries import _filtered_entries

def _non_fuzzy_search(entries: List(Dict), get_targets, queries: List(Str), max_results: Nat, equal: Bool) -> List(Dict):
    results = []
    if not queries:
        return results

    for e in entries:
        targets = [
            str(t).strip().lower()
            for t in get_targets(e)
            if t is not None
        ]
        if not targets:
            continue

        matched = False
        if equal:
            for q in queries:
                if q in targets:
                    matched = True
                    break
        else:
            for t in targets:
                for q in queries:
                    if q in t:
                        matched = True
                        break
                if matched:
                    break

        if matched:
            results.append(e)
            if len(results) >= max_results:
                break

    return results

def _fuzzy_search(entries: List(Dict), get_targets, queries: List(Str), max_results: Nat) -> List(Dict):
    import difflib

    if not queries:
        return []

    scored = []

    for e in entries:
        targets = [
            str(t).strip().lower()
            for t in get_targets(e)
            if t is not None
        ]
        if not targets:
            continue

        best = 0.0
        for t in targets:
            for q in queries:
                score = difflib.SequenceMatcher(None, q, t).ratio()
                if score > best:
                    best = score
        if best > 0.0:
            scored.append((best, e))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [e for _, e in scored[:max_results]]

def search(
    json_data: Dict,
    field: Str,
    query: Union(Str, List(Str)),
    schema: Schema,
    filters: Filters,
    fuzzy: Bool,
    max_results: Nat,
    scalar: Bool,
    equal: Bool,
) -> List(Dict):

    qs = _normalize_queries(query) or []
    entries = _filtered_entries(
        schema=schema,
        json_data=json_data,
        filters=filters,
    )

    if scalar:
        get_targets = lambda e: [e.get(field, "")]
    else:
        get_targets = lambda e: e.get(field, []) or []

    if not fuzzy:
        return _non_fuzzy_search(
            entries=entries,
            get_targets=get_targets,
            queries=qs,
            max_results=max_results,
            equal=equal,
        )

    return _fuzzy_search(
        entries=entries,
        get_targets=get_targets,
        queries=qs,
        max_results=max_results,
    )
