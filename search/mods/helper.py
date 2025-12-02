from typed import Str, List, Union

def _normalize_queries(query: Union(Str, List(Str))) -> List(Str):
    if isinstance(query, (list, tuple)):
        return [str(x).strip().lower() for x in query if x]
    if query:
        return [str(query).strip().lower()]
    return []
