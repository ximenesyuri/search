import re
import difflib
from typed import typed, List, Dict, Str, Bool, Maybe, Union, Regex, Range
from utils.types import Nat
from search.mods.models import Filters, Schema
from search.mods.entries import _filtered_entries
from search.mods.indexes import _index_specs

QUERY_PATTERN = r'^(?:\s*(?:\(|\)|AND|OR|NOT|[^()\s]+)\s*)+$'
Query = Regex(QUERY_PATTERN)

QUERY_RE = re.compile(QUERY_PATTERN, re.VERBOSE)
TOKEN_RE = re.compile(r'\s*(\(|\)|AND|OR|NOT|[^()\s]+)\s*')

def _insert_implicit_and(tokens):
    def _is_term(tok):
        return tok not in ("AND", "OR", "NOT", "(", ")")

    new_tokens = []
    prev = None

    for tok in tokens:
        if prev is not None:
            if (( _is_term(prev) or prev == ")" ) and
                ( _is_term(tok)  or tok in ("(", "NOT") )):
                new_tokens.append("AND")
        new_tokens.append(tok)
        prev = tok

    return new_tokens

def _similarity_threshold(temp):
    t = max(0, min(100, temp))
    return 0.9 - 0.8 * (t / 100.0)


def _targets_match_term(targets, term, fuzzy, exact, temp):
    term = str(term).strip().lower()
    if not term:
        return False

    norm_targets = [
        str(t).strip().lower()
        for t in targets
        if t is not None
    ]
    if not norm_targets:
        return False

    if not fuzzy:
        if exact:
            return term in norm_targets
        return any(term in t for t in norm_targets)

    threshold = _similarity_threshold(temp)
    best = 0.0
    for t in norm_targets:
        score = difflib.SequenceMatcher(None, term, t).ratio()
        if score > best:
            best = score
    return best >= threshold


class _QueryParser:
    def __init__(self, tokens, fuzzy, exact, temp, get_targets):
        self.tokens = tokens
        self.pos = 0
        self.fuzzy = fuzzy
        self.exact = exact
        self.temp = temp
        self.get_targets = get_targets

    def _peek(self):
        if self.pos >= len(self.tokens):
            return None
        return self.tokens[self.pos]

    def _consume(self, expected=None):
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of query")
        if expected is not None and tok != expected:
            raise ValueError(f"Expected '{expected}' but got '{tok}'")
        self.pos += 1
        return tok

    def parse(self):
        if not self.tokens:
            return lambda entry: False
        expr = self._parse_expr()
        if self._peek() is not None:
            raise ValueError(f"Unexpected token: {self._peek()!r}")
        return expr

    def _parse_expr(self):
        """
        expr := term (OR term)*
        """
        left = self._parse_term()
        while self._peek() == "OR":
            self._consume("OR")
            right = self._parse_term()
            left_func = left
            right_func = right
            left = lambda entry, lf=left_func, rf=right_func: lf(entry) or rf(entry)
        return left

    def _parse_term(self):
        """
        term := factor (AND factor)*
        """
        left = self._parse_factor()
        while self._peek() == "AND":
            self._consume("AND")
            right = self._parse_factor()
            left_func = left
            right_func = right
            left = lambda entry, lf=left_func, rf=right_func: lf(entry) and rf(entry)
        return left

    def _parse_factor(self):
        """
        factor := NOT factor | primary
        """
        if self._peek() == "NOT":
            self._consume("NOT")
            inner = self._parse_factor()
            inner_func = inner
            return lambda entry, f=inner_func: not f(entry)
        return self._parse_primary()

    def _parse_primary(self):
        """
        primary := TERM | '(' expr ')'
        """
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of query inside expression")

        if tok == "(":
            self._consume("(")
            expr = self._parse_expr()
            if self._peek() != ")":
                raise ValueError("Missing closing parenthesis")
            self._consume(")")
            return expr

        if tok in ("AND", "OR", "NOT", ")"):
            raise ValueError(f"Unexpected token {tok!r} where a term was expected")

        term = self._consume()
        def _pred(entry, t=term, fuzzy=self.fuzzy, exact=self.exact, temp=self.temp, get_targets=self.get_targets):
            targets = get_targets(entry)
            return _targets_match_term(targets, t, fuzzy=fuzzy, exact=exact, temp=temp)
        return _pred


def _build_query_predicate(query, fuzzy, exact, temp, get_targets):
    q = str(query or "").strip()
    if not q:
        return lambda entry: False

    if not QUERY_RE.match(q):
        raise ValueError(f"Invalid query syntax: {q!r}")

    tokens = TOKEN_RE.findall(q)

    tokens = _insert_implicit_and(tokens)

    parser = _QueryParser(tokens, fuzzy=fuzzy, exact=exact, temp=temp, get_targets=get_targets)
    return parser.parse()

def _reshape_entry(entry, schema):
    """
    Convert a flat entry:
      { "id": ..., "title": ..., "publisher.name": ... }

    into:
      {
        "root": "<root>",
        "indexes": { "id": ... },
        "fields":  { "title": ..., "publisher.name": ... }
      }
    """
    index_names = [spec["name"] for spec in _index_specs(schema)]
    indexes = {}
    fields = {}
    for k, v in entry.items():
        if k in index_names:
            indexes[k] = v
        else:
            fields[k] = v

    return {
        "root": str(schema.root),
        "indexes": indexes,
        "fields": fields,
    }

def _get_targets_auto(e, field):
    val = e.get(field, None)
    if val is None:
        return []
    if isinstance(val, (list, tuple)):
        return list(val)
    return [val]

@typed
def search(
    json_data: Dict,
    fields: Union(Str, List(Str)),
    query: Query,
    schema: Schema,
    filters_model: Maybe(Filters),
    fuzzy: Bool=False,
    max_results: Nat=5,
    exact: Bool=False,
    temp: Range(0, 100)=80,
    **filters_kwargs: Dict
) -> Union(List(Dict), Dict):

    if filters_model is None:
        from search.mods.decorators import filters
        @filters(schema=schema)
        class NullFilters(Filters): pass

        filters_model = NullFilters

    if filters_model and not getattr(filters_model, 'is_filters', False):
        raise TypeError(
            f"The model 'filters_model' of type '{filters_model.__display__}' "
            f"was not created from the '@filters' decorator."
        )

    if isinstance(query, (list, tuple)):
        query_str = " OR ".join(str(q) for q in query if q)
    else:
        query_str = str(query or "")

    if isinstance(fields, (list, tuple)):
        results_by_field: Dict = {}
        for f in fields:
            field_results = search(
                json_data=json_data,
                fields=str(f),
                query=query_str,
                schema=schema,
                filters_model=filters_model,
                fuzzy=fuzzy,
                max_results=max_results,
                exact=exact,
                temp=temp,
                **filters_kwargs
            )
            results_by_field[str(f)] = field_results
        return results_by_field

    query_str = query_str.strip()
    if not query_str:
        return []

    entries = _filtered_entries(
        schema=schema,
        json_data=json_data,
        filters=filters_model(**filters_kwargs),
    )

    get_targets = lambda e, field=fields: _get_targets_auto(e, field)

    predicate = _build_query_predicate(
        query=query_str,
        fuzzy=fuzzy,
        exact=exact,
        temp=temp,
        get_targets=get_targets,
    )

    matched = []
    for e in entries:
        if predicate(e):
            matched.append(e)
            if len(matched) >= max_results:
                break

    return [_reshape_entry(e, schema) for e in matched]

