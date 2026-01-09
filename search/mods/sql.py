import re
from typed import typed, Any, Str, Dict, List
from search.mods.entries import _all_entries
from search.mods.indexes import _index_specs
from search.mods.fields import _field_specs
from search.mods.models import Schema
from search.mods.search_ import _reshape_entry

SCHEMA_REGISTRY = {}

def register_schema(schema):
    root = schema.root
    if not isinstance(root, str):
        root = str(root)
    SCHEMA_REGISTRY[root] = schema


_LIT_INT_RE = re.compile(r"^-?\d+$")
_LIT_FLOAT_RE = re.compile(r"^-?\d+\.\d*$")

def _parse_literal(s):
    s = s.strip()
    if not s:
        return s

    if (len(s) >= 2) and ((s[0] == s[-1] == "'") or (s[0] == s[-1] == '"')):
        return s[1:-1]

    upper = s.upper()
    if upper == "TRUE":
        return True
    if upper == "FALSE":
        return False

    if _LIT_INT_RE.fullmatch(s):
        try:
            return int(s)
        except ValueError:
            pass

    if _LIT_FLOAT_RE.fullmatch(s):
        try:
            return float(s)
        except ValueError:
            pass

    return s

_WHERE_TOKEN_RE = re.compile(
    r"""
    \s*(
        \(|\)            |   # parentheses
        AND|OR           |   # boolean ops
        =                |   # equality
        '(?:[^'\\]|\\.)*'|   # single-quoted string
        "(?:[^"\\]|\\.)*"|   # double-quoted string
        -?\d+\.\d+       |   # float
        -?\d+            |   # int
        [A-Za-z_][A-Za-z0-9_.]*  # identifier (field, indexes.id, books.title, movies.indexes.id, etc.)
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _tokenize_where(where):
    where = where.strip()
    if not where:
        return []

    tokens = [m.group(1) for m in _WHERE_TOKEN_RE.finditer(where)]
    if not tokens:
        raise ValueError(f"Invalid WHERE clause: {where!r}")
    return tokens

class _WhereParser:
    def __init__(self, tokens, index_names, primary_root):
        self.tokens = tokens
        self.pos = 0
        self.index_names = set(index_names)
        self.primary_root = primary_root

    def _peek(self):
        if self.pos >= len(self.tokens):
            return None
        return self.tokens[self.pos]

    def _peek_upper(self):
        t = self._peek()
        return t.upper() if t is not None else None

    def _consume(self, expected=None):
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of WHERE clause")
        if expected is not None and tok.upper() != expected.upper():
            raise ValueError(f"Expected {expected!r} but got {tok!r} in WHERE clause")
        self.pos += 1
        return tok

    def _consume_identifier(self):
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of WHERE clause (expected identifier)")
        if tok in ("(", ")", "=", "AND", "OR"):
            raise ValueError(f"Expected identifier but got {tok!r} in WHERE clause")
        self.pos += 1
        return tok

    def _consume_value_token(self):
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of WHERE clause (expected value)")
        if tok.upper() in ("AND", "OR", "(", ")", "="):
            raise ValueError(f"Expected value but got {tok!r} in WHERE clause")
        self.pos += 1
        return tok

    def parse(self):
        if not self.tokens:
            return lambda e: True
        expr = self._parse_expr()
        if self._peek() is not None:
            raise ValueError(f"Unexpected token {self._peek()!r} in WHERE clause")
        return expr

    def _parse_expr(self):
        left = self._parse_term()
        while self._peek_upper() == "OR":
            self._consume("OR")
            right = self._parse_term()
            lf, rf = left, right
            left = lambda e, lf=lf, rf=rf: lf(e) or rf(e)
        return left

    def _parse_term(self):
        left = self._parse_factor()
        while self._peek_upper() == "AND":
            self._consume("AND")
            right = self._parse_factor()
            lf, rf = left, right
            left = lambda e, lf=lf, rf=rf: lf(e) and rf(e)
        return left

    def _parse_factor(self):
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of WHERE clause")

        if tok == "(":
            self._consume("(")
            expr = self._parse_expr()
            if self._peek() != ")":
                raise ValueError("Missing closing ')' in WHERE clause")
            self._consume(")")
            return expr

        return self._parse_condition()

    def _parse_condition(self):
        ident = self._consume_identifier()
        self._consume("=")
        val_tok = self._consume_value_token()
        value = _parse_literal(val_tok)

        field_name: str

        if self.primary_root and ident.startswith(self.primary_root + "."):
            sub = ident[len(self.primary_root) + 1 :]
            ident = sub

        if ident.lower().startswith("indexes."):
            idx_name = ident.split(".", 1)[1]
            if idx_name not in self.index_names:
                raise ValueError(
                    f"Unknown index name {idx_name!r} in WHERE (available: {sorted(self.index_names)})"
                )
            field_name = idx_name
        elif ident in self.index_names:
            field_name = ident
        else:
            field_name = ident

        def _pred(entry, fname=field_name, val=value):
            return entry.get(fname) == val
        return _pred

def _build_where_predicate(where, primary_schema: Schema, primary_root):
    if not where:
        return lambda e: True

    tokens = _tokenize_where(where)
    index_names = [spec["name"] for spec in _index_specs(primary_schema)]
    parser = _WhereParser(tokens, index_names=index_names, primary_root=primary_root)
    return parser.parse()

_JOIN_COND_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_.]*)\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)\s*$")

def _parse_from_root_spec(spec):
    parts = spec.split(".")
    root = parts[0]

    schema = SCHEMA_REGISTRY.get(root)
    if schema is None:
        raise KeyError(
            f"No schema registered for root {root!r}. "
            f"Use register_schema(schema) first."
        )

    index_specs = _index_specs(schema)
    index_names = [s["name"] for s in index_specs]

    if len(parts) > 1:
        expected = index_names[: len(parts) - 1]
        if parts[1:] != expected:
            raise ValueError(
                f"FROM path indexes {parts[1:]} do not match schema indexes {expected}"
            )

    return root, schema, index_names


def _parse_qualified_ident(ident, left_root, right_root, left_schema, right_schema):
    parts = ident.split(".")
    if len(parts) < 2:
        raise ValueError(
            f"JOIN identifiers must be qualified with a root, e.g. books.title, movies.indexes.id; got {ident!r}"
        )

    root = parts[0]
    if root == left_root:
        schema = left_schema
    elif root == right_root:
        schema = right_schema
    else:
        raise ValueError(
            f"Unknown root {root!r} in JOIN condition; expected {left_root!r} or {right_root!r}"
        )

    index_names = {spec["name"] for spec in _index_specs(schema)}

    fields_model_cls = schema.fields
    if not isinstance(fields_model_cls, type) and hasattr(fields_model_cls, "__class__"):
        fields_model_cls = fields_model_cls.__class__
    field_specs = _field_specs(fields_model_cls)

    if len(parts) >= 3 and parts[1] == "indexes":
        idx_name = parts[2]
        if idx_name not in index_names:
            raise ValueError(
                f"Unknown index {idx_name!r} in JOIN condition for root {root!r} "
                f"(available: {sorted(index_names)})"
            )
        return {"root": root, "is_index": True, "name": idx_name}

    name_rest = ".".join(parts[1:])
    if len(parts) == 2 and parts[1] in index_names and name_rest not in field_specs:
        return {"root": root, "is_index": True, "name": parts[1]}

    if name_rest not in field_specs:
        raise ValueError(
            f"Unknown field {name_rest!r} in JOIN condition for root {root!r} "
            f"(available fields: {sorted(field_specs.keys())})"
        )
    return {"root": root, "is_index": False, "name": name_rest}


def _parse_join_on(on_clause, left_root, right_root, left_schema, right_schema):
    if not on_clause:
        raise ValueError("JOIN ... ON clause is required")

    parts = re.split(r"\s+AND\s+", on_clause, flags=re.IGNORECASE)
    conditions = []

    for part in parts:
        part = part.strip()
        if not part:
            continue

        m = _JOIN_COND_RE.match(part)
        if not m:
            raise ValueError(f"Invalid JOIN ON condition: {part!r}")

        ident1, ident2 = m.group(1), m.group(2)
        side1 = _parse_qualified_ident(
            ident1, left_root, right_root, left_schema, right_schema
        )
        side2 = _parse_qualified_ident(
            ident2, left_root, right_root, left_schema, right_schema
        )
        conditions.append((side1, side2))

    if not conditions:
        raise ValueError(f"Empty JOIN ON clause: {on_clause!r}")

    return conditions


def _build_join_predicate(conditions, left_root, right_root):
    def value_for_side(side, left_entry, right_entry):
        root = side["root"]
        name = side["name"]
        if root == left_root:
            src = left_entry
        else:
            src = right_entry
        return src.get(name)

    def _pred(left_entry, right_entry):
        for s1, s2 in conditions:
            v1 = value_for_side(s1, left_entry, right_entry)
            v2 = value_for_side(s2, left_entry, right_entry)
            if v1 != v2:
                return False
        return True

    return _pred


def _combine_join_entries(left_entry, right_entry, right_root, right_schema):
    combined = dict(left_entry)

    right_index_names = {spec["name"] for spec in _index_specs(right_schema)}

    for key, value in right_entry.items():
        if key in right_index_names:
            combined[f"{right_root}.indexes.{key}"] = value
        else:
            combined[f"{right_root}.{key}"] = value

    return combined

_SQL_RE = re.compile(
    r"""
    ^\s*
    SELECT\s+(?P<select>.+?)          # SELECT list
    \s+FROM\s+(?P<from_and_rest>.+?)  # FROM + optional JOIN/WHERE
    \s*;?\s*$
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

@typed
def sql(query: Str, json_data: Dict) -> List(Dict):
    import re

    m = _SQL_RE.match(query)
    if not m:
        raise ValueError(f"Invalid SQL query: {query!r}")

    select_part = m.group("select").strip()
    from_and_rest = m.group("from_and_rest").strip()

    m_where = re.search(r"\bWHERE\b", from_and_rest, flags=re.IGNORECASE)
    if m_where:
        from_join_part = from_and_rest[: m_where.start()].strip()
        where_part = from_and_rest[m_where.end() :].strip()
    else:
        from_join_part = from_and_rest
        where_part = None

    m_join = re.search(
        r"\b(INNER\s+JOIN|CROSS\s+JOIN|CROSS|JOIN)\b",
        from_join_part,
        flags=re.IGNORECASE,
    )
    if not m_join:
        primary_from = from_join_part
        right_from = None
        on_clause = None
        join_type = None
    else:
        join_token = m_join.group(1).upper().strip()
        if join_token.startswith("INNER"):
            join_type = "INNER"
        elif join_token.startswith("CROSS"):
            join_type = "CROSS"
        else:
            join_type = "JOIN"

        primary_from = from_join_part[: m_join.start()].strip()
        rest_join = from_join_part[m_join.end() :].strip()

        m_on = re.search(r"\bON\b", rest_join, flags=re.IGNORECASE)

        if join_type == "CROSS":
            if m_on:
                raise ValueError("CROSS JOIN must not have an ON clause")
            right_from = rest_join.strip()
            on_clause = None
        else:
            if not m_on:
                raise ValueError(f"{join_type} without ON clause is not supported")
            right_from = rest_join[: m_on.start()].strip()
            on_clause = rest_join[m_on.end() :].strip()

    left_root, left_schema, left_index_names = _parse_from_root_spec(primary_from)

    left_fields_model_cls = left_schema.fields
    if not isinstance(left_fields_model_cls, type) and hasattr(left_fields_model_cls, "__class__"):
        left_fields_model_cls = left_fields_model_cls.__class__
    left_field_specs = _field_specs(left_fields_model_cls)
    left_field_names = list(left_field_specs.keys())
    left_field_names_qualified = [f"{left_root}.{n}" for n in left_field_names]

    join_used = right_from is not None

    if not join_used:
        all_field_names_for_star_internal = left_field_names
        allowed_select_names = set(left_field_names) | set(left_field_names_qualified)
        right_root = None
        right_schema = None
        right_field_specs = {}
        right_index_specs = []
    else:
        right_root, right_schema, _right_index_names = _parse_from_root_spec(right_from)

        right_fields_model_cls = right_schema.fields
        if not isinstance(right_fields_model_cls, type) and hasattr(
            right_fields_model_cls, "__class__"
        ):
            right_fields_model_cls = right_fields_model_cls.__class__
        right_field_specs = _field_specs(right_fields_model_cls)
        right_field_names = list(right_field_specs.keys())

        right_field_names_qualified = [f"{right_root}.{name}" for name in right_field_names]

        right_index_specs = _index_specs(right_schema)
        right_index_field_names = [
            f"{right_root}.indexes.{spec['name']}" for spec in right_index_specs
        ]

        all_field_names_for_star_internal = left_field_names + right_field_names_qualified

        allowed_select_names = set()
        allowed_select_names.update(left_field_names)
        allowed_select_names.update(left_field_names_qualified)
        allowed_select_names.update(right_field_names_qualified)
        allowed_select_names.update(right_index_field_names)

    select_raw = [f.strip() for f in select_part.split(",") if f.strip()]
    if len(select_raw) == 1 and select_raw[0] == "*":
        select_internal = list(all_field_names_for_star_internal)
    else:
        select_internal = []
        for name in select_raw:
            if name not in allowed_select_names:
                if not join_used:
                    raise ValueError(
                        f"Unknown field {name!r} in SELECT "
                        f"(available: {sorted(left_field_names_qualified)} or unqualified {sorted(left_field_names)})"
                    )
                else:
                    right_fields_for_msg = (
                        [f"{right_root}.{n}" for n in right_field_specs.keys()]
                        if right_schema is not None
                        else []
                    )
                    right_index_for_msg = (
                        [f"{right_root}.indexes.{spec['name']}" for spec in right_index_specs]
                        if right_schema is not None
                        else []
                    )
                    raise ValueError(
                        f"Unknown field {name!r} in SELECT for JOIN query. "
                        f"Allowed left fields: {sorted(left_field_names_qualified)} (or unqualified {sorted(left_field_names)}); "
                        f"right fields: {sorted(right_fields_for_msg)}; "
                        f"right indexes: {sorted(right_index_for_msg)}."
                    )

            if name.startswith(left_root + "."):
                base = name[len(left_root) + 1 :]
                select_internal.append(base)
            else:
                select_internal.append(name)

    if not join_used:
        entries = _all_entries(schema=left_schema, json_data=json_data, filters=None)

        where_pred = _build_where_predicate(
            where_part, primary_schema=left_schema, primary_root=left_root
        )
        entries = [e for e in entries if where_pred(e)]

    else:
        assert right_root is not None and right_schema is not None

        left_entries = _all_entries(schema=left_schema, json_data=json_data, filters=None)
        right_entries = _all_entries(schema=right_schema, json_data=json_data, filters=None)

        if on_clause:
            conditions = _parse_join_on(
                on_clause=on_clause,
                left_root=left_root,
                right_root=right_root,
                left_schema=left_schema,
                right_schema=right_schema,
            )
            join_pred = _build_join_predicate(
                conditions=conditions,
                left_root=left_root,
                right_root=right_root,
            )
        else:
            def join_pred(_le, _re) -> bool:
                return True

        joined_flat_entries: List[Dict[str, Any]] = []
        for le in left_entries:
            for re in right_entries:
                if join_pred(le, re):
                    combined = _combine_join_entries(
                        left_entry=le,
                        right_entry=re,
                        right_root=right_root,
                        right_schema=right_schema,
                    )
                    joined_flat_entries.append(combined)

        where_pred = _build_where_predicate(
            where_part, primary_schema=left_schema, primary_root=left_root
        )
        entries = [e for e in joined_flat_entries if where_pred(e)]

    results = []
    for e in entries:
        reshaped = _reshape_entry(e, left_schema)
        root_value = reshaped.get("root")
        indexes_dict = reshaped["indexes"]
        fields_dict = reshaped["fields"]

        if select_internal == all_field_names_for_star_internal:
            selected_fields = dict(fields_dict)
        else:
            selected_fields = {f: fields_dict.get(f) for f in select_internal}

        results.append(
            {
                "root": root_value,
                "indexes": indexes_dict,
                "fields": selected_fields,
                "_all_fields": fields_dict
            }
        )

    return results

