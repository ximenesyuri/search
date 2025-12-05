# search/mods/sql.py

import re
from typing import Any, Dict, List, Tuple, Callable

from search.mods.entries import _all_entries
from search.mods.indexes import _index_specs
from search.mods.fields import _field_specs
from search.mods.models import Schema
from search.mods.search import _reshape_entry  # builds {"root": ..., "indexes": ..., "fields": ...}

# Simple registry to map root name -> Schema
SCHEMA_REGISTRY: Dict[str, Schema] = {}


def register_schema(schema: Schema) -> None:
    """
    Register a Schema so that it can be used in SQL queries.

    The key is schema.root, e.g. "books".
    """
    root = schema.root
    if not isinstance(root, str):
        root = str(root)
    SCHEMA_REGISTRY[root] = schema


# ---------- Literal parsing ----------

_LIT_INT_RE = re.compile(r"^-?\d+$")
_LIT_FLOAT_RE = re.compile(r"^-?\d+\.\d*$")


def _parse_literal(s: str) -> Any:
    s = s.strip()
    if not s:
        return s

    # Quoted string: 'foo' or "foo"
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


# ---------- WHERE parsing (AND / OR) ----------

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


def _tokenize_where(where: str) -> List[str]:
    where = where.strip()
    if not where:
        return []

    tokens = [m.group(1) for m in _WHERE_TOKEN_RE.finditer(where)]
    if not tokens:
        raise ValueError(f"Invalid WHERE clause: {where!r}")
    return tokens


class _WhereParser:
    """
    Parse a WHERE expression with grammar:

        expr   := term (OR term)*
        term   := factor (AND factor)*
        factor := '(' expr ')' | condition
        condition := IDENT '=' LITERAL

    IDENT examples:
      - "title", "publisher.city"                  (primary root fields)
      - "id", "indexes.id"                         (primary root indexes)
      - "books.title", "books.indexes.id"          (qualified primary root)
      - "movies.title", "movies.indexes.id"        (joined root)
    """

    def __init__(self, tokens: List[str], index_names: List[str], primary_root: str | None):
        self.tokens = tokens
        self.pos = 0
        self.index_names = set(index_names)
        self.primary_root = primary_root

    def _peek(self) -> str | None:
        if self.pos >= len(self.tokens):
            return None
        return self.tokens[self.pos]

    def _peek_upper(self) -> str | None:
        t = self._peek()
        return t.upper() if t is not None else None

    def _consume(self, expected: str | None = None) -> str:
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of WHERE clause")
        if expected is not None and tok.upper() != expected.upper():
            raise ValueError(f"Expected {expected!r} but got {tok!r} in WHERE clause")
        self.pos += 1
        return tok

    def _consume_identifier(self) -> str:
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of WHERE clause (expected identifier)")
        if tok in ("(", ")", "=", "AND", "OR"):
            raise ValueError(f"Expected identifier but got {tok!r} in WHERE clause")
        self.pos += 1
        return tok

    def _consume_value_token(self) -> str:
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of WHERE clause (expected value)")
        if tok.upper() in ("AND", "OR", "(", ")", "="):
            raise ValueError(f"Expected value but got {tok!r} in WHERE clause")
        self.pos += 1
        return tok

    def parse(self) -> Callable[[Dict[str, Any]], bool]:
        if not self.tokens:
            return lambda e: True
        expr = self._parse_expr()
        if self._peek() is not None:
            raise ValueError(f"Unexpected token {self._peek()!r} in WHERE clause")
        return expr

    def _parse_expr(self) -> Callable[[Dict[str, Any]], bool]:
        left = self._parse_term()
        while self._peek_upper() == "OR":
            self._consume("OR")
            right = self._parse_term()
            lf, rf = left, right
            left = lambda e, lf=lf, rf=rf: lf(e) or rf(e)
        return left

    def _parse_term(self) -> Callable[[Dict[str, Any]], bool]:
        left = self._parse_factor()
        while self._peek_upper() == "AND":
            self._consume("AND")
            right = self._parse_factor()
            lf, rf = left, right
            left = lambda e, lf=lf, rf=rf: lf(e) and rf(e)
        return left

    def _parse_factor(self) -> Callable[[Dict[str, Any]], bool]:
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

    def _parse_condition(self) -> Callable[[Dict[str, Any]], bool]:
        ident = self._consume_identifier()
        self._consume("=")
        val_tok = self._consume_value_token()
        value = _parse_literal(val_tok)

        field_name: str

        # If qualified with primary root, strip it:
        #   books.publisher.city -> publisher.city
        #   books.indexes.id     -> indexes.id
        if self.primary_root and ident.startswith(self.primary_root + "."):
            sub = ident[len(self.primary_root) + 1 :]
            # Now handle like an unqualified ident
            ident = sub

        # "indexes.id" or bare "id" for primary indexes
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
            # Anything else is a direct key in the flattened entry
            # (e.g. "publisher.city", "movies.title", "movies.indexes.id", ...)
            field_name = ident

        def _pred(entry: Dict[str, Any],
                  fname: str = field_name,
                  val: Any = value) -> bool:
            return entry.get(fname) == val

        return _pred


def _build_where_predicate(
    where: str | None,
    primary_schema: Schema,
    primary_root: str | None,
) -> Callable[[Dict[str, Any]], bool]:
    if not where:
        return lambda e: True

    tokens = _tokenize_where(where)
    index_names = [spec["name"] for spec in _index_specs(primary_schema)]
    parser = _WhereParser(tokens, index_names=index_names, primary_root=primary_root)
    return parser.parse()


# ---------- JOIN parsing ----------

_JOIN_COND_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_.]*)\s*=\s*([A-Za-z_][A-Za-z0-9_.]*)\s*$"
)


def _parse_from_root_spec(spec: str) -> Tuple[str, Schema, List[str]]:
    """
    Parse something like "books" or "books.id" into:

      (root_name, schema_for_root, [index_names_in_order])
    """
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


def _parse_qualified_ident(
    ident: str,
    left_root: str,
    right_root: str,
    left_schema: Schema,
    right_schema: Schema,
) -> Dict[str, Any]:
    """
    Parse a qualified identifier in JOIN ON clause, e.g.:

        books.publisher.city
        books.indexes.id
        movies.title
        movies.indexes.id

    Returns a dict:

        { "root": <root>, "is_index": bool, "name": <flat_key_in_entry> }
    """
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

    # books.indexes.id
    if len(parts) >= 3 and parts[1] == "indexes":
        idx_name = parts[2]
        if idx_name not in index_names:
            raise ValueError(
                f"Unknown index {idx_name!r} in JOIN condition for root {root!r} "
                f"(available: {sorted(index_names)})"
            )
        return {"root": root, "is_index": True, "name": idx_name}

    # books.id   (might be an index or a field)
    name_rest = ".".join(parts[1:])
    if len(parts) == 2 and parts[1] in index_names and name_rest not in field_specs:
        # Prefer index if it's not a defined field with same name
        return {"root": root, "is_index": True, "name": parts[1]}

    # Otherwise, treat as a flattened field name,
    # e.g. "publisher.city", "title"
    if name_rest not in field_specs:
        raise ValueError(
            f"Unknown field {name_rest!r} in JOIN condition for root {root!r} "
            f"(available fields: {sorted(field_specs.keys())})"
        )
    return {"root": root, "is_index": False, "name": name_rest}


def _parse_join_on(
    on_clause: str,
    left_root: str,
    right_root: str,
    left_schema: Schema,
    right_schema: Schema,
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Parse ON clause of the form:

        <identA> = <identB> [AND <identC> = <identD> ...]

    where each ident is fully qualified with a root.
    """
    if not on_clause:
        raise ValueError("JOIN ... ON clause is required")

    parts = re.split(r"\s+AND\s+", on_clause, flags=re.IGNORECASE)
    conditions: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []

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


def _build_join_predicate(
    conditions: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    left_root: str,
    right_root: str,
) -> Callable[[Dict[str, Any], Dict[str, Any]], bool]:
    """
    Build a function (left_entry, right_entry) -> bool
    from the parsed JOIN ON conditions.
    """

    def value_for_side(
        side: Dict[str, Any],
        left_entry: Dict[str, Any],
        right_entry: Dict[str, Any],
    ) -> Any:
        root = side["root"]
        name = side["name"]
        if root == left_root:
            src = left_entry
        else:
            src = right_entry
        return src.get(name)

    def _pred(left_entry: Dict[str, Any], right_entry: Dict[str, Any]) -> bool:
        for s1, s2 in conditions:
            v1 = value_for_side(s1, left_entry, right_entry)
            v2 = value_for_side(s2, left_entry, right_entry)
            if v1 != v2:
                return False
        return True

    return _pred


def _combine_join_entries(
    left_entry: Dict[str, Any],
    right_entry: Dict[str, Any],
    right_root: str,
    right_schema: Schema,
) -> Dict[str, Any]:
    """
    Merge left_entry and right_entry into a single flat entry.

    - Left entry's keys (indexes + fields) are preserved as-is.
    - Right entry's *fields* are added as "right_root.<field_name>".
    - Right entry's *indexes* are added as "right_root.indexes.<index_name>".

    This combined entry is then passed to _reshape_entry with the left schema,
    so that:
      - left indexes -> result["indexes"]
      - everything else (including right_* stuff) -> result["fields"]
    """
    combined = dict(left_entry)

    right_index_names = {spec["name"] for spec in _index_specs(right_schema)}

    for key, value in right_entry.items():
        if key in right_index_names:
            combined[f"{right_root}.indexes.{key}"] = value
        else:
            combined[f"{right_root}.{key}"] = value

    return combined


# ---------- Main SQL entry-point ----------

_SQL_RE = re.compile(
    r"""
    ^\s*
    SELECT\s+(?P<select>.+?)          # SELECT list
    \s+FROM\s+(?P<from_and_rest>.+?)  # FROM + optional JOIN/WHERE
    \s*;?\s*$
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)


def sql(query: str, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Execute a tiny subset of SQL against json_data using registered Schemas.

    Supported syntax (no aliases, no ORDER, no LIMIT):

        SELECT f1, f2, ...
        FROM <root1>[.<idx_path1>]
        [INNER] JOIN <root2>[.<idx_path2>]
            ON <root1>.<field_or_index> = <root2>.<field_or_index> [AND ...]
        [WHERE <bool_expr>]

    Examples:

        SELECT books.title, books.author
        FROM books

        SELECT books.title, books.author, books.publisher.city
        FROM books
        WHERE books.publisher.city = 'London' AND books.available = TRUE

        SELECT books.title, movies.title, movies.studio.city
        FROM books
        INNER JOIN movies
            ON books.publisher.city = movies.studio.city
        WHERE books.available = TRUE AND movies.studio.city = 'Burbank'

    - <rootX> must be a key in SCHEMA_REGISTRY.
    - SELECT attributes cover only the *fields* in the final result; indexes of
      the primary root are always returned in result["indexes"].

      For JOIN:
        - Left-root fields may be written as either "field" or "<left_root>.field".
          Internally, they are stored as just "field" (e.g. "title").
        - Right-root fields are exposed as "<right_root>.<field>", e.g.
          "movies.title", "movies.studio.city".
        - Right-root indexes are exposed as "<right_root>.indexes.<idx>".

    - WHERE:
      - Supports: =, AND, OR, parentheses.
      - For primary root indexes: "id", "indexes.id", or "<left_root>.indexes.id".
      - For primary root fields: "field" or "<left_root>.field".
      - For joined root: use "right_root.field" or "right_root.indexes.id".
    - Result format:
        [
          {
            "root": "<primary_root>",
            "indexes": { <index1>: ..., <index2>: ... },   # only primary root
            "fields":  { <selected_field1>: ..., <selected_field2>: ... }
          },
          ...
        ]
    """
    import re

    m = _SQL_RE.match(query)
    if not m:
        raise ValueError(f"Invalid SQL query: {query!r}")

    select_part = m.group("select").strip()
    from_and_rest = m.group("from_and_rest").strip()

    # Split WHERE (if any) from FROM/JOIN part
    m_where = re.search(r"\bWHERE\b", from_and_rest, flags=re.IGNORECASE)
    if m_where:
        from_join_part = from_and_rest[: m_where.start()].strip()
        where_part = from_and_rest[m_where.end() :].strip()
    else:
        from_join_part = from_and_rest
        where_part = None

    # Detect JOIN
    m_join = re.search(r"\b(INNER\s+JOIN|JOIN)\b", from_join_part, flags=re.IGNORECASE)
    if not m_join:
        primary_from = from_join_part
        right_from = None
        on_clause = None
    else:
        primary_from = from_join_part[: m_join.start()].strip()
        rest_join = from_join_part[m_join.end() :].strip()

        m_on = re.search(r"\bON\b", rest_join, flags=re.IGNORECASE)
        if not m_on:
            raise ValueError("JOIN without ON clause is not supported")

        right_from = rest_join[: m_on.start()].strip()
        on_clause = rest_join[m_on.end() :].strip()

    # ------------------------------------------------------------------
    # Resolve primary (left) root spec
    # ------------------------------------------------------------------
    left_root, left_schema, left_index_names = _parse_from_root_spec(primary_from)

    # ------------------------------------------------------------------
    # Resolve SELECT list (we need field names from left, maybe right)
    # ------------------------------------------------------------------
    # Left fields
    left_fields_model_cls = left_schema.fields
    if not isinstance(left_fields_model_cls, type) and hasattr(left_fields_model_cls, "__class__"):
        left_fields_model_cls = left_fields_model_cls.__class__
    left_field_specs = _field_specs(left_fields_model_cls)
    left_field_names = list(left_field_specs.keys())
    left_field_names_qualified = [f"{left_root}.{n}" for n in left_field_names]

    join_used = right_from is not None

    if not join_used:
        # No JOIN
        all_field_names_for_star_internal = left_field_names
        allowed_select_names = set(left_field_names) | set(left_field_names_qualified)
        right_root = None
        right_schema = None
        right_field_specs = {}
        right_index_specs: List[Dict[str, Any]] = []
    else:
        # Right (joined) root
        right_root, right_schema, _right_index_names = _parse_from_root_spec(right_from)

        right_fields_model_cls = right_schema.fields
        if not isinstance(right_fields_model_cls, type) and hasattr(
            right_fields_model_cls, "__class__"
        ):
            right_fields_model_cls = right_fields_model_cls.__class__
        right_field_specs = _field_specs(right_fields_model_cls)
        right_field_names = list(right_field_specs.keys())

        # Right fields appear in combined entry as "<right_root>.<field>"
        right_field_names_qualified = [f"{right_root}.{name}" for name in right_field_names]

        # Right indexes appear as "<right_root>.indexes.<idx>"
        right_index_specs = _index_specs(right_schema)
        right_index_field_names = [
            f"{right_root}.indexes.{spec['name']}" for spec in right_index_specs
        ]

        # "*" should include all real fields (left + right), not right indexes
        all_field_names_for_star_internal = left_field_names + right_field_names_qualified

        allowed_select_names = set()
        allowed_select_names.update(left_field_names)
        allowed_select_names.update(left_field_names_qualified)
        allowed_select_names.update(right_field_names_qualified)
        allowed_select_names.update(right_index_field_names)

    # Parse SELECT
    select_raw = [f.strip() for f in select_part.split(",") if f.strip()]
    if len(select_raw) == 1 and select_raw[0] == "*":
        select_internal = list(all_field_names_for_star_internal)
    else:
        select_internal: List[str] = []
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

            # Map external name to internal flattened key
            if name.startswith(left_root + "."):
                # "books.title" -> "title"
                base = name[len(left_root) + 1 :]
                select_internal.append(base)
            else:
                # For right side ("movies.*" or "movies.indexes.*") or bare left
                select_internal.append(name)

    # ------------------------------------------------------------------
    # Build base entries (flattened)
    # ------------------------------------------------------------------
    if not join_used:
        entries = _all_entries(schema=left_schema, json_data=json_data, filters=None)

        where_pred = _build_where_predicate(
            where_part, primary_schema=left_schema, primary_root=left_root
        )
        entries = [e for e in entries if where_pred(e)]

    else:
        # JOIN path
        assert right_root is not None and right_schema is not None

        left_entries = _all_entries(schema=left_schema, json_data=json_data, filters=None)
        right_entries = _all_entries(schema=right_schema, json_data=json_data, filters=None)

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

    # ------------------------------------------------------------------
    # Projection + reshape into {"root": ..., "indexes": ..., "fields": ...}
    # ------------------------------------------------------------------
    results: List[Dict[str, Any]] = []
    for e in entries:
        reshaped = _reshape_entry(e, left_schema)  # {"root": ..., "indexes": {...}, "fields": {...}}
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
            }
        )

    return results

