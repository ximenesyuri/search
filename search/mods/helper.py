def _normalize_queries(query):
    if isinstance(query, (list, tuple)):
        return [str(x).strip().lower() for x in query if x]
    if query:
        return [str(query).strip().lower()]
    return []

def _ensure_no_defaults(cls, kind):
    ann = getattr(cls, '__annotations__', {})
    for name in ann:
        if name in cls.__dict__:
            raise TypeError(
                f"{kind} model '{cls.__name__}' cannot define a default value "
                f"for attribute '{name}'. Defaults are not allowed."
            )

def _ensure_extends(cls, base, kind):
    bases = getattr(cls, '__bases__', ())
    if not any(isinstance(b, type) and issubclass(b, base) for b in bases):
        raise TypeError(
            f"{kind} model '{cls.__name__}' must extend '{base.__name__}'. "
            f"Define it as 'class {cls.__name__}({base.__name__}): ...'."
        )
