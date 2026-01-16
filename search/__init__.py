from utils.general import lazy

__imports__ = {
    "search":          "search.mods.search_",
    "Query":           "search.mods.search_",
    "Schema":          "search.mods.models",
    "Fields":          "search.mods.models",
    "Filters":         "search.mods.models",
    "Indexes":         "search.mods.models",
    "fields":          "search.mods.decorators",
    "filters":         "search.mods.decorators",
    "indexes":         "search.mods.decorators",
    "register_schema": "search.mods.sql",
    "sql":              "search.mods.sql",
    "unflat":          "search.mods.unflat"
}


if lazy(__imports__):
    from search.mods.search_ import search, Query
    from search.mods.models import Schema, Fields, Filters, Indexes
    from search.mods.decorators import fields, filters, indexes
    from search.mods.sql import register_schema, sql
    from search.mods.unflat import unflat
