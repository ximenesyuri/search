from typed import model
from typed.models import MODEL, LAZY_MODEL
from typed.mods.meta.models import _MODEL_, _LAZY_MODEL_
from utils.types import Entry

@model
class Indexes: pass

@model
class Fields: pass

@model
class Filters: pass

class _INDEXES_(_LAZY_MODEL_, _MODEL_):
    def __instancecheck__(cls, instance):
        if not instance in MODEL and not instance in LAZY_MODEL:
            return False
        if not instance <= Indexes:
            return False
        if not getattr(instance, 'is_indexes', False):
            return False
        return True

class _FIELDS_(_LAZY_MODEL_, _MODEL_):
    def __instancecheck__(cls, instance):
        if not instance in MODEL and not instance in LAZY_MODEL:
            return False
        if not instance <= Fields:
            return False
        if not getattr(instance, 'is_fields', False):
            return False
        return True

    def __call__(cls, *args, **kwargs):
        return type.__call__(*args, **kwargs)

INDEXES = _INDEXES_("INDEXES", (LAZY_MODEL, MODEL), {})
FIELDS = _FIELDS_("FIELDS", (LAZY_MODEL, MODEL), {})

@model
class Schema:
    root: Entry
    indexes: INDEXES
    fields: FIELDS
