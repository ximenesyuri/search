from typed import model, Entry, MODEL
from typed.mods.meta.models import MODEL_META

@model
class Indexes: pass

@model
class Fields: pass

@model
class Filters: pass

class _INDEXES_(MODEL_META):
    def __instancecheck__(cls, instance):
        if not instance in MODEL:
            return False
        if not instance <= Indexes:
            return False
        if not getattr(instance, 'is_indexes', False):
            return False
        return True

class _FIELDS_(MODEL_META):
    def __instancecheck__(cls, instance):
        if not instance in MODEL:
            return False
        if not instance <= Fields:
            return False
        if not getattr(instance, 'is_fields', False):
            return False
        return True

INDEXES = _INDEXES_('INDEXES', (MODEL,), {})
FIELDS = _FIELDS_('FIELDS', (MODEL,), {})

@model
class Schema:
    root: Entry
    indexes: INDEXES
    fields: FIELDS
