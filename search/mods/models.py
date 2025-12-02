from typed import model, MODEL, Str

@model(ordered=True)
class Indexes(MODEL): pass

@model
class Fields(MODEL): pass

@model
class Filters(Indexes, Fields): pass

@model
class Schema(MODEL):
    root: Str
    indexes: Indexes
    fields: Fields
