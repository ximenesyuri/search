from typed import model, MODEL, _MODEL_, Str

@model(ordered=True)
class Indexes(MODEL): pass

@model
class Fields(MODEL): pass

@model
class Filters(Indexes, Fields, _MODEL_): pass

@model
class Schema(MODEL):
    root: Str
    indexes: Indexes
    fields: Fields
