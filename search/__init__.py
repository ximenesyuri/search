from search.mods.search import search
from search.mods.models import Schema, Fields, Filters, Indexes
from search.mods.decorators import fields, filters, indexes
from search.mods.sql import register_schema, sql
from search.mods.unflat import unflat

json_data = {
    "books": {
        "book_101": {
            "title": "aaaa",
            "author": "Douglas Adams",
            "genre": "Science Fiction",
            "tags": ["humor", "space", "adventure"],
            "publication_year": 1979,
            "available": True,
            "publisher": {
                "name": "Pan Books",
                "city": "London"
            }
        },
        "book_102": {
            "title": "aaaabbbb",
            "author": "Jane Austen",
            "genre": "Romance",
            "tags": ["classic", "love", "social commentary"],
            "publication_year": 1813,
            "available": False,
            "publisher": {
                "name": "T. Egerton, Whitehall",
                "city": "London"
            }
        },
        "book_103": {
            "title": "1984",
            "author": "Georgeaaaa Orwell",
            "genre": "Dystopian",
            "tags": ["politics", "surveillance", "totalitarianism"],
            "publication_year": 1949,
            "available": True,
            "publisher": {
                "name": "Secker & Warburg",
                "city": "London"
            }
        }
    },
    "movies": {
        "movie_201": {
            "title": "aaaa",
            "director": "The Wachowskis",
            "genre": "Science Fiction",
            "tags": ["cyberpunk", "philosophy", "action"],
            "release_year": 1999,
            "available": True,
            "studio": {
                "name": "Warner Bros.",
                "city": "Burbank"
            }
        },
        "movie_202": {
            "title": "Dune",
            "director": "Denis Villeneuve",
            "genre": "Science Fiction",
            "tags": ["epic", "fantasy", "desert"],
            "release_year": 2021,
            "available": True,
            "studio": {
                "name": "Legendary Pictures",
                "city": "Burbank"
            }
        }
    },
    "music": {
        "album_301": {
            "title": "Abbey Road",
            "artist": "The Beatles",
            "genre": "Rock",
            "tags": ["classic", "british", "60s"],
            "release_year": 1969,
            "publisher": {
                "name": "Apple Records",
                "city": "London"
            }
        }
    }
}

from typed import Str, List, Nat, Bool, Maybe

@indexes
class BookIndexes(Indexes):
    id: Str

@fields
class BookPublisher(Fields):
    name: Str
    city: Str

@fields
class BookFields(Fields):
    title: Str
    author: Str
    genre: Str
    tags: List(Str)
    publication_year: Nat
    available: Bool
    publisher: BookPublisher

book_schema = Schema(
    root="books",
    indexes=BookIndexes,   # model class
    fields=BookFields,     # model class
)

register_schema(book_schema)

@indexes
class MoviesIndexes(Indexes):
    id: Str

@fields
class MoviesStudio(Fields):
    name: Str
    city: Str

@fields
class MoviesFields(Fields):
    title: Str
    author: Str
    genre: Str
    tags: List(Str)
    publication_year: Nat
    available: Bool
    studio: MoviesStudio

movies_schema = Schema(
    root="movies",
    indexes=MoviesIndexes,   # model class
    fields=MoviesFields,     # model class
)

register_schema(movies_schema)

@filters(schema=book_schema)
class BookFilter1(Filters):
    id: Str

@filters(schema=book_schema)
class BookFilter2(Filters):
    id: Maybe(Str) = None

@filters(schema=book_schema)
class BookFilter3(Filters):
    id: Maybe(Str) = None
    available: Bool = True

def search_by_title(
    json_data,
    query: Str,
    filters_model=None,
    fuzzy: Bool = False,
    max_results = 5,
    temp=80,
    exact: Bool = False,
    **filters_kwargs,         # e.g. id=..., available=...
):
    """
    Example:
      search_by_title(json_data, "aaa", filter_model=BookFilter3, id=None)
    """

    results = search(
        json_data=json_data,
        fields=["title", "author"],
        query=query,
        schema=book_schema,
        filters_model=filters_model,
        fuzzy=fuzzy,
        temp=temp,
        max_results=max_results,
        exact=exact,
        **filters_kwargs
    )
    return results

from utils import json
#print(json.print(unflat(search_by_title(json_data=json_data, fuzzy=True, query="a NOT b", temp=100, max_results=5, available=False))))

sql_query = """
SELECT title
FROM books
"""

json.print(unflat(sql(json_data=json_data, query=sql_query)))
