from typing import List, Any

import redis
from redis_lru import RedisLRU

from models import Author, Quote

client = redis.StrictRedis(host="localhost", port=6379, password=None)
cache = RedisLRU(client)


@cache
def find_by_tag(tag: str) -> list[str | None]:
    print(f"Find by {tag}")
    quotes = Quote.objects(tags__iregex=tag)
    result = [q.quote for q in quotes]
    return result


@cache
def find_by_author(author: str) -> list[list[Any]]:
    print(f"Find by {author}")
    authors = Author.objects(fullname__iregex=author)
    result = {}
    for a in authors:
        quotes = Quote.objects(author=a)
        result[a.fullname] = [q.quote for q in quotes]
    return result


@cache
def find_by_tags(tags: List[str]) -> list[str | None]:
    tag_key = '_'.join(sorted(tags))
    cached_result = cache.get(tag_key)
    if cached_result:
        print(f"Found cached result for tags: {tag_key}")
        return cached_result

    print(f"Find by tags: {', '.join(tags)}")
    quotes = Quote.objects(tags__all=tags)
    result = [q.quote for q in quotes]

    cache.set(tag_key, result)
    return result


if __name__ == '__main__':
    print(find_by_tag('mi'))
    print(find_by_tag('mi'))

    print(find_by_author('in'))
    print(find_by_author('in'))
    quotes = Quote.objects().all()
    print([e.to_json() for e in quotes])
    
    tags_set_1 = ['inspiration', 'motivation']
    tags_set_2 = ['motivation', 'success']

    print(find_by_tags(tags_set_1))
    print(find_by_tags(tags_set_1))  

    print(find_by_tags(tags_set_2))
    print(find_by_tags(tags_set_2))
