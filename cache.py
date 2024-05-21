
# pylint: disable-all

import json
import functools

from os import path


def cached(filename):
    def interface(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            store = FileCache.get(filename)
            memo = store.read(str(args))
            if not memo:
                val = func(*args, **kwargs)
                store.write(str(args), val)
                return val
            return memo
        return wrapped
    return interface


class FileCache:
    def __init__(self, filename):
        self.file = filename

    @classmethod
    def get(cls, filename):
        cache = cls(filename)
        if not path.exists(filename):
            cache.save(filename, dict())
        return cache

    def read(self, key):
        with open(self.file, 'r') as f:
            cache = json.load(f)
            return cache.get(key)

    def write(self, key, value):
        return self.save(self.file, {key: value})

    def save(self, filename, data):
        cache = {}
        try:
            with open(filename, 'r') as f:
                cache = json.load(f)
        except Exception:
            pass

        with open(filename, 'w') as f:
            cache.update(data)
            json.dump(cache, f, indent=2)


if __name__ == '__main__':
    @cached('test.json')
    def example(x, y):
        return x + y


    assert example(10, 20) == 30
    assert example(10, 20) == 30
    assert example(20, 10) == 30
    assert example(40, 50) == 90
    with open('./test.json', 'r') as f:
        store = json.load(f)
        assert sum(store.values()) == 150