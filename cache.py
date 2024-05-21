
# pylint: disable-all

import json


class FileCache:
    def __init__(self, filename):
        self.file = filename

    def read(self, key):
        with open(self.file, 'r') as f:
            cache = json.load(f)
            return cache.get(key)

    def write(self, key, value):
        return self.save(self.file, {key: value})

    def save(self, filename, data, bulk=False):
        mode = 'w' if bulk else 'a'
        with open(filename, mode) as f:
            f.write(json.dumps(data, indent=2))
