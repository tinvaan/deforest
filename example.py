# pylint: disable-all

"""
1. Use env vars
2. Use prepared statements
3. May help to have a singleton access to db connections
4. nit: fix imports
5. question: reasoning behind lru_cache decorator, alternatives?
6. if __name__ == '__main__' with argparse commands (if applicable) -- question
7. does psycopg2 conn close when object goes out of scope? if not close, explicitly (or even otherwise close explicitly)
8. redundant db connection calls -- consider a ctor/dtor approach
9. handle exceptions (db, read/io exceptions for eg)
10. unit tests

"""

# pipeline.scoring.deforestation

import os
import psycopg2

from functools import lru_cache
from os.path import dirname, join

import pipeline.lib.geoprocessing as gp



cutoff_year = 2005


class Database:
    pipeline = None
    external = None

    @classmethod
    def connect(cls):
        cls.external = cls.external or psycopg2.connect(user=os.getenv('DATABASE_USER'),
                                                        host=os.getenv('EXTERNAL_DATABASE'),
                                                        database=os.getenv('DATABASE_NAME'),
                                                        password=os.getenv('DATABASE_PASSWORD'))
        cls.pipeline = cls.pipeline or psycopg2.connect(user=os.getenv('DATABASE_USER'),
                                                        host=os.getenv('EXTERNAL_DATABASE'),
                                                        database=os.getenv('DATABASE_NAME'),
                                                        password=os.getenv('DATABASE_PASSWORD'))
        return cls()

    def close(self):
        self.external.close()
        self.pipeline.close()


class HarvestProcessor:
    def __init__(self):
        self.db = Database.connect()

    def __del__(self):
        self.db.close()

    @lru_cache(maxsize=None)
    def is_in_protected_area(self, db_cursor, latitude, longitude):
        db_cursor.execute("SELECT geometry FROM protected_areas")
        return gp.is_in_protected_area(db_cursor.fetchall(), latitude, longitude)

    def deforestation_scoring(
        self, signs_of_deforestation, year_deforestation_occurred, is_farm_in_protected_area
    ):
        if signs_of_deforestation:
            if year_deforestation_occurred < cutoff_year:
                return True, "Deforestation occurred before the cutoff year."

            if is_farm_in_protected_area:
                return False, "Recent deforestation in a protected area."

            return True, "Recent deforestation not in a protected area."

        return True, "No signs of deforestation"

    def process_country_harvest(self, country, harvest):
        try:
            query_file_name = '%s_%s_surveys.sql' % (country, harvest)
            query_path = join(dirname(__file__), query_file_name)
            cur = self.db.external.cursor()
            cur.execute(open(query_path).read())

            for (survey_id, signs_deforestation_occurred,
                year_deforestation_occurred, latitude, longitude
            ) in cur.fetchall():
                is_farm_in_protected_area = self.is_in_protected_area(cur, latitude, longitude)
                passes, reason = self.deforestation_scoring(signs_deforestation_occurred,
                                                            year_deforestation_occurred,
                                                            is_farm_in_protected_area)
                cur2 = self.db.pipeline.cursor()
                insert_q = """
                INSERT INTO scoring_results (survey_id, country, harvest,
                                            DF1_passes, DF1_explanation )
                VALUES ('%s', '%s', '%s', '%s', '%s')
                """ % (survey_id, country, harvest, passes, reason)
                cur2.execute(insert_q)
                self.db.pipeline.commit()
        except psycopg2.Error as err:
            print("Failed to process ('%s', '%s')" % (country, harvest), err)
            with open('/tmp/errors.txt', 'a') as f:
                f.write("%s - %s" % (country, harvest))


if __name__ == '__main__':
    p = HarvestProcessor()
    path = join(dirname(__file__), 'pipelines.txt')
    while True:
        for pipeline in open(path).read().split('\n'):
            country, harvest = pipeline.split(',')
            p.process_country_harvest(country, harvest)
