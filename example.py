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

import os.path
import psycopg2
from functools import lru_cache
import pipeline.lib.geoprocessing as gp

cutoff_year = 2005

@lru_cache(maxsize=None)
def is_in_protected_area(db_cursor, latitude, longitude):
    db_cursor.execute("SELECT geometry FROM protected_areas")
    return gp.is_in_protected_area(db_cursor.fetchall(),
                                   latitude, longitude)


def deforestation_scoring(signs_of_deforestation,
                          year_deforestation_occurred,
                          is_farm_in_protected_area):
    if signs_of_deforestation:
        if year_deforestation_occurred < cutoff_year:
            return True, "Deforestation occurred before the cutoff year."

        if is_farm_in_protected_area:
            return False, "Recent deforestation in a protected area."

        return True, "Recent deforestation not in a protected area."

    return True, "No signs of deforestation"


def process_country_harvest(country, harvest):
    conn1 = psycopg2.connect(host="external_db.enveritas.org",
                             database="postgres", user="postgres",
                             password="postgres")
    conn2 = psycopg2.connect(host="pipeline_dest.enveritas.org",
                             database="postgres", user="postgres",
                             password="postgres")

    query_file_name = '%s_%s_surveys.sql' % (country, harvest)
    query_path = os.path.join(os.path.dirname(__file__), query_file_name)
    cur = conn1.cursor()
    cur.execute(open(query_path).read())

    for (survey_id, signs_deforestation_occurred,
         year_deforestation_occurred,
         latitude, longitude) in cur.fetchall():

        is_farm_in_protected_area = is_in_protected_area(cur,
                                                         latitude, longitude)

        passes, reason = deforestation_scoring(signs_deforestation_occurred,
                                               year_deforestation_occurred,
                                               is_farm_in_protected_area)

        cur2 = conn2.cursor()
        insert_q = """
        INSERT INTO scoring_results (survey_id, country, harvest,
                                     DF1_passes, DF1_explanation ) 
        VALUES ('%s', '%s', '%s', '%s', '%s')
        """ % (survey_id, country, harvest, passes, reason)

        cur2.execute(insert_q)
        conn2.commit()


path = os.path.join(os.path.dirname(__file__), 'pipelines.txt')

while True:
    pipelines_to_process = open(path).read().split('\n')

    for pipeline in pipelines_to_process:
        country, harvest = pipeline.split(',')
        process_country_harvest(country, harvest)
