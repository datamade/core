from os.path import exists, join, dirname, abspath
from openelex.base.load_place import BaseLoader
from mongoengine import *
from openelex.models import Candidate, Result, Contest
import json
import unicodecsv
import datetime
import scrapelib
from datetime import datetime

"""
load() accepts an object from filenames.json

Usage:

from openelex.us.md import load
l = load.LoadResults()
file = l.filenames['2000'][49] # just an example
l.load(file)
"""

class LoadResults(BaseLoader):
    
    working_dir = abspath(dirname(__file__))
    mappings_dir = join(working_dir, 'mappings')
    cache_dir = join(working_dir, 'cache')

    def load(self):
        connect('openelex_il_test')
        self.make_contests():
    
    @property
    def mapper_file(self):
        return json.load(open(join(self.mappings_dir, 'filenames.json'), 'rb'))

    def make_contests(self):
        for year, elections in self.mapper_file.items():
            for election in elections:
                contest = {
                    'election_id': election['election_id'],
                    'ocd': election['ocd_id'],
                    'start_date': datetime.strptime(election['date'], '%Y-%m-%d'),
                    'end_date': datetime.strptime(election['date'], '%Y-%m-%d'),
                    'result_type': 'certified',
                    'state': 'IL',
                    'year': year,
                }
                c, created = Contest.objects.get_or_create(**contest)
                if created:
                    c.created = datetime.datetime.now()
                    c.save()
                # for contest in election['contests']:
                    # need to get ward, precinct, contest name, candidate_names

    @property
    def _scraper(self):
        s = scrapelib.Scraper(requests_per_minute=60,
                              follow_robots=True,
                              raise_errors=False,
                              retry_attempts=0)
        s.cache_storage = scrapelib.cache.FileCache(join(self.working_dir,'cache'))
        s.cache_write_only = False
        return s
    
