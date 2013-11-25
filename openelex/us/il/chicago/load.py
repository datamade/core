from os.path import exists, join, dirname, abspath
from openelex.base.load_place import BaseLoader
from mongoengine import *
from openelex.models import Candidate, Result, Contest, Office
import json
import unicodecsv
import datetime
import scrapelib
from datetime import datetime

"""
Usage:
  TODO: Explain how to use this thing
"""

class LoadResults(BaseLoader):
    

    def load(self):
        connect('openelex_il_test')
        self.make_contests()
    
    @property
    def mapper_file(self):
        print self.mappings_dir
        return json.load(open(join(self.mappings_dir, 'filenames.json'), 'rb'))

    def make_contests(self):
        for year, elections in self.mapper_file.items():
            for election in elections:
                contest = {
                    'election_id': election['election_id'],
                    'start_date': datetime.strptime(election['date'], '%Y-%m-%d'),
                    'end_date': datetime.strptime(election['date'], '%Y-%m-%d'),
                    'result_type': 'certified',
                    'state': self.state,
                    'year': year,
                }
                c, created = Contest.objects.get_or_create(**contest)
                if created:
                    c.created = datetime.now()
                    c.save()
                else:
                    c.updated = datetime.now()
                    c.save()
                for result in election['results']:
                    # TODO: Need to somehow figure out the
                    # what spatial area the offices cover
                    o = {
                        'state': self.state,
                        'name': result['result_name'],
                    }
                    office, created = Office.objects.get_or_create(**o)

                    # need to get ward, precinct, contest name, candidate_names

    @property
    def _scraper(self):
        s = scrapelib.Scraper(requests_per_minute=60,
                              follow_robots=True,
                              raise_errors=False,
                              retry_attempts=0)
        s.cache_storage = scrapelib.cache.FileCache(self.cache_dir)
        s.cache_write_only = False
        return s
    
