from os.path import exists, join
from openelex.base.load import BaseLoader
from openelex.us.md import fetch
from mongoengine import *
from openelex.models import Candidate, Result, Contest
import json
import unicodecsv
import requests
import datetime

"""
load() accepts an object from filenames.json

Usage:

from openelex.us.md import load
l = load.LoadResults()
file = l.filenames['2000'][49] # just an example
l.load(file)
"""

class LoadResults(BaseLoader):
    
    working_dir = os.path.abspath(os.path.dirname(__file__))
    mappings_dir = os.path.join(self.working_dir, 'mappings')
    cache_dir = os.path.join(self.working_dir, 'cache')

    def load(self, year=None, election=None):
        connect('openelex_il_test')
        contest = self.get_contest(year, election)
        if contest.year == 2002:
            self.load_2002_file(file, contest)
        elif contest.year == 2000 and contest.election_type == 'primary': # special case for 2000 primary
            self.load_2000_primary_file(file, contest)
        else:
            self.load_non2002_file(file, contest)
        contest.updated = datetime.datetime.now()
        contest.save()
    
    def elections(self, year):
        url = "http://openelections.net/api/v1/state/%s/year/%s/" % (self.state, year)
        response = json.loads(requests.get(url).text)
        return response['elections']
    
    def get_contest(self, year, election):
        year = int(file['generated_name'][0:4])
        election = [e for e in self.elections(year) if e['id'] == file['election']][0]
        start_year, start_month, start_day = election['start_date'].split('-')
        end_year, end_month, end_day = election['end_date'].split('-')
        contest, created = Contest.objects.get_or_create(state=self.state, year=year, election_id=election['id'], start_date=datetime.date(int(start_year), int(start_month), int(start_day)), end_date=datetime.date(int(end_year), int(end_month), int(end_day)), election_type=election['election_type'], result_type=election['result_type'], special=election['special'])
        if created == True:
            contest.created = datetime.datetime.now()
            contest.save()
        return contest
    
