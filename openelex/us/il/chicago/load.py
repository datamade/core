from os.path import exists, join, dirname, abspath
from openelex.base.load_place import BaseLoader
from mongoengine import *
from openelex.models import Candidate, Result, Contest, Office
import json
import unicodecsv
import datetime
import scrapelib
from datetime import datetime
from BeautifulSoup import BeautifulSoup
from urlparse import parse_qs, urlparse
import requests
import unicodedata
import re

"""
Usage:
  TODO: Explain how to use this thing
"""

def slugify(value):
    try:
        value = value.decode('utf-8')
    except UnicodeEncodeError:
        pass
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    return re.sub('[-\s]+', '-', value)

class LoadResults(BaseLoader):

    def load(self, year=None, election=None, update_contests=False):
        # This step takes a while so make it optional
        if update_contests:
            self.update_contests()
        self.load_alderman_results(year=year, election_id=election)
    
    @property
    def mapper_file(self):
        return json.load(open(join(self.mappings_dir, 'filenames.json'), 'rb'))

    def update_contests(self):
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
                    # There is also a need to somehow create 
                    # a uniform way of representing an individual Office name
                    # ex: Alderman 14th Ward is the same as 14th Ward Alderman
                    name = result['result_name'].title()
                    if 'Alderman' in name:
                        o = {
                            'state': self.state,
                            'name': result['result_name'].title(),
                        }
                        office, created = Office.objects.get_or_create(**o)

    def load_alderman_results(self, **kwargs):
        for k,v in kwargs.items():
            if not kwargs[k]:
                del kwargs[k]
        contests = Contest.objects(**kwargs)
        mapped = self.mapper_file
        for contest in contests:
            election_results = [e['results'] for e in mapped[str(contest.year)]
                if e['election_id'] == contest.election_id][0]
            Contest.objects(id=contest.id).update_one(set__results=[])
            for result in self._make_alderman_results(election_results):
                Contest.objects(id=contest.id).update_one(push__results=result)
        return 'boogers'

    def _make_alderman_results(self, election_results):
        for election in election_results:
            name = election['result_name']
            urls = election['results_links']
            if 'alderman' in name.lower():
                raw_office = name
                office, created = Office.objects.get_or_create(
                    name=name,
                    state=self.state)
                for page in urls:
                    ward = parse_qs(urlparse(page).query)['Ward'][0]
                    soup = BeautifulSoup(self._scraper.urlopen(page))
                    table = soup.find('table')
                    all_rows = self.parse_table(table)
                    header = all_rows.next()[1:]
                    for row in all_rows:
                        try:
                            precinct = int(row.pop(0))
                            res = {'reporting_level': 'precinct'}
                            cand = {'state': self.state}
                            res['raw_office'] = raw_office
                            res['office'] = office
                            res['ocd_id'] = 'ocd-division/country:us/state:il/place:chicago/ward:%s/precinct:%s' % (ward, precinct)
                            for option,votes in zip(header, row):
                                cand['name'] = option
                                res['total_votes'] = votes
                                c, created = Candidate.objects.get_or_create(**cand)
                                res['candidate'] = c
                                r = Result(**res)
                                yield r
                        except ValueError:
                            continue

    def parse_table(self, results_table) :
        for row in results_table.findAll('tr'):
            row_list = []
            cells = row.findAll('td')
            if len(cells) < 2:
                continue
            for cell in cells:
                row_list.append(cell.text)
            if len(row_list) > 2:
                yield row_list[::2]
            else:
                yield row_list

    @property
    def _scraper(self):
        s = scrapelib.Scraper(requests_per_minute=60,
                              follow_robots=True,
                              raise_errors=False,
                              retry_attempts=0)
        s.cache_storage = scrapelib.cache.FileCache(self.cache_dir)
        s.cache_write_only = False
        return s
    
