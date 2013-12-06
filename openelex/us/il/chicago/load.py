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

    def load(self, **kwargs):
        # This step takes a while so make it optional
        self.load_alderman_results(year=kwargs.get('year'), election_id=kwargs.get('election'))
    
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
                    'source': 'http://chicagoelections.com',
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
                        office = Office(**o)

    def load_alderman_results(self, **kwargs):
        year = kwargs.get('year')
        elections = self.mapper_file
        if kwargs.get('year'):
            elections = elections[kwargs.get('year')]
            if kwargs.get('election_id'):
                elections = [e for e in elections if e['election_id'] == kwargs.get('election_id')]
        if kwargs.get('election_id'):
            elex = []
            for year, e in elections:
                elex.extend([el for el in e if el['election_id'] == kwargs.get('election_id')])
            elections = elex
        for election in elections:
            for result_set in election['results']:
                name = result_set['result_name'].title()
                if 'Alderman' in name:
                    o = {
                        'state': self.state,
                        'name': name,
                    }
                    office = Office(**o)
                    c = {
                        'election_id': election['election_id'],
                        'start_date': datetime.strptime(election['date'], '%Y-%m-%d'),
                        'end_date': datetime.strptime(election['date'], '%Y-%m-%d'),
                        'result_type': 'certified',
                        'state': self.state,
                        'year': year,
                        'source': 'http://chicagoelections.com',
                        'office': office,
                        'raw_office': name,
                        'slug': slugify(name),
                    }
                    contest, created = Contest.objects.get_or_create(**c)
                    if created:
                        contest.created = datetime.now()
                        contest.save()
                    else:
                        contest.updated = datetime.now()
                        contest.save()
                    self._make_alderman_results(result_set, contest)
        return 'boogers'

    def _make_alderman_results(self, election_results, contest):
        name = election_results['result_name']
        urls = election_results['results_links']
        if 'alderman' in name.lower():
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
                        res['ocd_id'] = 'ocd-division/country:us/state:il/place:chicago/ward:%s/precinct:%s' % (ward, precinct)
                        res['contest'] = contest
                        res['state'] = self.state
                        res['raw_jurisdiction'] = 'Chicago, IL Ward %s' % ward
                        res['election_id'] = contest.election_id
                        for option,votes in zip(header, row):
                            cand['raw_full_name'] = option
                            try:
                                c = Candidate.objects.get(raw_full_name=option)
                                can = {}
                                if not contest.election_id in c.election_ids:
                                    can['push__contests'] = contest
                                    can['push__contest_slugs'] = contest.slug
                                    can['push__election_ids'] = contest.election_id
                                    Candidate.objects(id=c.id).update_one(**can)
                            except Candidate.DoesNotExist:
                                cand['contests'] = [contest]
                                cand['contest_slugs'] = [contest.slug]
                                cand['election_ids'] = [contest.election_id]
                                cand['source'] = 'http://chicagoelections.com'
                                cand['slug'] = slugify(option)
                                c = Candidate(**cand)
                                c.save()
                            res['raw_total_votes'] = votes
                            res['total_votes'] = votes
                            res['candidate'] = c
                            res['contest'] = contest
                            res['candidate_slug'] = c.slug
                            res['contest_slug'] = contest.slug
                            res['source'] = page
                            r, created = Result.objects.get_or_create(**res)
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
    
