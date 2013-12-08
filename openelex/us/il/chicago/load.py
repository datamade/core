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
            self._make_ward_voter_counts(election)
        return 'Aldermanic contests made'

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

    def _make_ward_voter_counts(self, election):
        election_id = election['election_id']
        for url in election['voters']:
            ward = parse_qs(urlparse(url).query)['Ward'][0]
            soup = BeautifulSoup(self._scraper.urlopen(url))
            table = soup.find('table')
            all_rows = self.parse_table(table)
            all_rows.next()
            for row in all_rows:
                try:
                    precinct = int(row.pop(0))
                    voters = int(row[0])
                    ocd_id = 'ocd-division/country:us/state:il/place:chicago/ward:%s/precinct:%s' % (ward, precinct)
                    results = Result.objects(Q(election_id=election_id) & Q(ocd_id=ocd_id))\
                        .update(set__registered_voters=voters, multi=True)
                    print 'Updated %s in Ward %s, precinct %s' % (results, ward, precinct)
                except ValueError:
                    continue
        return 'Registered voter counts made'

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
    
