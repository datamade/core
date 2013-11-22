"""
Get URLs for election results pages on chicagoelections.com
and write them to mappings/filenames.json
"""
import re
import os
import requests
import scrapelib
from BeautifulSoup import BeautifulSoup
from datetime import date
from operator import itemgetter
from itertools import groupby
import csv

# from openelex.api import elections as elec_api
from openelex.base.datasource import BaseDatasource

class Datasource(BaseDatasource):

    base_url = "https://www.chicagoelections.com"
    working_dir = os.path.abspath(os.path.dirname(__file__))
    mappings_dir = os.path.join(self.working_dir, 'mappings')

    # PUBLIC INTERFACE
    def contest_urls(self, election=None, year=None):
        """ 
        Get a list of contest URLs optionally filtered by election or year
        """
        if not hasattr(self, '_contest_urls'):
            self._contest_urls = {}
            self._voters_urls = {}
            self._ballots_urls = {}
            if year and election:
                elections = [(e['name'], e['url'], e['year'], e['election_id']) 
                    for e in self.elections(year=year).values()[0] if e['name'] == election]
            elif year:
                elections = [(e['name'], e['url'], e['year'], e['election_id']) 
                    for e in self.elections(year=year).values()[0]]
            elif election:
                elections = [(e['name'], e['url'], e['year'], e['election_id']) 
                    for e in self.elections(year=year).values()[0] if e['name'] == election]
            else:
                elections = []
                mappings = self.elections_urls()
                for year, elex in mappings.items():
                    elections.extend([(e['name'], e['url'], e['year'], e['election_id']) for e in elex])
            for name, url, year, election_id in elections:
                if not self._contest_urls.has_key(year):
                    self._contest_urls[year] = {}
                self._contest_urls[year][name] = {'election_id': election_id, 'contests': []}
                if not self._voters_urls.has_key(year):
                    self._voters_urls[year] = {}
                self._voters_urls[year][name] = {'election_id': election_id, 'voters': []}
                if not self._ballots_urls.has_key(year):
                    self._ballots_urls[year] = {}
                self._ballots_urls[year][name] = {'election_id': election_id, 'ballots': []}
                if url:
                    page = self._scraper.urlopen(url)
                    for option in self.get_select_options(page):
                        payload = {'D3' : option, 'flag' : 1, 'B1' : '  View the Results'}
                        contests = self._scraper.urlopen(url, 'POST', payload)
                        soup = BeautifulSoup(contests)
                        links = ['%s/%s' % (self.base_url, a['href']) for a in soup.findAll('a')]
                        # cache the junk
                        for link in links:
                            self._scraper.urlopen(link)
                        if 'registered' in option.lower():
                            self._voters_urls[year][name]['voters'].extend(links)
                        elif 'ballots' in option.lower():
                            self._ballots_urls[year][name]['ballots'].extend(links)
                        else:
                            self._contest_urls[year][name]['contests'].extend(links)
                        print self._contest_urls[year].keys()
        return self._contest_urls

    def voters_urls(self, election=None, year=None):
        if not hasattr(self, '_voters_urls'):
            self.contest_urls(election=election, year=year)
        return self._voters_urls

    def ballots_urls(self, election=None, year=None):
        if not hasattr(self, '_ballots_urls'):
            self.contest_urls(election=election, year=year)
        return self._ballots_urls

    def elections(self, year=None):
        if not hasattr(self, '_elections'):
            self._elections = {}
            date_file = os.path.join(self.working_dir, 'election_dates.csv')
            election_dates = list(csv.DictReader(open(date_file, 'rb')))
            with_years = [{'year': d['Date'].split('-')[0], 
                           'date': d['Date'], 
                           'name': d['Election'],
                           'url': d['URL'],
                           'ocd_id': 'ocd-division/country:us/state:il/place:chicago',
                           'election_id': '-'.join(d['Election'].replace(',', '').split(' '))}
                          for d in election_dates]
            with_years = sorted(with_years, key=itemgetter('year'))
            for y, group in groupby(with_years, key=itemgetter('year')):
                self._elections[y] = list(group)
        if year:
            return {year: self._elections[year]}
        else:
            return self._elections

    def mappings(self, year=None, election=None):
        d = {}
        elections = self.elections(year=year)
        contests = self.contest_urls(year=year, election=election)
        ballots = self.ballots_urls(year=year, election=election)
        voters = self.voters_urls(year=year, election=election)
        for year, elex in elections.items():
            d[year] = []
            for election in elex:
                election['contests'] = contests[election['year']][election['name']]['contests']
                election['voters'] = voters[election['year']][election['name']]['voters']
                election['ballots'] = ballots[election['year']][election['name']]['ballots']
                d[year].append(election) 
        f = open(os.path.join(self.mappings_dir, 'filenames.json'), 'w')
        f.write(json.dumps(d))
        f.close()
        return d

    def get_select_options(self, resp) :
        soup = BeautifulSoup(resp)
        options = soup.findAll('select', attrs={'name' : 'D3'})
        if options :
            for option in options[0].findAll('option') :
                option_value = option.get('value')
                if option_value is not None :
                    yield option_value

    @property
    def _scraper(self):
        s = scrapelib.Scraper(requests_per_minute=60,
                              follow_robots=True,
                              raise_errors=False,
                              retry_attempts=0)
        s.cache_storage = scrapelib.cache.FileCache(os.path.join(self.working_dir,'cache'))
        s.cache_write_only = False
        return s
