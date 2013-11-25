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
import json

# from openelex.api import elections as elec_api
from openelex.base.datasource_place import BaseDatasource

class Datasource(BaseDatasource):

    base_url = "https://www.chicagoelections.com"
    working_dir = os.path.abspath(os.path.dirname(__file__))
    mappings_dir = os.path.join(working_dir, 'mappings')

    # PUBLIC INTERFACE
    def results_urls(self, election=None, year=None):
        """ 
        Get a list of results URLs optionally filtered by election or year
        """
        if not hasattr(self, '_results_urls'):
            self._results_urls = {}
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
                mappings = self.elections()
                for year, elex in mappings.items():
                    elections.extend([(e['name'], e['url'], e['year'], e['election_id']) for e in elex])
            for name, url, year, election_id in elections:
                if not self._results_urls.has_key(year):
                    self._results_urls[year] = {}
                self._results_urls[year][name] = {'election_id': election_id, 'results': []}
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
                        results = self._scraper.urlopen(url, 'POST', payload)
                        soup = BeautifulSoup(results)
                        links = ['%s/%s' % (self.base_url, a['href']) for a in soup.findAll('a')]
                        # cache the junk
                        for link in links:
                            self._scraper.urlopen(link)
                        if 'registered' in option.lower():
                            self._voters_urls[year][name]['voters'].extend(links)
                        elif 'ballots' in option.lower():
                            self._ballots_urls[year][name]['ballots'].extend(links)
                        else:
                            self._results_urls[year][name]['results'].append({'result_name': option, 'results_links': links})
        return self._results_urls

    def voters_urls(self, election=None, year=None):
        if not hasattr(self, '_voters_urls'):
            self.results_urls(election=election, year=year)
        return self._voters_urls

    def ballots_urls(self, election=None, year=None):
        if not hasattr(self, '_ballots_urls'):
            self.results_urls(election=election, year=year)
        return self._ballots_urls

    def elections(self, year=None, election=None):
        if not hasattr(self, '_elections'):
            self._elections = {}
            date_file = os.path.join(self.working_dir, 'election_dates.csv')
            election_dates = list(csv.DictReader(open(date_file, 'rb')))
            with_years = []
            for e_date in election_dates:
                if election and e_date['Election'] == election:
                    d = {}
                    d['year'] = e_date['Date'].split('-')[0]
                    d['date'] = e_date['Date']
                    d['name'] = e_date['Election']
                    d['url'] = e_date['URL']
                    d['ocd_id'] = 'ocd-division/country:us/state:il/place:chicago'
                    election_id = '-'.join(e_date['Election'].lower().replace(',', '').split(' '))
                    d['election_id'] = 'il-%s-%s' % (e_date['Date'], election_id)
                    with_years.append(d)
                    break
                else:
                    d = {}
                    d['year'] = e_date['Date'].split('-')[0]
                    d['date'] = e_date['Date']
                    d['name'] = e_date['Election']
                    d['url'] = e_date['URL']
                    d['ocd_id'] = 'ocd-division/country:us/state:il/place:chicago'
                    election_id = '-'.join(e_date['Election'].lower().replace(',', '').split(' '))
                    d['election_id'] = 'il-%s-%s' % (e_date['Date'], election_id)
                    with_years.append(d)
            with_years = sorted(with_years, key=itemgetter('year'))
            for y, group in groupby(with_years, key=itemgetter('year')):
                self._elections[y] = list(group)
        if year:
            return {year: self._elections[year]}
        else:
            return self._elections

    def mappings(self, year=None, election=None):
        try:
            f = open(os.path.join(self.mappings_dir, 'filenames.json'), 'rb')
            d = json.loads(f.read())
        except IOError:
            d = self.update_mappings(year=year, election=election)
        print d
        if year and election:
            elec = [e for e in d[year] if e['name'] == election]
            d = {year: elec}
        elif year:
            d = {year: d[year]}
        return d

    def update_mappings(self, year=None, election=None):
        d = {}
        elections = self.elections(year=year, election=election)
        results = self.results_urls(year=year, election=election)
        ballots = self.ballots_urls(year=year, election=election)
        voters = self.voters_urls(year=year, election=election)
        for year, elex in elections.items():
            d[year] = []
            for elec in elex:
                if election and elec['name'] == election:
                    elec['results'] = results[elec['year']][elec['name']]['results']
                    elec['voters'] = voters[elec['year']][elec['name']]['voters']
                    elec['ballots'] = ballots[elec['year']][elec['name']]['ballots']
                    d[year].append(elec) 
                    break
                else:
                    elec['results'] = results[elec['year']][elec['name']]['results']
                    elec['voters'] = voters[elec['year']][elec['name']]['voters']
                    elec['ballots'] = ballots[elec['year']][elec['name']]['ballots']
                    d[year].append(elec) 
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
        s = scrapelib.Scraper(requests_per_minute=120,
                              follow_robots=True,
                              raise_errors=False,
                              retry_attempts=0)
        s.cache_storage = scrapelib.cache.FileCache(os.path.join(self.working_dir,'cache'))
        s.cache_write_only = False
        return s
