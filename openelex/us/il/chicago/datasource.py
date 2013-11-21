"""
Get URLs for election results pages on chicagoelections.com and
save to mappings/filenames.json

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

    base_url = "https://www.chicagoelections.com/"
    working_dir = os.path.abspath(os.path.dirname(__file__))

    # PUBLIC INTERFACE
    def mappings(self, year=None):
        """Return array of dicts  containing source url and 
        standardized filename for raw results file, along 
        with other pieces of metadata
        """
        mappings = []
        for yr, elecs in self.elections(year).items():
            mappings.extend(self._build_metadata(yr, elecs))
        return mappings

    def target_urls(self, year=None):
        "Get list of source data urls, optionally filtered by year"
        return [item['raw_url'] for item in self.mappings(year)]

    def filename_url_pairs(self, year=None):
        return [(item['generated_filename'], item['raw_url']) 
                for item in self.mappings(year)]

    def elections(self, year=None):
        # Fetch all elections initially and stash on instance
        if not hasattr(self, '_elections'):
            self._elections = {}
            date_file = os.path.join(self.working_dir, 'election_dates.csv')
            election_dates = list(csv.DictReader(open(date_file, 'rb')))
            with_years = [{'year': d['Date'].split('-')[0], 
                           'date': d['Date'], 
                           'election_name': d['Election'],
                           'url': d['URL'],
                           'election_id': '-'.join(d['Election'].replace(',', '').split(' '))}
                          for d in election_dates]
            with_years = sorted(with_years, key=itemgetter('year'))
            for k, g in groupby(with_years, key=itemgetter('year')):
                self._elections[k] = list(g)
        return self._elections
    
    @property
    def _scraper(self):
        s = scrapelib.Scraper(requests_per_minute=60,
                              follow_robots=True,
                              raise_errors=False,
                              retry_attempts=0)
        s.cache_storage = scrapelib.cache.FileCache('cache')
        s.cache_write_only = False
        return s

