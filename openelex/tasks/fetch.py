import sys
import os
import importlib

import click

from openelex.base.fetch import BaseFetcher
from .utils import default_state_options, load_module

@click.command(help="Scrape data files and store in local file cache "
    "under standardized name")
@default_state_options
@click.option('--unprocessed', is_flag=True,
    help="Fetch unprocessed data files only")
def fetch(state, datefilter='', unprocessed=False):
    """
    Scrape data files and store in local file cache
    under standardized name.

    State is required. Optionally provide 'datefilter' 
    to limit files that are fetched.
    """
    state_mod = load_module(state, ['datasource', 'fetch'])
    datasrc = state_mod.datasource.Datasource()
    if hasattr(state_mod, 'fetch'):
        fetcher = state_mod.fetch.FetchResults()
    else:
        fetcher = BaseFetcher(state)

    if unprocessed:
        try:
            filename_url_pairs = datasrc.unprocessed_filename_url_pairs(datefilter)
        except NotImplementedError:
            sys.exit("No unprocessed data files are available. Try running this "
                    "task without the --unprocessed option.")
    else:
        filename_url_pairs = datasrc.filename_url_pairs(datefilter)

    for std_filename, url in filename_url_pairs:
        fetcher.fetch(url, std_filename)

@click.command(help="Scrape places and store results files")
@default_state_options
@click.option('--place', help="the name of the place to scrape")
def scrape(state, place,  datefilter=''):

    placemod = importlib.import_module('openelex.us.%s.places.%s.scraper' %(state, place))
    s = placemod.Scraper()

    if not os.path.exists('election_json'):
        os.mkdir('election_json')

    for elec_name, contests, registered_voters, ballots_cast in s.election_urls():
        s.make_elections_json(elec_name, contests, registered_voters, ballots_cast)
