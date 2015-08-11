import os.path
import sys

import click
import importlib

from .utils import default_state_options, load_module

@click.command(name='load.run', help="Load cached data files into the database")
@default_state_options
@click.argument('filenames', nargs=-1)
@click.option('--place', help="the name of a place to load")
def run(state, place, datefilter='', filenames=[]):
    """
    Load cached data files into MongoDB.

    State is required. Optionally provide:
    - 'place' to load place files 
    - 'datefilter' to limit files that are loaded.
    """

    if datefilter and len(filenames):
        sys.stderr.write("You must specify a datefilter or filename but not both")
        sys.exit(1)

    # load a place within a state
    if place:
        place_load_mod = importlib.import_module('openelex.us.%s.places.%s.load' %(state, place))
        loader = place_load_mod.LoadResults()
        loader.run()

    # load a state
    else:
        state_mod = load_module(state, ['datasource', 'load'])
        datasrc = state_mod.datasource.Datasource()
        loader = state_mod.load.LoadResults()

        if len(filenames):
            # A filename was specified.  Load only this file.
            mappings = [datasrc.mapping_for_file(os.path.basename(fn))
                        for fn in filenames]
        else:
            # Load all files for the specified date filter
            mappings = datasrc.mappings(datefilter)

        #TODO: Notify user if there's a mismatch between expected files and
        # cache.diff
        for mapping in mappings:
            loader.run(mapping)
