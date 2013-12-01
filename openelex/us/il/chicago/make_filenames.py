import json
import csv
from itertools import groupby
from operator import itemgetter

def transmogrify():
    with open('precinct_pages.json', 'rb') as f:
        pages = json.load(f)
    dates = list(csv.DictReader(open('election_dates.csv', 'rb')))
    template = {
        'ballots': [],
        'date': '',
        'election_id': '',
        'name': '',
        'ocd_id': 'ocd-division/country:us/state:il/place:chicago',
        'results': [],
        'url': '',
        'voters': [],
        'year': '',
    }
    all_elex = []
    for election, races in pages.items():
        item = template.copy()
        election_id = '-'.join(election.lower().replace(',', '').split(' '))
        for e_date in dates:
            if e_date['Election'] == election:
                item['election_id'] = 'il-%s-%s' % (e_date['Date'], election_id)
                item['year'] = e_date['Date'].split('-')[0]
                item['url'] = e_date['URL']
                item['date'] = e_date['Date']
        for race_name, urls in races.items():
            if 'ballots' in race_name.lower():
                item['ballots'].extend(urls)
            elif 'registered' in race_name.lower():
                item['voters'].extend(urls)
            else:
                item['results'].append({'result_name': race_name, 'results_links': urls})
        item['name'] = election
        all_elex.append(item)
    with_years = sorted(all_elex, key=itemgetter('year'))
    elections = {}
    for y, group in groupby(with_years, key=itemgetter('year')):
        elections[y] = list(group)
    fnames = open('mappings/filenames.json', 'wb')
    fnames.write(json.dumps(elections))
    fnames.close()

if __name__ == "__main__":
    transmogrify()
