import json
import re
from .base import get

def find(state, datefilter=''):
    kwargs = {
        'state__postal__iexact': state.strip(),
    }
    if datefilter:
        kwargs.update({
            'start_date__contains': datefilter
        })
    response = get(resource_type='election', params=kwargs)
    if response.status_code == 200:
        payload = json.loads(response.content)['objects']
    else:
        msg = "Request raised error: %s (state: %s, datefilter: %s)"
        payload =  msg % (response.status_code, state, datefilter)
    return payload
