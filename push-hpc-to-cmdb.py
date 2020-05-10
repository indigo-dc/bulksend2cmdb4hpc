#!/usr/bin/env python

import argparse
from datetime import datetime
import logging
import pytz
import subprocess

import requests
import simplejson as json
from six.moves import urllib
import yaml


logging.basicConfig(level=logging.DEBUG)
logging.getLogger('requests').setLevel(logging.DEBUG)
logging.getLogger('urllib').setLevel(logging.DEBUG)
logging.getLogger('json').setLevel(logging.DEBUG)


opts = None
provider_id = 'PSNC'
services = {
    'https://deep.eagle.man.poznan.pl': 'https://deep.eagle.man.poznan.pl',
    'https://qcg-deep.apps.paas-dev.psnc.pl/api': '/cip/bin/run-qcg.sh'
}


def cmdb_get_request(url_endpoint):
    '''Performs GET HTTP requests to CMDB

    :url_endpoint: URL endpoint
    '''
    _l = []
    url = urllib.parse.urljoin(
        opts.cmdb_read_endpoint, url_endpoint)
    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        r_json = r.json()
        if 'error' not in r_json.keys():
            # 'provider' has no rows
            if 'rows' not in r_json.keys():
                _l.append(r_json)
            else:
                for item in r_json['rows']:
                    _l.append(item['doc'])
        else:
            logging.debug('Got CMDB error in HTTP request: %s' % r_json)
    return _l


def set_bulk_format(json_data):
    '''Set JSON data according to CouchDB format for bulk operations.

    :json_data: JSON data
    '''
    d = {}
    d['docs'] = json_data
    return json.dumps(d)


def cmdb_bulk_post(json_data):
    '''Performs BULK POST HTTP request to CMDB

    :json_data: JSON data
    '''
    headers = {
        'Content-Type': 'application/json',
    }
    url = urllib.parse.urljoin(
        opts.cmdb_write_endpoint,
        '_bulk_docs')
    url = opts.cmdb_write_endpoint + '/_bulk_docs'
    logging.debug("BULK POSTING TO %s" % url)
    bulk_json_data = set_bulk_format(json_data)
    s = requests.Session()
    s.auth = (opts.cmdb_db_user, opts.cmdb_db_pass)
    r = s.post(url, headers=headers, data=bulk_json_data)
    logging.debug('Result/s of BULK POST: %s' % r.content)


def get_from_cmdb(entity, parent):
    if entity == 'provider':
        url_endpoint = 'provider/id/%s?include_docs=true' % parent
    elif entity == 'service':
        url_endpoint = ('service/filters/provider_id/%s'
                        '?include_docs=true') % parent

    return cmdb_get_request(url_endpoint)


def get_provider_data():
    with open(opts.provider_config_file, 'r') as config_file:
        try:
            _data = yaml.safe_load(config_file)
        except yaml.YAMLError as exc:
            logging.error(exc)

    d = {
        'type': 'provider',
        'owners': _data['site']['owner_contacts_iam'],
        'data': {
            'name': _data['site']['name'],
            'country': _data['site']['country'],
            'country_code': _data['site']['country_code'],
            'roc': _data['site']['roc'],
            'is_public': _data['site']['is_public'],
            'owners': _data['site']['owner_contacts']
        }
    }

    return d


def validate_timestamp(time_str):
    o1 = datetime.strptime(time_str, '%Y%m%d %H%M%S')
    now = datetime.now(pytz.timezone('Europe/Madrid'))
    naive = now.replace(tzinfo=None)
    diff = naive - o1
    logging.info("Total seconds difference: %s" % diff.total_seconds())


def get_input_opts():
    '''Manage input arguments.'''
    parser = argparse.ArgumentParser(description=('CIP->CMDBv1 data pusher.'))
    parser.add_argument('--cmdb-read-endpoint',
                        metavar='URL',
                        help='Specify CMDB read URL')
    parser.add_argument('--cmdb-write-endpoint',
                        metavar='URL',
                        help='Specify CMDB write URL')
    parser.add_argument('--cmdb-db-user',
                        metavar='USERNAME',
                        help=('With password authentication, this specifies '
                              'CMDB username'))
    parser.add_argument('--cmdb-db-pass',
                        metavar='PASSWORD',
                        help=('With password authentication, this specifies '
                              'CMDB password'))
    parser.add_argument('--provider-config-file',
                        metavar='FILE',
                        help=('Location of the CIP-like configuration file '
                              'for the provider'))
    return parser.parse_args()


def main():
    global opts
    opts = get_input_opts()

    records = []
    # entity == provider
    provider_data = get_from_cmdb('provider', provider_id)
    _data = get_provider_data()
    if provider_data:
        _data['_id'] = provider_data[0]['_id']
        _data['_rev'] = provider_data[0]['_rev']
    records.append(_data)

    # entity == service
    service_data = get_from_cmdb('service', provider_id)
    
    for service_id, info_provider in services.items():
        _data = {}
        o = urllib.parse.urlparse(info_provider)
        if o.scheme in ['http', 'https']:
            r = requests.get(info_provider)
            if r.status_code == requests.codes.ok:
                _data['data'] = r.json()
                # WORKAROUND: data['endpoint'] must be always provided
                # AND match the API endpoint being requested. 
                # This is NOT fulfilled by deep.eagle.man.poznan.pl
                if 'endpoint' not in _data['data'].keys():
                    _data['data']['endpoint'] = info_provider
                validate_timestamp(_data['data']['timestamp'])
        else:
            # run cmd
            try:
                r = subprocess.Popen(info_provider,
                                     shell=True,
                                     stdout=subprocess.PIPE).stdout.read()
            except Exception:
                r = {}
            r = json.loads(r)
            # KEYS: [u'provider_id', u'endpoint', u'sitename', u'config', u'queues', u'hostname', u'date', u'service_type', u'nodes', u'type']
            #r.pop(u'config')
            _data['data'] = r
        _data['type'] = 'service'
        # Set id and revision if record is already in CMDB
        for srv in service_data:
        #    if srv['_id'] == service_id:
            if srv['data']['endpoint'] == service_id:
                logging.info(("Revision for service '%s' (already in "
                              "CMDB): %s" % (service_id, srv['_rev'])))
                _data['_id'] = srv['_id']
                _data['_rev'] = srv['_rev']
        records.append(_data)
    #logging.debug(json.dumps(records, indent=4))
    
    cmdb_bulk_post(records)

if __name__ == '__main__':
    main()
