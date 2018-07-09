from hashlib import md5
from os import listdir, makedirs, unlink
from os.path import join, isdir, isfile
from shutil import move, rmtree

import create_rss
import credentials
import dataset
import db
import requests


def mkdir_if_not_exists(directory):
	if not isdir(directory):
		makedirs(directory)

def get_json_field(json, field):
	if type(field) is list or type(field) is tuple:
		if field[0] in json:
			new_field = field[1:]
			if len(new_field) == 1:
				new_field = new_field[0]	
			return get_json_field(json[field[0]], new_field)
		else:
			return False
	else:
		if field in json:
			return json[field]
		else:
			return False

rss_dir = '/opt/rh/httpd24/root/var/www/html/rss/'
tmp_dir = '/opt/rh/httpd24/root/var/www/html/rss/tmp/'

# Get recordsets
conn = db.db_conn()
c = conn.cursor()
sql = """
	SELECT DISTINCT `recordset` FROM `ms_specimens`
	"""

recordsets = db.db_execute(c, sql)
recordsets = [r['recordset'] for r in recordsets if 'recordset' in r and r['recordset'] is not None]

publisher_dict = {}
for r in recordsets:
	resp = requests.get('https://search.idigbio.org/v2/search/recordsets?rsq={%22uuid%22:%22' + r + '%22}')
	resp = resp.json()
	if resp['itemCount'] != 1:
		raise ValueError('More than one recordset found for UUID ' + r)
	else:
		pub_uuid = resp['items'][0]['indexTerms']['publisher']
		pub_resp = requests.get('https://search.idigbio.org/v2/search/publishers?pq={%22uuid%22:%22' + pub_uuid + '%22}')
		pub_resp = pub_resp.json()
		if pub_resp['itemCount'] != 1:
			raise ValueError('More than one publisher found for UUID ' + pub_uuid)
		if pub_uuid not in publisher_dict:
			publisher_dict[pub_uuid] = {
				'name': get_json_field(pub_resp['items'][0], ['data', 'name']),
				'id': pub_uuid,
				'json': pub_resp['items'][0],
				'recordsets': {}
			}
		publisher_dict[pub_uuid]['recordsets'][r] = {
			'name': get_json_field(resp['items'][0], ['data', 'collection_name']),
			'id': r,
			'json': resp['items'][0]
		}

# Create files in temporary directory

print "Creating files"
dataset.create_datasets(publisher_dict, tmp_dir)
create_rss.gen_rss(tmp_dir)

# Delete everything in rss directory except tmp

print "Deleting pre-existing files"
for f in listdir(rss_dir):
	f_path = join(rss_dir, f)
	if isfile(f_path):
		unlink(f_path)
	elif isdir(f_path) and f != 'tmp':
		rmtree(f_path)

# Move files from temporary directory to rss directory

print "Moving newly generated files into directory"
for f in listdir(tmp_dir):
	f_path = join(tmp_dir, f)
	move(f_path, rss_dir)
	if isfile(f_path):
		unlink(f_path)
	elif isdir(f_path):
		rmtree(f_path)