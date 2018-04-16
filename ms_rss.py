from hashlib import md5
from os import makedirs
from os.path import join, isdir, isfile

import create_audubon_core
import create_dl_report
import create_dl_request_report
import create_rss
import credentials
import db

def mkdir_if_not_exists(directory):
	if not isdir(directory):
		makedirs(directory)

def create_new_datasets(recordset, d_dir):
	create_rss.gen_rss(recordset, join(r_dir, 'ms.rss'))
	create_audubon_core.gen_csv(recordset, join(d_dir, 'ms.csv'))
	create_dl_report.gen_csv(recordset, join(d_dir, 'dl.csv'))
	create_dl_request_report.gen_csv(recordset, join(d_dir, 'dl_request.csv'))

rss_dir = '/home/rapiduser/rss/'
tmp_dir = '/home/rapiduser/tmp/'

# Get recordsets
conn = db.db_conn_socket()
c = conn.cursor()
sql = """
	SELECT DISTINCT `recordset` FROM `ms_specimens`
	"""

recordsets = db.db_execute(c, sql)
recordsets = [r['recordset'] for r in recordsets if 'recordset' in r and r['recordset'] is not None]

for r in recordsets:
	print(r)
	r_dir = join(rss_dir, r)
	d_dir = join(r_dir, 'datasets')
	# Do RSS/Dataset files already exist?
	if (isdir(r_dir) and isdir(d_dir) and isfile(join(r_dir, 'ms.rss')) and
        isfile(join(d_dir, 'ms.csv')) and isfile(join(d_dir, 'dl.csv')) and
        isfile(join(d_dir, 'dl_request.csv'))
	   ):
		# Do Dataset files need to be updated?
		create_audubon_core.gen_csv(r, join(tmp_dir, 'ms.csv'))
		create_dl_report.gen_csv(r, join(tmp_dir, 'dl.csv'))
		create_dl_request_report.gen_csv(r, join(tmp_dir, 'dl_request.csv'))

		file_diff = 0
		for filename in ['ms.csv', 'dl.csv', 'dl_request.csv']:
			tmp_md5 = md5(open(join(tmp_dir, filename, 'rb')).read()).hexdigest()
			cur_md5 = md5(open(join(d_dir, filename, 'rb')).read()).hexdigest()
			if tmp_md5 != cur_md5:
				file_diff = 1

		if file_diff:
			create_new_datasets(r, d_dir)
	else:
		# Create new
		mkdir_if_not_exists(r_dir)
		mkdir_if_not_exists(d_dir)
		create_new_datasets(r, d_dir)
		