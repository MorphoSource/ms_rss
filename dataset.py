from hashlib import md5
from os import makedirs
from os.path import join, isdir, isfile

import create_audubon_core
import create_dl_report
import create_dl_request_report
import create_eml

R_FILES = {
	'datasets': ['ms.csv', 'dl.csv', 'dl_request.csv'],
	'eml': ['ms.xml', 'dl.xml', 'dl_request.xml']
}

def mkdir_if_not_exists(directory):
	if not isdir(directory):
		makedirs(directory)

def r_files_exist(r_dir):
	for r_file_subdir, r_file_array in R_FILES.iteritems():
		for filename in r_file_array:
			if not isfile(join(r_dir, r_file_subdir, filename)):
				return False
	return True

def r_files_differ(tmp_dir, r_dir, r):
	print 'creating audubon core csv'
	create_audubon_core.gen_csv(r, join(tmp_dir, 'ms.csv'))
	print 'creating dl report'
	create_dl_report.gen_csv(r, join(tmp_dir, 'dl.csv'))
	print 'creating dl request report'
	create_dl_request_report.gen_csv(r, join(tmp_dir, 'dl_request.csv'))
	for filename in ['ms.csv', 'dl.csv', 'dl_request.csv']:
		tmp_md5 = md5(open(join(tmp_dir, filename), 'rb').read()).hexdigest()
		cur_md5 = md5(open(join(r_dir, filename), 'rb').read()).hexdigest()
		if tmp_md5 != cur_md5:
			return 1
	return 0

def gen_recordset_datasets(r, r_name, p, p_name, r_dir):
	mkdir_if_not_exists(r_dir)
	mkdir_if_not_exists(join(r_dir, 'datasets'))
	mkdir_if_not_exists(join(r_dir, 'eml'))
	create_audubon_core.gen_files(r, r_name, p, p_name, r_dir)
	create_dl_report.gen_files(r, r_name, p, p_name, r_dir)
	create_dl_request_report.gen_files(r, r_name, p, p_name, r_dir)

def gen_all_datasets(pub_dict, p_dir):
	for r, r_dict in pub_dict['recordsets'].iteritems():
		print('Recordset: ' + r)		
		gen_recordset_datasets(r, r_dict['name'], pub_dict['id'], pub_dict['name'], join(p_dir, r))

def create_datasets(publishers, root_dir):
	for pub_uuid, pub_dict in publishers.iteritems():
		print('Publisher: ' + pub_uuid)
		p_dir = join(root_dir, pub_uuid)
		mkdir_if_not_exists(p_dir)
		gen_all_datasets(pub_dict, p_dir)