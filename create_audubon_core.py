from os import makedirs
from os.path import join, split, isdir

import create_eml
import db
import ms_media_file
import pandas

def mkdir_if_not_exists(directory):
	if not isdir(directory):
		makedirs(directory)

def convert_filepath_to_urlpath(filepath):
	return 'https://www.morphosource.org/rss' + filepath.split('tmp')[1]

def gen_csv(recordset, csvpath):
	mkdir_if_not_exists(split(csvpath)[0])

	conn = db.db_conn()
	c = conn.cursor()

	sql = """ SELECT * FROM `ms_specimens` AS s 
			  INNER JOIN `ms_media` AS m ON s.specimen_id = m.specimen_id 
			  INNER JOIN `ms_media_files` AS mf ON m.media_id = mf.media_id
			  INNER JOIN `ms_facilities` AS f ON m.facility_id = f.facility_id
			  INNER JOIN `ms_scanners` AS sc ON m.scanner_id = sc.scanner_id
			  INNER JOIN `ca_users` AS u ON m.user_id = u.user_id
			  WHERE s.recordset = %s """	

	r = db.db_execute(c, sql, recordset)

	ac = pandas.DataFrame(columns=
		['dcterms:identifier', 
		'associatedSpecimenReference',
		'providerManagedID',
		'derivedFrom',
		'providerLiteral',
		'provider',
		'dc:type',
		'dcterms:type',
		'subtypeLiteral',
		'subtype',
		'accessURI',
		'dc:format',
		'subjectPart',
		'subjectOrientation',
		'caption',
		'LocationCreated',
		'captureDevice',
		'dc:creator',
		'ms:scanningTechnician',
		'fundingAttribution',
		'exif:Xresolution',
		'exif:Yresolution',
		'dicom:SpacingBetweenSlices',
		'dc:rights',
		'dcterms:rights',
		'Owner',
		'UsageTerms',
		'WebStatement',
		'licenseLogoURL',
		'Credit',
		'coreid',
		'exif:PixelXDimension',
		'exif:PixelYDimension',
		'exif:ResolutionUnit',
		'IDofContainingCollection'])

	print len(r)

	for mf_dict in r:
		print mf_dict['media_id']
		mf = ms_media_file.MsMediaFile(mf_dict)

		if mf.is_published():
			if len(mf.ac_mf_dict.keys()) > 0:
				ac = ac.append(mf.ac_mf_dict, ignore_index=True)
			if len(mf.ac_mfp_dict.keys()) > 0:
				ac = ac.append(mf.ac_mfp_dict, ignore_index=True)

	ac.to_csv(csvpath, index=False, index_label=False, encoding='utf-8')

def gen_eml(recordset, r_name, publisher, p_name, xmlpath, csvpath):
	if not r_name:
		r_name = 'unnamed'
	r_str = r_name + ' recordset (iDigBio UUID ' + recordset + ')'
	title = 'MorphoSource media, Audubon Core format for ' + r_str
	desc = 'List of all MorphoSource media associated with ' + r_str + ' formatted using Darwin Core/Audubon Core metadata standard.'
	link = convert_filepath_to_urlpath(csvpath)
	ac = True
	create_eml.gen_eml_file(title, desc, link, ac, recordset, r_name, publisher, p_name, xmlpath)

def gen_files(recordset, r_name, publisher, p_name, dirpath):
	csvpath = join(dirpath, 'datasets', 'ms.csv')
	gen_csv(recordset, csvpath)
	gen_eml(recordset, r_name, publisher, p_name, join(dirpath, 'eml', 'ms.xml'), csvpath)