import db
import ms_media_file
import pandas

def gen_csv(recordset, csv_path):
	conn = db.db_conn_socket()
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
		'ac:associatedSpecimenReference',
		'ac:providerManagedID',
		'ac:derivedFrom',
		'ac:providerLiteral',
		'ac:provider',
		'dc:type',
		'dcterms:type',
		'ac:subtypeLiteral',
		'ac:subtype',
		'ac:accessURI',
		'dc:format',
		'ac:subjectPart',
		'ac:subjectOrientation',
		'ac:caption',
		'Iptc4xmpExt:LocationCreated',
		'ac:captureDevice',
		'dc:creator',
		'ms:scanningTechnician',
		'ac:fundingAttribution',
		'exif:Xresolution',
		'exif:Yresolution',
		'dicom:SpacingBetweenSlices',
		'dc:rights',
		'dcterms:rights',
		'xmpRights:Owner',
		'xmpRights:UsageTerms',
		'xmpRights:WebStatement',
		'ac:licenseLogoURL',
		'photoshop:Credit',
		'coreid'])

	for mf_dict in r:
		mf = ms_media_file.MsMediaFile(mf_dict)

		if mf.is_published():
			if len(mf.ac_mf_dict.keys()) > 0:
				ac = ac.append(mf.ac_mf_dict, ignore_index=True)
			if len(mf.ac_mfp_dict.keys()) > 0:
				ac = ac.append(mf.ac_mfp_dict, ignore_index=True)

	ac.to_csv(csv_path, index=False, index_label=False, encoding='utf-8')

