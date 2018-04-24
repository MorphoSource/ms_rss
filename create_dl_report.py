import db
import media_download_stats as stats
import pandas

def get_num_mf_derived_from(mf_id):
    conn = db.db_conn()
    c = conn.cursor()
    sql = """ 
        SELECT * FROM `ms_media_files`
        WHERE `derived_from_media_file_id` = %s
    """
    db_result = db.db_execute(c, sql, mf_id)
    return len(db_result)

def get_num_m_derived_from(m_id):
    conn = db.db_conn()
    c = conn.cursor()
    sql = """ 
        SELECT * FROM `ms_media`
        WHERE `derived_from_media_id` = %s
    """
    db_result = db.db_execute(c, sql, m_id)
    return len(db_result)

def gen_csv(recordset, csv_path):
	conn = db.db_conn_socket()
	c = conn.cursor()

	sql = """
		SELECT * FROM `ms_media_download_stats` AS s
		LEFT JOIN `ca_users` AS u ON u.user_id = s.user_id
		LEFT JOIN `ms_media_files` AS mf ON mf.media_file_id = s.media_file_id
		LEFT JOIN `ms_media` AS m ON m.media_id = s.media_id
		LEFT JOIN `ms_specimens` as sp ON sp.specimen_id = m.specimen_id
		LEFT JOIN `ms_specimens_x_taxonomy` AS sxt ON sxt.specimen_id = sp.specimen_id
	    LEFT JOIN `ms_taxonomy_names` AS t ON t.alt_id = sxt.alt_id
	    WHERE `recordset` = %s
		"""

	# sql = """ 
	# 		SELECT * FROM `ms_media_files` AS mf
	# 		LEFT JOIN `ms_media` AS m ON m.media_id = mf.media_id
	# 		LEFT JOIN `ms_specimens` AS s ON s.specimen_id = m.specimen_id
	#         LEFT JOIN `ms_specimens_x_taxonomy` AS sxt ON sxt.specimen_id = s.specimen_id
	#         LEFT JOIN `ms_taxonomy_names` AS t ON t.alt_id = sxt.alt_id
	# 		WHERE `recordset` = %s 
	# 		"""

	download_requests = db.db_execute(c, sql, recordset)

	mf_dict = {}
	mg_dict = {}
	for dl in download_requests:
		if 'media_file_id' in dl and dl['media_file_id'] is not None:
			mf_id = dl['media_file_id']
			if mf_id not in mf_dict:
				mf_dict[mf_id] = []
			mf_dict[mf_id].append(dl)
		if 'media_id' in dl and dl['media_id'] is not None:
			mg_id = dl['media_id']
			if mg_id not in mg_dict:
				mg_dict[mg_id] = []
			mg_dict[mg_id].append(dl)


	m_stats = {}

	for mg_id, mg_array in mg_dict.iteritems():
		m_stats[mg_id] = {
			'mg_array': mg_array,
			'mg_stats': stats.MediaDownloadStats(mg_array),
			'mf_dict': {},
			'mf_stats_dict': {}
		}

	for mf_id, mf_array in mf_dict.iteritems():
		mg_id = mf_array[0]['media_id']
		m_stats[mg_id]['mf_dict'][mf_id] = mf_array
		m_stats[mg_id]['mf_stats_dict'][mf_id] = stats.MediaDownloadStats(mf_array)

	usage_report = pandas.DataFrame(columns=
	    ['media_file_id',
	    'media_file_derived_from',
	    'num_media_files_derived_from_this',
	    'media_group_id',
	    'media_group_derived_from',
	    'num_media_groups_derived_from_this',
	    'specimen_id',
	    'specimen_institution_code',
	    'specimen_collection_code',
	    'specimen_catalog_number',
	    'specimen_genus',
	    'specimen_species',
	    'total_downloads',
	    'dl_intended_use_School',
	    'dl_intended_use_School_K_6',
	    'dl_intended_use_School_7_12',
	    'dl_intended_use_School_College_Post_Secondary',
	    'dl_intended_use_School_Graduate_school',
	    'dl_intended_use_Education',
	    'dl_intended_use_Education_K_6',
	    'dl_intended_use_Education_7_12',
	    'dl_intended_use_Education_College_Post_Secondary',
	    'dl_intended_use_Educaton_general',
	    'dl_intended_use_Education_museums_public_outreach',
	    'dl_intended_use_Personal_interest',
	    'dl_intended_use_Research',
	    'dl_intended_use_Commercial',
	    'dl_intended_use_Art',
	    'dl_intended_use_other',
	    'dl_intended_use_3d_print',
	    'total_download_users',
	    'u_affiliation_Student',
	    'u_affiliation_Student:_K-6',
	    'u_affiliation_Student:7-12',
	    'u_affiliation_Student:_College/Post-Secondary',
	    'u_affiliation_Student:_Graduate',
	    'u_affiliation_Faculty',
	    'u_affiliation_Faculty:_K-6',
	    'u_affiliation_Faculty:7-12',
	    'u_affiliation_Faculty_College/Post-Secondary',
	    'u_affiliation_Staff:_College/Post-Secondary',
	    'u_affiliation_General_Educator',
	    'u_affiliation_Museum',
	    'u_affiliation_Museum_Curator',
	    'u_affiliation_Museum_Staff',
	    'u_affiliation_Librarian',
	    'u_affiliation_IT',
	    'u_affiliation_Private_Individual',
	    'u_affiliation_Researcher',
	    'u_affiliation_Private_Industry',
	    'u_affiliation_Artist',
	    'u_affiliation_Government',
	    'u_affiliation_other',
	    ])

	for m_id, s in m_stats.iteritems():
	    # Add media group row
	    mg = s['mg_array'][0]
	    mg_stats = s['mg_stats']
	    row = {
	        'media_file_id': None,
	        'media_file_derived_from': None,
	        'num_media_files_derived_from_this': None,
	        'media_group_id': m_id,
	        'media_group_derived_from': mg['derived_from_media_id'],
	        'num_media_groups_derived_from_this': get_num_m_derived_from(m_id),
	        'specimen_id': mg['specimen_id'],
	        'specimen_institution_code': mg['institution_code'],
	        'specimen_collection_code': mg['collection_code'],
	        'specimen_catalog_number': mg['catalog_number'],
	        'specimen_genus': mg['genus'],
	        'specimen_species': mg['species'],
	        'total_downloads': mg_stats.total_downloads,
	        'total_download_users': mg_stats.total_users
	    }
	    for use, num in mg_stats.intended_use_dict.iteritems():
	        row['dl_intended_use_' + use] = num
	    for demo, num in mg_stats.user_demo_dict.iteritems():
	        row['u_affiliation_' + demo.replace(' ', '_')] = num

	    usage_report = usage_report.append(row, ignore_index=True)

	    # Add media files row
	    for mf_id, mf_array in s['mf_dict'].iteritems():
	        mf_stats = s['mf_stats_dict'][mf_id]
	        mf = mf_array[0]
	        row = {
	            'media_file_id': mf_id,
	            'media_file_derived_from': mf['derived_from_media_file_id'],
	            'num_media_files_derived_from_this': get_num_mf_derived_from(mf_id),
	            'media_group_id': mf['media_id'],
	            'media_group_derived_from': mf['derived_from_media_id'],
	            'num_media_groups_derived_from_this': get_num_m_derived_from(mf['media_id']),
	            'specimen_id': mf['specimen_id'],
	            'specimen_institution_code': mf['institution_code'],
	            'specimen_collection_code': mf['collection_code'],
	            'specimen_catalog_number': mf['catalog_number'],
	            'specimen_genus': mf['genus'],
	            'specimen_species': mf['species'],
	            'total_downloads': mf_stats.total_downloads,
	            'total_download_users': mf_stats.total_users
	        }
	        for use, num in mf_stats.intended_use_dict.iteritems():
	            row['dl_intended_use_' + use] = num
	        for demo, num in mf_stats.user_demo_dict.iteritems():
	            row['u_affiliation_' + demo.replace(' ', '_')] = num

	        usage_report = usage_report.append(row, ignore_index=True)

	usage_report.to_csv(csv_path, index=False, index_label=False)
