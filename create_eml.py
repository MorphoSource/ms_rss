from os.path import join, split
import xml.etree.ElementTree as ET
import time

def convert_filepath_to_urlpath(filepath):
	return 'https://www.morphosource.org/rss' + filepath.split('tmp')[1]

def gen_eml_file(title, desc, link, ac, recordset, r_name, publisher, p_name, filepath):
	email = 'info@morphosource.org'
	url = 'https://www.morphosource.org'
	org = 'MorphoSource'
	pm_given_name = 'Julie'
	pm_sur_name = 'Winchester'

	eml = ET.Element('eml:eml', {
		'xmlns:eml': 'eml://ecoinformatics.org/eml-2.1.1',
		'xmlns:d': 'eml://ecoinformatics.org/dataset-2.1.1',
		'xmlns:res': 'eml://ecoinformatics.org/resource-2.1.1',
		'xmlns:dc': 'http://purl.org/dc/terms/',
		'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
		'xsi:schemaLocation': 'eml://ecoinformatics.org/eml-2.1.1 http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd',
		'xml:lang': 'eng'
		})

	dataset = ET.SubElement(eml, 'dataset')

	ds_alt_id = ET.SubElement(dataset, 'alternateIdentifier')
	ds_alt_id.text = convert_filepath_to_urlpath(filepath)

	ds_title = ET.SubElement(dataset, 'title')
	ds_title.text = title

	metadata_provider = ET.SubElement(dataset, 'metadataProvider')
	mp_org_name = ET.SubElement(metadata_provider, 'organizationName')
	mp_org_name.text = org
	mp_ind_name = ET.SubElement(metadata_provider, 'individualName')
	mp_giv_name = ET.SubElement(mp_ind_name, 'givenName')
	mp_giv_name.text = pm_given_name
	mp_sur_name = ET.SubElement(mp_ind_name, 'surName')
	mp_sur_name.text = pm_sur_name
	mp_email = ET.SubElement(metadata_provider, 'electronicmailAddress')
	mp_email.text = email
	mp_url = ET.SubElement(metadata_provider, 'onlineUrl')
	mp_url.text = url

	associated_party = ET.SubElement(dataset, 'associatedParty')
	ap_ind_name = ET.SubElement(associated_party, 'individualName')
	ap_giv_name = ET.SubElement(ap_ind_name, 'givenName')
	ap_giv_name.text = 'Doug'
	ap_sur_name = ET.SubElement(ap_ind_name, 'surName')
	ap_sur_name.text = 'Boyer'
	ap_position = ET.SubElement(associated_party, 'positionName')
	ap_position.text = 'MorphoSource Director'
	ap_email = ET.SubElement(associated_party, 'electronicMailAddress')
	ap_email.text = 'doug.boyer@duke.edu'

	pub_date = ET.SubElement(dataset, 'pubDate')
	pub_date.text = ''

	lang = ET.SubElement(dataset, 'language')
	lang.text = 'English'

	abstract = ET.SubElement(dataset, 'abstract')
	abstract.text = desc

	if ac:
		keyword_set = ET.SubElement(dataset, 'keywordSet')
		keyword = ET.SubElement(keyword_set, 'keyword')
		keyword.text = 'Multimedia'
		keyword_thes = ET.SubElement(keyword_set, 'keywordThesaurus')
		keyword_thes.text = 'Audubon Core Terms: http://rs.tdwg.org/ac/terms/Multimedia'

	rights = ET.SubElement(dataset, 'intellectualRights')
	rights_para = ET.SubElement(rights, 'para')
	rights_para.text = 'The metadata described here and in the resource are licensed by creative commons CC-BY-NC. The copyright and licensing statements of the multimedia described by the resource can be found in the resource metadata.'

	online = ET.SubElement(dataset, 'online')
	online.text = link

	maint = ET.SubElement(dataset, 'maintenance')
	maint_desc = ET.SubElement(maint, 'description')
	maint_desc_para = ET.SubElement(maint_desc, 'para')
	maint_desc_para.text = 'Resource updated daily.'
	maint_up_freq = ET.SubElement(maint, 'maintenanceUpdateFrequency')
	maint_up_freq.text = 'daily'

	contact = ET.SubElement(dataset, 'contact')
	c_ind_name = ET.SubElement(contact, 'individualName')
	c_giv_name = ET.SubElement(c_ind_name, 'givenName')
	c_giv_name.text = pm_given_name
	c_sur_name = ET.SubElement(c_ind_name, 'surName')
	c_sur_name.text = pm_sur_name
	c_org_name = ET.SubElement(contact, 'organizationName')
	c_org_name.text = org
	c_email = ET.SubElement(contact, 'electronicMailAddress')
	c_email.text = email
	c_url = ET.SubElement(contact, 'onlineUrl')
	c_url.text = url

	addt_metadata = ET.SubElement(eml, 'additionalMetadata')
	metadata = ET.SubElement(addt_metadata, 'metadata')
	idigbio = ET.SubElement(metadata, 'idigbio')
	recordset_section = ET.SubElement(idigbio, 'recordset')
	recordset_id = ET.SubElement(recordset_section, 'id')
	recordset_id.text = recordset
	recordset_name = ET.SubElement(recordset_section, 'name')
	recordset_name.text = r_name
	publisher_section = ET.SubElement(idigbio, 'publisher')
	publisher_id = ET.SubElement(publisher_section, 'id')
	publisher_id.text = publisher
	publisher_name = ET.SubElement(publisher_section, 'name')
	publisher_name.text = p_name
	morphosource = ET.SubElement(metadata, 'morphosource')
	pub_time = ET.SubElement(morphosource, 'pubTime')
	pub_time.text = time.strftime("%a, %d %b %Y %H:%M:%S %z")

	eml_tree = ET.ElementTree(eml)
	eml_tree.write(filepath, xml_declaration=True, encoding='utf-8')
	return eml_tree

def gen_eml_files(rss_tree, eml_dir):
	rss = rss_tree.getroot()
	for item in rss.iter('item'):
		title = item.find('title').text
		desc = item.find('description').text
		link = item.find('link').text
		filename = split(link)[1][:-4]+'.xml'
		if filename == 'ms.xml':
			ac = True
		else:
			ac = False
		filepath = join(eml_dir, filename)
		gen_eml_file(title, desc, link, ac, filepath)
