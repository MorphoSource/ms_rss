from os import walk
from os.path import join

import xml.etree.ElementTree as ET
import time

def convert_filepath_to_urlpath(filepath):
	return 'https://www.morphosource.org/rss' + filepath.split('tmp')[1]

def gen_xml_item(et_parent, title, desc, pub_date, link, eml_link):
	item = ET.SubElement(et_parent, 'item')
	item_title = ET.SubElement(item, 'title')
	item_title.text = title
	item_id = ET.SubElement(item, 'id')
	item_id.text = link
	item_type = ET.SubElement(item, 'type')
	item_type.text = 'CSV'
	item_desc = ET.SubElement(item, 'description')
	item_desc.text = desc
	item_link = ET.SubElement(item, 'link')
	item_link.text = link
	item_eml = ET.SubElement(item, 'ipt:eml')
	item_eml.text = eml_link
	item_pubDate = ET.SubElement(item, 'pubDate')
	item_pubDate.text = pub_date
	return et_parent

def gen_rss(rss_dir):
	rss_file_path = join(rss_dir, 'ms_rss.xml')

	rss = ET.Element('rss', {
		'xmlns:ipt': 'http://ipt.gbif.org',
		'version': '2.0'
		})

	channel = ET.SubElement(rss, 'channel')
	title = ET.SubElement(channel, 'title')
	title.text = 'MorphoSource RSS feed'
	link = ET.SubElement(channel, 'link')
	link.text = 'https://www.morphosource.org/rss/ms.rss'
	description = ET.SubElement(channel, 'description')
	description.text = 'RSS feed describing and summarizing MorphoSource media, downloads of MorphoSource media, and requests to download MorphoSource media representing specimens reported to iDigBio'
	language = ET.SubElement(channel, 'language')
	language.text = 'en-us'

	for path, dirs, files in walk(rss_dir):
		xml_files = [f for f in files if '.xml' in f and f != 'ms_rss.xml']
		for f in xml_files:
			eml = ET.parse(join(path, f)).getroot()
			title = eml.find('dataset').find('title').text
			desc = eml.find('dataset').find('abstract').text
			pub_date = eml.find('additionalMetadata').find('metadata').find('morphosource').find('pubTime').text
			link = eml.find('dataset').find('online').text
			eml_link = convert_filepath_to_urlpath(join(path, f))
			channel = gen_xml_item(channel, title, desc, pub_date, link, eml_link)

	rss_tree = ET.ElementTree(rss)
	rss_tree.write(rss_file_path, xml_declaration=True, encoding='utf-8')
	return rss_tree
