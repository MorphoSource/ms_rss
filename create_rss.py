import xml.etree.ElementTree as ET

import time

def gen_rss(recordset, rss_file_path):
	rss = ET.Element('rss', {
		'xmlns:ipt': 'http://ipt.gbif.org/',
		'version': '2.0'
		})

	channel = ET.SubElement(rss, 'channel')
	title = ET.SubElement(channel, 'title')
	title.text = 'MorphoSource Media RSS Feed for Recordset ' + recordset
	link = ET.SubElement(channel, 'link')
	link.text = 'https://www.morphosource.org/rss/' + recordset + '/ms.rss'
	description = ET.SubElement(channel, 'description')
	description.text = 'RSS feed describing MorphoSource media, downloads of MorphoSource media, and requests to download MorphoSource media associated with recordset ' + recordset + '.'
	language = ET.SubElement(channel, 'language')
	language.text = 'en-us'

	item_data = [{
		'title': 'MorphoSource Media, Audubon Core format',
		'description': 'List of all MorphoSource media associated with recordset ' + recordset + ' formatted using Darwin Core/Audubon Core metadata standard.',
		'filename': 'ms.csv'
	},
	{
		'title': 'Download Report for MorphoSource Media',
		'description': 'Report of downloads of MorphoSource media associated with recordset ' + recordset + ' with intended download uses and downloading user profile affiliations.',
		'filename': 'dl.csv'
	},
	{
		'title': 'Download Request Report for MorphoSource Media',
		'description': 'Report of all download requests for MorphoSource media associated with recordset ' + recordset + '.',
		'filename': 'dl_request.csv'
	}]

	for item in item_data:
		link_text = 'https://www.morphosource.org/rss/' + recordset + '/datasets/' + item['filename']
		item_tag = ET.SubElement(channel, 'item')
		item_title = ET.SubElement(item_tag, 'title')
		item_title.text = item['title']
		item_id = ET.SubElement(item_tag, 'id')
		item_id.text = link_text
		item_type = ET.SubElement(item_tag, 'type')
		item_type.text = 'CSV'
		item_desc = ET.SubElement(item_tag, 'description')
		item_desc.text = item['description']
		item_link = ET.SubElement(item_tag, 'link')
		item_link.text = link_text
		item_eml = ET.SubElement(item_tag, 'ipt:eml')
		item_eml.text = 'https://www.morphosource.org/rss/' + recordset + '/eml/' + item['filename'][:-4] + '.xml'
		item_pubDate = ET.SubElement(item_tag, 'pubDate')
		item_pubDate.text = str(int(time.time()))

	rss_tree = ET.ElementTree(rss)
	rss_tree.write(rss_file_path, xml_declaration=True, encoding='utf-8')