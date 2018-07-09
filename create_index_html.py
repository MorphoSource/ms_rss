def gen_html(pub_dict, html_file_path):
	if 'data' in pub_dict['json'] and 'name' in pub_dict['json']['data']:
		pub_name = pub_dict['json']['data']['name']
	else:
		pub_name = ''
	if 'uuid' in pub_dict['json']:
		pub_uuid = pub_dict['json']['uuid']
	else:
		raise ValueError('Error getting UUID for pub_dict ' + str(pub_dict))

	f = open(html_file_path, 'wb')
	f.write('<!DOCTYPE html>')
	f.write('<html>')
	f.write('<head><title>MorphoSource RSS Feed Information</title></head>')
	f.write('<body><h1>MorphoSource Media and Usage Report RSS Feed Information</h1><p><b>Publisher:</b></p><p>' + pub_name + ' (' + pub_uuid + ')</p><p><b>Recordsets:</b></p>')
	for r in pub_dict['recordsets']:
		f.write('<p><a href="https://www.idigbio.org/portal/recordsets/' + r + '">' + r + '</a></p>')
	f.write('</br><p><b><a href="https://www.morphosource.org/rss/' + pub_uuid + '/ms.rss">RSS Feed Link</a></b></p></body>')
	f.close()
