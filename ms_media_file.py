import pandas
import phpserialize
import zlib
import warnings

class MsMediaFile:
	'''This class defines a MorphoSource media file object'''
	db_dict = {}
	ac_mf_dict = {}
	ac_mfp_dict = {}
	mf_info_dict = {}

	def __init__(self, db_dict):
		self.db_dict = db_dict
		try:
			self.mf_info_dict = self.blob_to_array(self.db_dict["mf.media"])
		except:
			print("Error creating mf_info_dict for media number " + str(self.db_dict['media_file_id']))
		self.create_ac_mf_dict()
		self.create_ac_mfp_dict()

	def blob_to_array(self, blob):
		try:
			return phpserialize.unserialize(zlib.decompress(blob))
		except:
			return phpserialize.unserialize(blob.decode('base64'))


	def is_published(self):
		if self.db_dict['mf.published'] is None:
			if self.db_dict['published'] > 0:
				return True
		elif self.db_dict['mf.published'] > 0: # MF Pub value is filled in
			return True
		else:
			return False

	def get_mf_element(self):
		if self.db_dict['mf.element'] is not None:
			return self.db_dict['mf.element']
		else:
			return self.db_dict['m.element']

	def get_mf_side(self):
		if self.db_dict['mf.element'] is not None:
			return self.db_dict['mf.element']
		else:
			return self.db_dict['m.element']

	def creator_string(self):
		return self.db_dict['fname'] + " " + self.db_dict['lname'] + " <" + self.db_dict['email'] + ">" 

	def copyright_permission(self):
		return {
			0: 'Copyright permission not set',
			1: 'Person loading media owns copyright and grants permission for use of media on MorphoSource',
			2: 'Permission to use media on MorphoSource granted by copyright holder',
			3: 'Permission pending',
			4: 'Copyright expired or work otherwise in public domain',
			5: 'Copyright permission not yet requested'
		}[int(self.db_dict['copyright_permission'])]

	def copyright_license(self):
		return {
			0: 'Media reuse policy not set',
			1: 'CC0 - relinquish copyright',
			2: 'Attribution CC BY - reuse with attribution',
			3: 'Attribution-NonCommercial CC BY-NC - reuse but noncommercial',
			4: 'Attribution-ShareAlike CC BY-SA - reuse here and applied to future uses',
			5: 'Attribution- CC BY-NC-SA - reuse here and applied to future uses but noncommercial',
			6: 'Attribution-NoDerivs CC BY-ND - reuse but no changes',
			7: 'Attribution-NonCommercial-NoDerivs CC BY-NC-ND - reuse noncommerical no changes',
			8: 'Media released for onetime use, no reuse without permission',
			20: 'Unknown - Will set before project publication'
		}[int(self.db_dict['copyright_license'])]

	def copyright_license_uri(self):
		return {
			0: '',
			1: 'https://creativecommons.org/publicdomain/zero/1.0/',
			2: 'https://creativecommons.org/licenses/by/3.0/',
			3: 'https://creativecommons.org/licenses/by-nc/3.0/',
			4: 'https://creativecommons.org/licenses/by-sa/3.0/',
			5: 'https://creativecommons.org/licenses/by-nc-sa/3.0/',
			6: 'https://creativecommons.org/licenses/by-nd/3.0/',
			7: 'https://creativecommons.org/licenses/by-nc-nd/3.0/',
			8: '',
			20: ''
		}[int(self.db_dict['copyright_license'])]

	def copyright_license_logo_uri(self):
		return {
			0: '',
			1: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nc-nd.eu.png',
			2: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by.png',
			3: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nc.png',
			4: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-sa.png',
			5: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nc-sa.png',
			6: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nd.png',
			7: 'https://mirrors.creativecommons.org/presskit/buttons/88x31/png/by-nc-nd.png',
			8: '',
			20: ''
		}[int(self.db_dict['copyright_license'])]

	def citation_instructions(self):
		if self.db_dict['media_citation_instruction1']:
			return self.db_dict['media_citation_instruction1'] + " provided access to these data " + self.db_dict['media_citation_instruction2'] + " " + self.db_dict['media_citation_instruction3'] + ". The files were downloaded from www.morphosource.org, Duke University."

	def conv_mmpix_to_pixcm(self, value):
		try:
			return 1.0/(float(value)*0.1)
		except:
			return None

	def get_derived_from(self):
		if self.db_dict['derived_from_media_file_id'] is None:
			return None
		else:
			return 'ark:/87602/m4/M' + str(self.db_dict['derived_from_media_file_id'])

	def create_ac_mf_dict(self):
		self.ac_mf_dict = {
			'dcterms:identifier': self.db_dict['ark'], 
			'associatedSpecimenReference': self.db_dict['uuid'],
			'coreid': self.db_dict['occurrence_id'],
			'providerManagedID': self.db_dict['media_file_id'],
			'derivedFrom': self.get_derived_from(),
			'providerLiteral': 'MorphoSource',
			'provider': 'https://www.morphosource.org',
			'dc:type': 'Image',
			'dcterms:type': 'http://purl.org/dc/dcmitype/Image',
			'subtypeLiteral': '', # self.db_dict['modality'] when implemented
			'subtype': '', # need to have function to grab this when implemented
			'accessURI': 'https://www.morphosource.org/index.php/Detail/MediaDetail/Show/media_file_id/' + str(self.db_dict['media_file_id']),
			'dc:format': self.mf_info_dict['original']['MIMETYPE'],
			'subjectPart': self.get_mf_element(),
			'subjectOrientation': self.get_mf_side(),
			'caption': self.db_dict['mf.notes'],
			'LocationCreated': self.db_dict['name'],
			'captureDevice': self.db_dict['sc.name'],
			'dc:creator': self.creator_string(),
			'ms:scanningTechnician': self.db_dict['scanner_technicians'],
			'fundingAttribution': self.db_dict['grant_support'],
			'exif:PixelXDimension': self.mf_info_dict['original']['WIDTH'],
			'exif:PixelYDimension': self.mf_info_dict['original']['HEIGHT'],
			'exif:Xresolution': self.conv_mmpix_to_pixcm(self.db_dict['scanner_x_resolution']),
			'exif:Yresolution': self.conv_mmpix_to_pixcm(self.db_dict['scanner_y_resolution']),
			'exif:ResolutionUnit': 3,
			'dicom:SpacingBetweenSlices': self.db_dict['scanner_z_resolution'],
			'dc:rights': self.copyright_permission(),
			'dcterms:rights': self.copyright_license_uri(),
			'Owner': self.db_dict['copyright_info'],
			'UsageTerms': self.copyright_license(),
			'WebStatement': self.copyright_license_uri(),
			'licenseLogoURL': self.copyright_license_logo_uri(),
			'Credit': self.citation_instructions(),
			'IDofContainingCollection': self.db_dict['media_id']
		}

	def create_ac_mfp_dict(self):
		if not 'large' in self.mf_info_dict or not 'HASH' in self.mf_info_dict['large'] or not 'MAGIC' in self.mf_info_dict['large'] or not 'FILENAME' in self.mf_info_dict['large']:
			warnings.warn('MorphoSource media ' + str(self.db_dict['media_file_id']) + ' does not seem to have a preview image in its media info dict: ' + str(self.mf_info_dict))
			return
		p_url = "https://www.morphosource.org/media/morphosource/images/" + self.mf_info_dict['large']['HASH'] + "/" + str(self.mf_info_dict['large']['MAGIC']) + '_' + self.mf_info_dict['large']['FILENAME']
		self.ac_mfp_dict = {
			'dcterms:identifier': p_url, 
			'associatedSpecimenReference': self.db_dict['uuid'],
			'coreid': self.db_dict['occurrence_id'],
			'providerManagedID': str(self.db_dict['media_file_id']) + 'p',
			'derivedFrom': self.ac_mf_dict['dcterms:identifier'],
			'providerLiteral': 'MorphoSource',
			'provider': 'https://www.morphosource.org',
			'dc:type': 'StillImage',
			'dcterms:type': 'http://purl.org/dc/dcmitype/StillImage',
			'subtypeLiteral': 'Graphic',
			'subtype': 'https://terms.tdwg.org/wiki/AC_Subtype_Examples',
			'accessURI': p_url,
			'dc:format': self.mf_info_dict['large']['MIMETYPE'],
			'subjectPart': self.get_mf_element(),
			'subjectOrientation': self.get_mf_side(),
			'caption': self.db_dict['mf.title'],
			'LocationCreated': self.db_dict['name'],
			'captureDevice': self.db_dict['sc.name'],
			'dc:creator': self.creator_string(),
			'ms:scanningTechnician': self.db_dict['scanner_technicians'],
			'fundingAttribution': self.db_dict['grant_support'],
			'exif:PixelXDimension': self.mf_info_dict['large']['WIDTH'],
			'exif:PixelYDimension': self.mf_info_dict['large']['HEIGHT'],
			'exif:Xresolution': '',
			'exif:Yresolution': '',
			'exif:ResolutionUnit': '',
			'dicom:SpacingBetweenSlices': '',
			'dc:rights': self.copyright_permission(),
			'dcterms:rights': self.copyright_license_uri(),
			'Owner': self.db_dict['copyright_info'],
			'UsageTerms': self.copyright_license(),
			'WebStatement': self.copyright_license_uri(),
			'licenseLogoURL': self.copyright_license_logo_uri(),
			'Credit': self.citation_instructions(),
			'IDofContainingCollection': self.db_dict['media_id']
		}

		


