import xmltodict
import json

with open('PostHistory.xml') as fd:
	doc = xmltodict.parse(fd.read())

questionIdsAndCounts = []

for row in doc['posthistory']['row']:
	# Found duplicate question
	if int(row['@PostHistoryTypeId']) == 10:
		# Check if it has link(s) to original question(s)
		text = row['@Text']
		if 'OriginalQuestionIds' in text:
			textJSON = json.loads(text)
			ids = textJSON['OriginalQuestionIds']
			for id in ids:
				questionIdsAndCounts['ID' + id] = 1

print questionIdsAndCounts