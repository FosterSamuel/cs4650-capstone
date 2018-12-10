import re

# Change based on output from XSEDE Bridges
inputFile = "'part-r-00000 (2).txt'"
with open(inputFile) as fd:
	doc = fd.readlines()

# List of [linkId, count]
idCountList = []

# Change based on community
linkPrefix = "https://softwareengineering.stackexchange.com/questions/"

# Each line in the output
for line in doc:
	# Find the ID and count as separate multi-digit groups
	m = re.search('(\d*)\D*(\d*)', line) 
	if m:
		# First group is ID (add link prefix)
		id = linkPrefix + m.group(1)

		# Second group is count
		count = m.group(2)

		# Add to list
		idCountList.append([id, int(count)])

# Sort list in-place ascending
idCountList.sort(key=lambda tup: tup[1]) 

# Grab top 10 from end of list
topTenQuestions = idCountList[-10:]

# Put in descending order
topTenQuestions.reverse()

# Output the list of questions
index = 1

# Change this based on community being parse (SE, in this case)
outputFile = open("SE_topTenQuestions.txt","w+") 

for question in topTenQuestions:
	outputFile.write(str(index) + ") " + str(question[1]) + " times: " + question[0] + "\n")
	index = index + 1

# Close file
outputFile.close()