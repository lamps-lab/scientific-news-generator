import csv

def read_csv(file_name):
    data=[]
    with open(file_name, 'r') as file:
        reader = csv.reader(file, delimiter=',')
        header = next(reader)
        data = [row for row in reader]
    return data

data = read_csv('newurldata.csv')

categories = {}
total = 0
for row in data:
	category = row[3]
	if category in categories:
		total +=1
		categories[category] += 1
	else:
		total += 1
		categories[category] = 1


for category, count in categories.items():
    print(f"{category}: {count}")
print(f"\ntotal: {total}")