import os, sys, csv, re
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import ast
from playwright.sync_api import sync_playwright

from urllib.parse import urlsplit, urlunsplit

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


import cloudscraper

def get_dynamic_soup(url: str) -> BeautifulSoup:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        soup = BeautifulSoup(page.content(), "html.parser")
        browser.close()
        return soup

def is_published_paper(url, publishers):
    try:
        url_parts = urlsplit(url)
    except ValueError:
        print(f"Invalid URL: {url}")
        return False

    domain = url_parts.path
    for pub in publishers:
        if pub in domain:
            return True
    return False

def read_csv(file_name):
    data=[]
    with open(file_name, 'r') as file:
        reader = csv.reader(file, delimiter=',')
        header = next(reader)
        data = [row for row in reader]
    return data

def append_entry_to_csv(path: str, entry: dict):
    file_exists = os.path.isfile(path)

    # Define the field names (order matters in a CSV!)
    fieldnames = ['articleid', 'urlid', 'url', 'category']

    # Open the file in append mode ('a')
    with open(path, 'a', newline='') as f:
        # Create a CSV DictWriter object
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        # Write the header if the file didn't exist
        if not file_exists:
            writer.writeheader()

        # Write the entry
        writer.writerow(entry)



# id,title,author,category,date,url,plaintext,html,all_links

firstdata = read_csv('../../data/14-18.csv')
seconddata = read_csv('../../data/19-22.csv')
data = firstdata + seconddata
#count_categories(data)
print("reading files done")

published_domains = ["publishers_list","springer","umi","ebscohost","sciencedirect","emeraldinsight","sagepub","scopusnlai","intechopen","dart-europe","digitool","highwiredoaj",
"pnas","eprints.nottingham","digital.library.upenn.edu/books/","etd.ohiolink","escholarship","lib","ieeexplore","acm","wiley","sciencedirect",
"acs","aiaa","aip","ajpe","aps","ascelibrary","asm","asme","bioone","birpublications","bmj","emeraldinsight","geoscienceworld","icevirtuallibrary","informs","ingentaconnect",
"iop","jamanetwork","joponline","jstor","mitpressjournals","nrcresearchpress","oxfordjournals","royalsociety","rsc","rubberchemtechnol","sagepub","scientific","spiedigitallibrary","tandfonline","theiet",
"doi", "arxiv","eurekalert"]

# create root dir if it doesn't exist
root = os.getcwd() + "/belo"
if not os.path.exists(root):
	os.makedirs(root)

# get inside root
os.chdir('belo')

# Path to Chrome binary within the .app package
path_to_chrome_binary = r"/Volumes/Macintosh HD/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.binary_location = path_to_chrome_binary

path_to_driver = '/Users/zeus/Desktop/research/chromedriver_mac64/chromedriver'


import time

def getDriver(url):
    scraper = cloudscraper.create_scraper()
    retries = 3
    for i in range(retries):
        try:
            response = scraper.get(url)
            if response.status_code == 200:
                return response.text
        except requests.exceptions.SSLError:
            try:
                response = requests.get(url, verify=False)
                if response.status_code == 200:
                    return response.text
            except Exception as e:
                print(f"Error accessing {url} with requests: {e}")
        except requests.exceptions.ConnectionError as e:
            if i < retries - 1:  # if it's not the last try
                wait = 2**(i + 1)  # exponential backoff
                print(f"Connection error: {e}. Retrying in {wait} seconds...")
                time.sleep(wait)
                continue
            else:  # this was the last try
                print(f"Failed to retrieve {url} after {retries} tries.")
        except Exception as e:
            print(f"Error accessing {url} with cloudscraper: {e}")
        break  # if we haven't hit an exception, break the loop
    return None


count = 0
category_count = {}
totalSize = 0
totalLinks = 0
htmlCount = 0
for row in data:
	if count<=23494:
		count +=1
		continue

	#print(row[0])
	#break
	articleid = row[0]
	category = row[3]

	links = ast.literal_eval(row[8])

	currentPath = os.path.abspath(os.getcwd()) + '/'
	upperPath = currentPath + str(row[0])

	linkcount = 0
	url_list = []
	for link in links:
		if "google" in link or "wikipedia" in link or not is_published_paper(link, published_domains) or "http" not in link or link[0] == '/' or 'Sciencalert' in link or 'sciencealert' in link or link in url_list: # IN_URL and duplicate handling within articles
			continue

		url_list.append(link)

		html = getDriver(link)
		if html:
			print(link)
			# create directory if it doesn't exist with the id name
			if not os.path.exists(upperPath):
				os.mkdir(upperPath)

			# get inside the directory
			os.chdir(upperPath)
			# now save the file 
			file_name = f'{linkcount}.html'
			if not os.path.exists(upperPath+file_name):
				with open(file_name, 'w') as f:
					f.write(html)

				entry = {
					'articleid':articleid,
					'urlid': linkcount,
					'url':link,
					'category':category
				}
				linkcount+=1
				# append the urldata file
				append_entry_to_csv('../../urldata.csv', entry)
				#print("Belo")
				os.chdir(currentPath)
		else:
			# if driver isn't working
			# let us try the dynamic soup option 
			print("Baszódj meg akkor")


	# go back to previous directory for next article's links
	os.chdir(currentPath)
#print(f"\nTotal number of Links in the articles: {totalLinks}\nNumber of HTMLs to save: {htmlCount} with size: {totalSize/1024:.2f} MB\n")
#print(f"Number of entries: {entries}")
for category, count in category_count.items():
    print(f"{category}: {count}")

