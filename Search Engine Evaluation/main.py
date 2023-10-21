from bs4 import BeautifulSoup
import time
import requests
from random import randint
import json
import threading
import re
import pandas as pd
import numpy as np


USER_AGENT = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

QUERY_TEXT = 'query.txt'
SEARCH_RESULT_FILENAME = "hw1.json"
GOOGLE_DATA_SHEET = 'google.json'
STATISTICS_FILENAME = 'hw1.csv'

class SearchEngine:
    @staticmethod
    def search(query, sleep=True):
        if sleep:  # Prevents loading too many pages too soon
                time.sleep(randint(10, 100))
        temp_url = '+'.join(query.split())  # for adding + between words for the query
        query_result = []
        url = 'http://www.bing.com/search?q=' + temp_url + '&count=30'

        # While the query result is empty repeat
        while(len(query_result) == 0):
            # Request data
            try:
                response = requests.get(url, headers=USER_AGENT)
                response.raise_for_status()
                html_data = response.text
                soup = BeautifulSoup(html_data, "html.parser")
                query_result = SearchEngine.scrape_search_result(soup)
            except requests.exceptions.RequestException as e:
                print(f"Error in requests. Retrying query. {query}") # incase the request has issues
            if len(query_result) == 0:
                time.sleep(randint(5,20)) # incase no result is received then sleep for a bit and restart search
        return query_result

    @staticmethod
    def scrape_search_result(soup):
        raw_results = soup.find_all('li', attrs={'class': 'b_algo'})
        search_links = []
        unique_links = set() # use set to avoid any duplications
        for idx,result in enumerate(raw_results):
            link = result.find('a').get('href')
            if link and link not in unique_links:
                search_links.append(link)
                unique_links.add(link)
            if len(search_links) >= 10:
                break
        return search_links

class Crawler:
    def __init__(self, queryFileName, resultFileName):
        self.queryfile = queryFileName
        self.resultfile = resultFileName
        self.result = {}

    # Read query list text file
    def readQueryList(self):
        queries = []
        with open(self.queryfile, 'r') as f:
            for q in f:
                curQuery = q.rstrip('? \n')
                queries.append(curQuery)
                # Add queries in order they are read for keeping order
                self.result[curQuery] = []
        return queries

    # Write dict to file
    def writeSearchResult(self):
        with open(self.resultfile, 'w') as f:
            json.dump(self.result, f)

    # Do multiply threading
    def threader(self, queries):
        num_threads = 10 # alter number of threads if needed
        threads = []

        def worker():
            while True:
                try:
                    curQ = queries.pop()
                except IndexError:
                    break
                self.result[curQ] = SearchEngine.search(curQ)
                

        for _ in range(num_threads):
            thread = threading.Thread(target=worker)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

# For csv file
class Statistics():
    def __init__(self):
        self.google = GOOGLE_DATA_SHEET
        self.myResult = SEARCH_RESULT_FILENAME
        self.googleData = {}
        self.myData = {}
        self.queryStats = {'Queries': [], 'Number of Overlapping Results': [], 'Percent Overlap': [], 'Spearman Coefficient': []} 
    
    def run(self):
        self.readData()
        for idx, q in enumerate(list(self.googleData.keys())):
            try:
                self.getQueryStats(self.googleData[q], self.myData[q], idx+1)
            except Exception as err:
                print(f"Error in a query: {q}")
        self.getAverages()
        self.write()

    # Read written search data for our search and google
    def readData(self):
        with open(self.myResult, 'r') as f:
            self.myData = json.load(f)
        with open(self.google, 'r') as f:
            self.googleData = json.load(f)

    # Write the data
    def write(self):
        data = pd.DataFrame(self.queryStats)
        data.set_index("Queries", inplace=True)
        data.to_csv(STATISTICS_FILENAME)

    # Trimg url for better comparison
    def trim_url(self, url: str)->str:
        url = re.sub(r'^https?://', '', url, flags=re.IGNORECASE)
        url = re.sub(r'^www\.', '', url, flags=re.IGNORECASE)
        url = url.rstrip('/')    
        return url

    # Get average of the data we wrote
    def getAverages(self):
        self.queryStats['Queries'].append('Averages')
        self.queryStats['Number of Overlapping Results'].append(np.mean(self.queryStats['Number of Overlapping Results']))
        self.queryStats['Percent Overlap'].append(np.mean(self.queryStats['Percent Overlap']))
        self.queryStats['Spearman Coefficient'].append(np.mean(self.queryStats['Spearman Coefficient']))

    # calculate spearman coefficient
    def getRho(self, n:int, sum_d_sqd: int) -> float:
        return 1-((6*sum_d_sqd)/((n**3) - n))
    
    # get query stats
    def getQueryStats(self, g_data: list, m_data: list, counter: int):
        trim_gData, trim_myData = list(map(self.trim_url, g_data)), list(map(self.trim_url, m_data))
        common_urls = list(set(trim_gData) & set(trim_myData)) # Find common URLS
        n = len(common_urls) # Get Number of overlaping urls n
        p = 100 * (n / len(trim_gData)) # Get Percent of overlap 
        # Rank them
        gRank, mRank = {}, {}
        for idx, g in enumerate(trim_gData):
            gRank[g] = idx+1

        for idx, m in enumerate(trim_myData):
            mRank[m] = idx+1

        # Get Rho
        rho = 0
        if n > 1:
            d_sqd = []
            # For all common urls
            for c in common_urls:
                # calculate d and store
                d_sqd.append((gRank[c] - mRank[c])**2)
            rho = self.getRho(n, sum(d_sqd))
        if n == 1 and gRank[common_urls[0]] == mRank[common_urls[0]]:
            rho = 1
        self.queryStats['Queries'].append(f'Query {counter}')
        self.queryStats['Number of Overlapping Results'].append(n)
        self.queryStats['Percent Overlap'].append(p)
        self.queryStats['Spearman Coefficient'].append(rho)

if __name__ == "__main__":
    '''Crawl and get results'''
    crawl = Crawler(QUERY_TEXT, SEARCH_RESULT_FILENAME)
    queries = crawl.readQueryList()
    crawl.threader(queries)
    crawl.writeSearchResult() 
    stats = Statistics()
    stats.run()