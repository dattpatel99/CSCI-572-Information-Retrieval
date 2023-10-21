from scrapy.spiders import CrawlSpider, Rule
from scrapy.exceptions import CloseSpider
from scrapy.linkextractors import LinkExtractor
from collections import deque as Queue
from collections import defaultdict
import re
import pandas as pd

MAX_LIMIT = 20001

# URL Patterns for matching
URL_PATTERN = re.compile(r'^https?://')
A_TAG_PATTERN = re.compile(r'^(https?://(www\.)?latimes\.com|http://(www\.)?latimes\.com)')
HOME_PAGE_PATTERN = re.compile(r'^/')

FILE_1 = 'fetch_latimes.csv'
FILE_2 = 'visit_latimes.csv'
FILE_3 = 'urls_latimes.csv'

def find_content_type(content_info):
    if 'text/html' in content_info:
        return 'text/html'
    elif 'image/gif' in content_info:
        return 'image/gif'
    elif 'image/jpeg' in content_info:
        return 'image/jpeg'
    elif 'image/png' in content_info:
        return 'image/png'
    elif 'application/pdf' in content_info:
        return 'application/pdf'
    else:
        return 'Unknown'


class LaTimes(CrawlSpider):
    name = 'latimes'
    allowed_domains = ['www.latimes.com']
    start_urls = ['https://www.latimes.com/']
    custom_settings = {"DEPTH_LIMIT": 16, 'REDIRECT_ENABLED': False}
    handle_httpstatus_list = [
        200, 201, 202, 203, 204, 205, 206,
        300, 301, 302, 303, 304, 305, 307, 308,
        400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 421, 422, 423, 424, 425, 426, 428, 429, 431, 451,
        500, 501, 502, 503, 504, 505, 506, 507, 508, 510, 511]
    rules = (Rule(LinkExtractor(),
    callback="parse",
    follow=True),)
    
    # Define file dicts
    file_1 = {'Url': [], 'Status': []}
    file_2 = {'Url':[],'Size (Bytes)':[],'# of Outlinks':[], 'Content Type':[]}
    file_3 = {'Url': [], 'OK/N_OK': []}

    visited = defaultdict(int)

    url_queue = Queue([])
    total_pages_fetched = 0

    def parse(self, response):
        if self.visited[response.request.url] != 1:
            self.visited[response.request.url] = 1
            self.total_pages_fetched += 1
            if self.total_pages_fetched >= MAX_LIMIT:
                raise CloseSpider("reched maximum number of fetched URL")

            self.file_1['Url'].append(response.request.url)
            self.file_1['Status'].append(response.status)

            if 'image' not in str(response.headers['Content-Type']):
                # Check all a tags
                for links in response.css('a'):
                    url_pages = links.attrib['href']
                    if(A_TAG_PATTERN.match(url_pages)):
                        self.url_queue.append(url_pages)
                        self.file_3['Url'].append(url_pages)
                        self.file_3['OK/N_OK'].append('OK')
                    elif (URL_PATTERN.match(url_pages)):
                        self.file_3['Url'].append(url_pages)
                        self.file_3['OK/N_OK'].append('N_OK')

                # Check all outside link tags
                for links in response.css('link'):
                    if 'href' in links.get():
                        url_pages = links.attrib['href']
                        if(A_TAG_PATTERN.match(url_pages)):
                            self.url_queue.append(url_pages)
                            self.file_3['Url'].append(url_pages)
                            self.file_3['OK/N_OK'].append('OK')
                        elif(HOME_PAGE_PATTERN.match(url_pages)):
                            url_pages = "https://www.latimes.com" + url_pages
                            self.url_queue.append(url_pages)
                            self.file_3['Url'].append(url_pages)
                            self.file_3['OK/N_OK'].append('OK')
                        elif (URL_PATTERN.match(url_pages)):
                            self.file_3['Url'].append(url_pages)
                            self.file_3['OK/N_OK'].append('N_OK')

            if response.status == 200:
                content_info = find_content_type(str(response.headers['Content-Type']))
                outlinks = 0
                if 'image' not in content_info:
                    # Total Outlinks
                    outlinks = len(response.css('a')) + len(response.css('link'))
                if content_info != 'Unknown':
                    self.file_2['Url'].append(response.request.url)
                    self.file_2['Size (Bytes)'].append(len(response.body))
                    self.file_2['# of Outlinks'].append(outlinks)
                    self.file_2['Content Type'].append(content_info)
        while self.url_queue:
            next_link = self.url_queue.popleft()
            yield response.follow(next_link, callback= self.parse)
        

    def closed(self, reason):
        if(self.total_pages_fetched >= MAX_LIMIT):
            df_1 = pd.DataFrame(self.file_1) # File, URL, Status
            df_2 = pd.DataFrame(self.file_2) # Visit, URL, size, outlinks, content
            df_3 = pd.DataFrame(self.file_3) # Urls, Url, ok
            
            df_1.to_csv(FILE_1, index=False)
            df_2.to_csv(FILE_2, index=False)
            df_3.to_csv(FILE_3, index=False)
            
            unique_extracted = df_3.drop_duplicates('Url')
            ok_values = len(unique_extracted[unique_extracted['OK/N_OK'] == "OK"])
            nok_values = len(unique_extracted[unique_extracted['OK/N_OK'] == "N_OK"])

            status_meanings = {200: "OK",201: "Created",202: "Accepted",203: "Non-Authoritative Information",204: "No Content",205: "Reset Content",206: "Partial Content",300: "Multiple Choices",301: "Moved Permanently",302: "Found",303: "See Other",304: "Not Modified",305: "Use Proxy",307: "Temporary Redirect",308: "Permanent Redirect",400: "Bad Request",401: "Unauthorized",402: "Payment Required",403: "Forbidden",404: "Not Found",405: "Method Not Allowed",406: "Not Acceptable",407: "Proxy Authentication Required",408: "Request Timeout",409: "Conflict",410: "Gone",411: "Length Required",412: "Precondition Failed",413: "Payload Too Large",414: "URI Too Long",415: "Unsupported Media Type",416: "Range Not Satisfiable",417: "Expectation Failed",418: "I'm a teapot",421: "Misdirected Request",422: "Unprocessable Entity",423: "Locked",424: "Failed Dependency",425: "Too Early",426: "Upgrade Required",428: "Precondition Required",429: "Too Many Requests",431: "Request Header Fields Too Large",451: "Unavailable For Legal Reasons",500: "Internal Server Error",501: "Not Implemented",502: "Bad Gateway",503: "Service Unavailable",504: "Gateway Timeout",505: "HTTP Version Not Supported",506: "Variant Also Negotiates",507: "Insufficient Storage",508: "Loop Detected",510: "Not Extended",511: "Network Authentication Required"}
            status_codes = ""
            required_codes = {200,301,401,403,404}
            required_codes.update(list(df_1['Status'].unique()))
            for i, code in enumerate(required_codes):
                status_codes += f'{code} {status_meanings[int(code)]}: {df_1[df_1["Status"] == int(code)].shape[0]}\n'
            output_text = f'''Name: Datt Patel
USC ID: 2149812400
News site crawled: latimes.com
Number of threads: 1

Fetch Statistics
================
# fetches attempted: {df_1.shape[0]}
# fetches succeeded: {df_1[df_1['Status'] == 200].shape[0]}
# fetches failed or aborted: {df_1[df_1['Status'] != 200].shape[0]}

Outgoing URLs:
==============
Total URLs extracted: {df_2.loc[:,'# of Outlinks'].sum()}
# unique URLs extracted: {unique_extracted.shape[0]}
# unique URLs within News Site: {ok_values}
# unique URLs outside News Site: {nok_values}

Status Codes:
=============
{status_codes}
File Sizes:
===========
< 1KB: {df_2[df_2['Size (Bytes)'] < 1024].shape[0]}
1KB ~ <10KB: {df_2[(df_2['Size (Bytes)'] >= 1024) & (df_2['Size (Bytes)'] < 10240)].shape[0]}
10KB ~ <100KB: {df_2[(df_2['Size (Bytes)'] >= 10240) & (df_2['Size (Bytes)'] < 102400)].shape[0]}
100KB ~ <1MB: {df_2[(df_2['Size (Bytes)'] >= 102400) & (df_2['Size (Bytes)'] < 1048576)].shape[0]}
>= 1MB: {df_2[df_2['Size (Bytes)'] >= 1048576].shape[0]}

Content Types:
==============
text/html: {df_2[df_2.loc[:,'Content Type']=='text/html'].shape[0]}
image/gif: {df_2[df_2.loc[:,'Content Type']=='image/gif'].shape[0]}
image/jpeg: {df_2[df_2.loc[:,'Content Type']=='image/jpeg'].shape[0]}
image/png: {df_2[df_2.loc[:,'Content Type']=='image/png'].shape[0]}
application/pdf: {df_2[df_2.loc[:,'Content Type']=='application/pdf'].shape[0]}'''
            
            with open('CrawlReport_latimes.txt', 'w') as f:
                f.write(output_text)