import pandas as pd

df_1 = pd.read_csv('Results2/fetch_latimes.csv')
df_2 = pd.read_csv('Results2/visit_latimes.csv')
df_3 = pd.read_csv('Results2/urls_latimes.csv')

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
            
with open('Results2/CrawlReport_latimes.txt', 'w') as f:
    f.write(output_text)