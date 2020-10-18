from bs4 import BeautifulSoup
import requests
import re
import json
import time


# This function accepts an URL as parameter and returns a BSOB
def get_soup_object(url):
    # set a user-agent to be sent with request
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/61.0.3163.100 Safari/537.36"
    }

    print("Retrieving from: " + url)

    response = requests.get(url, headers)
    if (response.ok):
        # parse the raw HTML into a `soup' object
        soupObj = BeautifulSoup(response.text, "html.parser")

        return soupObj
    else:
        print("There was a problem reading from the URL: " + url)
        return None


# Extracts the links from a BSOB provided as a parameter
def extract_links(soupObj):
    result = []
    pattern = re.compile("\/*[0-9]{6}\.html$")
    if (soupObj is None):
        print("The BSOB object is null")
        return None

    links = soupObj.findAll('a')

    for link in links:
        href = link.attrs['href']
        match = re.search(pattern, href)
        if (match):
            result.append(href.replace('.html', ''))

    # print(result)
    return result


# Parses a BSOB object and extracts headers and data. The result is a list of dicts.
def get_event_content(soupObj):
    result = []

    try:
        headers = soupObj.find('table').tr.text
        headers = headers.strip().split('\n')
        table = soupObj.find('table').tbody

        for row in table.findAll("tr"):
            cols = row.findAll("td")
            if len(cols) == len(headers):

                tmp_dict = {}
                for header, i in zip(headers, range(len(headers))):
                    tmp_dict[header] = cols[i].text

            result.append(tmp_dict)

    except AttributeError as ae:
        return None

    return result


# Saves the content of a list of dictionary data into a formatted JSON file.
def save_to_file(records_list):
    with open('data.json', 'w') as outfile:
        json.dump(records_list, outfile, indent=4)


# Provided with a URL as argument, extracts an enhanced summary which is located on another page [level 3]
def get_extended_summary(url):
    try:
        summary = ''
        bsob = get_soup_object(url)
        summary = bsob.findAll("tr")[-1].td.get_text()
    except AttributeError as ae:
        return None

    return summary


# Receives a list of links, it parses the relative pages [level 2]. Furthermore, it uses the filename as an ID for each record. The result is written into a JSON file.
def retrieve_data(links):
    data = []
    tmp = []
    ids = []

    try:
        for link in links:
            inner_soup = get_soup_object(stem + link + '.html')
            ids = extract_links(inner_soup)
            # print(ids)
            tmp = get_event_content(inner_soup)
            # print(tmp)
            if (len(ids) != len(tmp)):
                print("Unexpected HTML format on page " + link)
                raise

            for i, j in zip(ids, range(len(tmp))):
                tmp[j]['Summary'] = get_extended_summary(stem + i + '.html')
                data.append({i: tmp[j]})

            # print(data)

        save_to_file(data)
    except TypeError:
        print("Cannot read from page " + link)
    finally:
        save_to_file(data)


# Main Block
stem = 'http://www.nuforc.org/webreports/'

outer_soup = get_soup_object(stem + "ndxevent.html")
pages = extract_links(outer_soup)
retrieve_data(pages)
