'''
MST961-EWS: Data Science Tools & Techniques
News Scraper Exercise
Cory Campbell, Josh Swain
14 April 2021

This script scrapes the CNN US Edition sitemap for a given year and month, and exports all articles to JSON as follows:
[
    {
        'headline': <article headline>,
        'modified': <date article was last modified>,
        'text':     <article full text>
    }, ...
]
'''

import concurrent.futures as cf
import json
import logging
import math
import re
import sys
import time
from datetime import datetime
from pprint import pprint as pp

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tqdm import tqdm  # the One True Import of import
from urllib3.util.retry import Retry

logfile = 'main-' + datetime.now().strftime('%Y-%m-%d-%H:%M:%S') + '.log'

logging.basicConfig(
    filename=logfile, 
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

failed_urls = []

excluded_pages = [
    re.compile(r'cnn\-underscored'),
    re.compile(r'fast\-facts'),
    re.compile(r'five\-things'),
    re.compile(r'\-trnd'),
    re.compile(r'what\-matters'),
    re.compile(r'week\-in\-review')
]

'''
Soupify.
'''
def get_soup(page):
  soup = BeautifulSoup(page, 'html.parser')
  return soup

'''
Requests session for get_html
'''
def requests_session(
    retries = 3,
    backoff_factor = 0.3,
    status_forcelist=(500, 502, 504),
    session=None,
    ):

    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


'''
Get raw HTML.
'''

def get_html(url):
    try:
        page = requests_session().get(url, timeout=10)
        text = page.text
        return text
    except Exception:
        logging.exception(f'Failure on {url}')
        logging.error(f'Failure on {url}')
        failed_urls.append(url)
        return

def handle_failures(failures):
    #This is where we change what we want to do with URL requests that fail.
    if len(failures) > 0:
        logging.info('Failed requests added to failed_urls.json')
        with open('failed_urls.json', 'w') as f:
            json.dump(failures, f)
    return

'''
Parse links from soup.
'''
def get_soup_links(soup):
    re_article = re.compile(r'/\d{4}/\d{2}/\d{2}/')
    links = []
    for link in soup.find_all('a', href=re_article):
        out_link = link.get('href')
        links.append(out_link)
    return links

'''
Scrape CNN US sitemap for given year, month. Returns list of all article urls.
'''
def crawl_links_month(year:int, month:int):
    failed_urls = []
    url_str = r'https://us.cnn.com/article/sitemap-{}-{}.html'.format(year,month)
    page = get_html(url_str)
    soup = get_soup(page)
    all_links = get_soup_links(soup)
    handle_failures(failed_urls)
    clean_links = []
    for url in all_links:
        valid = True
        for exclude in excluded_pages:
            if re.search(exclude, url) != None:
                valid = False
        if valid:
            clean_links.append(url)
    return clean_links

'''
Scrape CNN US sitemap for given full year. Returns list of all article urls. This might void your warranty.
'''
def crawl_links_year(year:int):
    all_links = []
    failed_urls = []
    for i in tqdm(range(1,13)):
        try:
            url_str = r'https://us.cnn.com/article/sitemap-{}-{}.html'.format(year,i)
            page = get_html(url_str)
            soup = get_soup(page)
            links = get_soup_links(soup)
            all_links += links
        except Exception as e:
            print(e)
            continue
    handle_failures(failed_urls)
    clean_links = []
    for url in all_links:
        valid = True
        for exclude in excluded_pages:
            if re.search(exclude, url) != None:
                valid = False
        if valid:
            clean_links.append(url)
    return clean_links

'''
Attempt to retrive a metadata tag about an article page.
'''
def parse_meta(soup, attr, **kwargs):
    try:
        result = soup.find('meta', attrs=kwargs)[attr]
    except:
        result = None
    return result

'''
Parse article from soup. Return dict of parsed article deliciousness.
'''
def parse_article(soup):
    re_paragraph = re.compile(r'body__paragraph')
    re_para_alt = re.compile(r'paragraph inline\-placeholder')
    paras = []
    #CNN, annoyingly, puts the first paragraph of the article in a p tag,
    #and each subsequent paragraph in a div tag.
    for para in soup.find_all('p', class_=re_paragraph):
        if not para == '':
            paras.append(para.text)
    for para in soup.find_all('div', class_=re_paragraph):
        if not para == '':
            paras.append(para.text)
    for para in soup.find_all('p', class_=re_para_alt):
        if not para == '':
            paras.append(para.text)
    text = ''.join(paras)
    modified = parse_meta(soup, 'content', **{'itemprop':'dateModified'})
    headline = parse_meta(soup, 'content', **{'itemprop':'alternativeHeadline'})
    if headline == None:
        headline = parse_meta(soup, 'content', **{'itemprop': 'headline'})
    article = {
      'headline': headline,
      'modified': modified,
      'text': text,
    }
    
    return article

'''
Worker for ThreadPoolExecutor
'''
def thread_worker(url_str):
    results = {}

    html_tic = time.perf_counter()
    page = get_html(url_str)
    html_toc = time.perf_counter()

    soup_tic = time.perf_counter()
    soup = get_soup(page)
    soup_toc = time.perf_counter()

    parse_tic = time.perf_counter()
    article = parse_article(soup)
    parse_toc = time.perf_counter()

    results['headline'] = article['headline']
    results['modified'] = article['modified']

    if sys.argv[1] != 'keywords':
        if article['text'] != '':
            results['article_text'] = article['text']
        else:
            logging.warning(f'Url {url_str} produced empty article.')

    if sys.argv[1] != 'text':
        results['keywords'] = article['keywords']
    
    results['html_time'] = (html_toc - html_tic)
    results['soup_time'] = (soup_toc - soup_tic)
    results['parse_time'] = (parse_toc - parse_tic)

    return results

'''
Parse many articles, given a list of urls.
'''
def parse_many(url_list):
    parsed_list = []
    html_times = []
    soup_times = []
    parse_times = []
    failed_urls = []

    with tqdm(total=len(url_list)) as pbar:
        with cf.ProcessPoolExecutor(max_workers=32) as executor:
            futures = {executor.submit(thread_worker, arg): arg for arg in url_list}
            for future in cf.as_completed(futures):
                results = future.result()
                if 'article_text' in results.keys():
                    parsed_list.append(results['article_text'])
                html_times.append(results['html_time'])
                soup_times.append(results['soup_time'])
                parse_times.append(results['parse_time'])
                pbar.update(1)
    
    handle_failures(failed_urls)

    return parsed_list, html_times, soup_times, parse_times

'''
Export parsed articles to JSON.
'''
def output_to_json(parsed_articles:list, filename:str):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(parsed_articles, f, ensure_ascii=False, indent=4)

'''
MAIN
'''

def main():
    try:
        if len(sys.argv) == 1:
            sys.argv.append('text')
        print('Welcome to the CNN Scraper 9000!\n')
        if len(sys.argv) == 2:
            year = int(input('Enter year of interest: '))
        else:
            year = int(sys.argv[2])
        if len(sys.argv) < 4:
            mode = str(input('Scrape a full year(1), or just one month(2)? '))
        elif sys.argv[3] == 'all':
            mode = '1'
        else:
            mode = '2'
        if mode == '1':  #scrape a year
            print('Scraping ...')
            month = 'all'
            links = crawl_links_year(year)
            print(f'\nFound {len(links)} articles. Parsing ...')
        elif mode == '2':  #scrape a month
            if len(sys.argv) < 4:
                month = int(input('Enter month of interest as digit: ').replace('0',''))
            else:
                month = int(sys.argv[3])
            print('Scraping ...')
            links = crawl_links_month(year, month)
            print(f'\nFound {len(links)} articles. Parsing ...')
        else:
            print('Invalid input.')
            raise Exception

        parsed_articles, html_times, soup_times, parse_times = parse_many(links)
        print(f'\nParsed {len(parsed_articles)}. Exporting ...')

        performance_timing = [
            '\nPerformance timing:',
            f'\nget_html()\t\tMin: {min(html_times):.6f}\tMax: {max(html_times):.6f}\tAvg: {sum(html_times)/len(html_times):.6f}',
            f'get_soup()\t\tMin: {min(soup_times):.6f}\tMax: {max(soup_times):.6f}\tAvg: {sum(soup_times)/len(soup_times):.6f}',
            f'parse_article()\t\tMin: {min(parse_times):.6f}\tMax: {max(parse_times):.6f}\tAvg: {sum(parse_times)/len(parse_times):.6f}',
        ]
        for line in performance_timing:
            print(line)
            logging.info(line)

        filename = f'CNN_{year}_{month}.json'
        output_to_json(parsed_articles, filename)
        print(f'\nExported to {filename}. Have a blessed day!')
    except Exception:
        logging.exception('It all went sideways!')
        logging.info(sys.argv)
        print('Ruh roh.')
    
if __name__ == '__main__':
    main()
