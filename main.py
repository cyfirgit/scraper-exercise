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

from tqdm import tqdm  #the One True Import of import
import requests
import re
import json
import time
from bs4 import BeautifulSoup

'''
Soupify.
'''
def get_soup(page):
  soup = BeautifulSoup(page, 'html.parser')
  return soup

'''
Get raw HTML.
'''
def get_html(url):
    page = requests.get(url)
    text = page.text
    return text

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
    url_str = r'https://us.cnn.com/article/sitemap-{}-{}.html'.format(year,month)
    page = get_html(url_str)
    soup = get_soup(page)
    all_links = get_soup_links(soup)
    return all_links

'''
Scrape CNN US sitemap for given full year. Returns list of all article urls. This might void your warranty.
'''
def crawl_links_year(year:int):
    all_links = []
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
    return all_links


'''
Parse article from soup. Return dict of parsed article deliciousness.
'''
def parse_article(soup):
    re_paragraph = re.compile(r'body__paragraph')
    paras = []
    #CNN, annoyingly, puts the first paragraph of the article in a p tag,
    #and each subsquent paragraph in a div tag.
    for para in soup.find_all('p', class_=re_paragraph):
        if not para == '':
            paras.append(para.text)
    for para in soup.find_all('div', class_=re_paragraph):
        if not para == '':
            paras.append(para.text)
    text = ''.join(paras)
    try:
        modified = soup.find('meta', itemprop='dateModified')['content']
    except:
        modified = None
    try:
        headline = soup.find('meta', itemprop='alternativeHeadline')['content']
    except:
        try:
            headline = soup.find('meta', itemprop='headline')['content']
        except:
            headline = None
    article = {
      'headline': headline,
      'modified': modified,
      'text': text,
    }
    
    return article

'''
Parse many articles, given a list of urls.
'''
def parse_many(url_list):
    parsed_list = []

    html_times = []
    soup_times = []
    parse_times = []

    for url_str in tqdm(url_list):
        try:
            html_tic = time.perf_counter()
            page = get_html(url_str)
            html_toc = time.perf_counter()

            soup_tic = time.perf_counter()
            soup = get_soup(page)
            soup_toc = time.perf_counter()

            parse_tic = time.perf_counter()
            article = parse_article(soup)
            parse_toc = time.perf_counter()

            if article['text'] != '':
                parsed_list.append(parse_article(soup))
            
            html_times.append(html_toc - html_tic)
            soup_times.append(soup_toc - soup_tic)
            parse_times.append(parse_toc - parse_tic)
        except Exception as e:
            print(e)
            continue
        
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
        print('Welcome to the CNN Scraper 9000!\n')
        year = int(input('Enter year of interest: '))
        mode = str(input('Scrape a full year(1), or just one month(2)? '))
        if mode == '1':  #scrape a year
            print('Scraping ...')
            month = 'all'
            links = crawl_links_year(year)
            print(f'\nFound {len(links)} articles. Parsing ...')
        elif mode == '2':  #scrape a month
            month = int(input('Enter month of interest as digit: ').replace('0',''))
            print('Scraping ...')
            links = crawl_links_month(year, month)
            print(f'\nFound {len(links)} articles. Parsing ...')
        else:
            print('Invalid input.')
            raise Exception

        parsed_articles, html_times, soup_times, parse_times = parse_many(links)
        print(f'\nParsed {len(parsed_articles)}. Exporting ...')

        print('\nPerformance timing:')
        print(f'\nget_html()\t\tMin: {min(html_times):.6f}\tMax: {max(html_times):.6f}\tAvg: {sum(html_times)/len(html_times):.6f}')
        print(f'get_soup()\t\tMin: {min(soup_times):.6f}\tMax: {max(soup_times):.6f}\tAvg: {sum(soup_times)/len(soup_times):.6f}')
        print(f'parse_article()\t\tMin: {min(parse_times):.6f}\tMax: {max(parse_times):.6f}\tAvg: {sum(parse_times)/len(parse_times):.6f}')

        filename = f'CNN_{year}_{month}.json'
        output_to_json(parsed_articles, filename)
        print(f'\nExported to {filename}. Have a blessed day!')
    except Exception:
        print('Ruh roh.')
    
if __name__ == '__main__':
    main()