#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 20:18:00 2018

Demonstrator for ThreadPoolExecutor from concurrent.futures
Multi-threaded Web Crawler
Can be adapted for IO-bound workload
Example from http://edmundmartin.com/multi-threaded-crawler-in-python/

@author: zeyi
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import queue
from concurrent.futures import ThreadPoolExecutor, Future

#Note: If using ProcessPoolExecutor, use multiprocessing.Queue instead

class MultiThreadCrawler:
    def __init__(self, base_url: str):
        assert(base_url is not None)
        self.base_url = base_url
        parsed_url = urlparse(base_url)
        self.root_url = "{}://{}".format(parsed_url.scheme, parsed_url.netloc)
        self.executor = ThreadPoolExecutor() # ThreadPoolExecutor automatically sets number of threads to 5x cores
        self.to_crawl = queue.Queue()
        self.to_crawl.put(self.base_url)
        self.scraped_pages = set()
        
    def parse_links(self, html: str):
        """Parse a raw html text for href links"""
        soup = BeautifulSoup(html)
        links = soup.find_all('a', href=True)
        for link in links:
            url = link['href']
            # Check if it is a internal link. Can be relative or absolute
            # TODO: Improvement for protocol switches (https -> http)
            if url.startswith('/') or url.startswith(self.root_url):
                url = urljoin(self.root_url, url)
                if url not in self.scraped_pages:
                    self.to_crawl.put(url)
                    #self.scraped_pages.add(url)
                    
    def custom_scraper(self, html):
        """Perform business logic here"""
        pass
    
    def post_scrap_callback(self, res: Future):
        """To perform after main request function. Can be more CPU-intensive"""
        result = res.result() #Extract result from future
        if result and result.status_code == 200:
            self.parse_links(result.text)
            self.custom_scraper(result.text)
            
    def request_page(self, url: str):
        """Function to be thrown into the thread pool. 
        Recommended to be extremely CPU-light to increase speed of crawler"""
        try:
            res = requests.get(url)
            return res # Note that this could have a 404 request
        except requests.RequestException:
            return
        
    def run_scraper(self):
       """Main loop for scraper"""
       while True:
           try:
               #Retrieve newest page from queue
               target_url = self.to_crawl.get(timeout=10)
               if target_url not in self.scraped_pages:
                   print("Scraping: %s" % target_url)
                   self.scraped_pages.add(target_url)
                   future = self.executor.submit(self.request_page, target_url)
                   #TODO: To understand why we add a post-done callback instead of plugging in the main callback
                   future.add_done_callback(self.post_scrap_callback) 
           except queue.Empty: #Nothing left in queue
               print("Task complete")
               return self.scraped_pages
           except Exception as e:
               print(e)
               continue
        
if __name__ == "__main__":
    
    crawler = MultiThreadCrawler("http://www.domainhole.com")
    res = crawler.run_scraper()
    print("Done")
    

