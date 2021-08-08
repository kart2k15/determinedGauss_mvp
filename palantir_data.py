#!/usr/bin/env python
# coding: utf-8

# In[1]:


import awswrangler as wr
import pandas as pd
import numpy as np
import requests
import json
import re
import fuzzy_match
from fuzzy_match import algorithims
from fuzzywuzzy import fuzz
pd.options.display.max_rows = 4000
import requests
import time
from googlesearch import search
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from googleapiclient.discovery import build
from selenium.webdriver.common.action_chains import ActionChains


# In[6]:


def parse_html_table(table):
    table_rows = []
    for tr in table.find_all("tr"):
        row = []
        for td in tr.find_all("td"):
            row.append(td.text)
        table_rows.append(row)
    return table_rows


# In[8]:


def get_instit_raw_nasdaq_data(tickr):
    instit_total = []
    url = "https://www.nasdaq.com/market-activity/stocks/{}/institutional-holdings".format(tickr.lower())
    driver = webdriver.Chrome(executable_path="/usr/bin/chromedriver")
    actions = ActionChains(driver)
    driver.get(url)
    soup = BeautifulSoup(driver.page_source)
    total_pages = soup.find_all("button", {"class": "pagination__page"})
    total_instit_rows = []
    
    def parse_page():
        soup = BeautifulSoup(driver.page_source)
        instit_div = soup.find("div", {"class":"institutional-holdings__data-container institutional-holdings__data-container--scrollable loaded"})
        instit_table = instit_div.find("table", {"class":"institutional-holdings__table"})
        instit_data = parse_html_table(instit_table)
        total_instit_rows.extend(instit_data)
    
    
    parse_page()#parsed 1st page
    t = random.randint(10,15)
    time.sleep(t)
    min_page_num = int(total_pages[0].text)
    max_page_num = int(total_pages[-1].text)
    for pnum in range(min_page_num+1, max_page_num+1):
        page_btn = driver.find_element_by_xpath("//button[@class='pagination__next']")
        actions.move_to_element(page_btn).click().perform()
        time.sleep(t)
        parse_page()
    driver.quit()
    return total_instit_rows


# In[9]:


instit_raw_data = get_instit_raw_nasdaq_data("PLTR")


# In[11]:


instit_raw_data = [row for row in instit_raw_data if len(row)!=0]


# In[13]:


def get_instit_df(raw_data):
    raw_data = [row for row in instit_raw_data if len(row)!=0]
    df = pd.DataFrame(raw_data, columns=["owner_name", "date", "shares_held",
                                         "change_in_shares", "change_%", "value_X1000"])
    return df


# In[14]:


instit_df = get_instit_df(instit_raw_data)


# In[16]:


instit_df.info()


# In[17]:


instit_df.to_csv("pltr_instit_df.csv", index=False, header=True, sep="\t", line_terminator="\n")


# In[ ]:




