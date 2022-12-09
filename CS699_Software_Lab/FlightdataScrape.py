#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

def Convert(string):
    li = list(string.split("\n"))
    return li

PATH = "/home/prathamy/Documents/Documents/ACADS/SEM1/SWL/Project/chromedriver_linux64/chromedriver"
driver1 = webdriver.Chrome(PATH)
driver2 = webdriver.Chrome(PATH)
driver3 = webdriver.Chrome(PATH)

loc1="DEL-Delhi"
loc2="BOM-Mumbai"

date1="2022-12-10"
date2="10122022"
# date3="20221210"

site1 = "https://paytm.com/flights/flightSearch/"+loc1+"/"+loc2+"/1/0/0/E/"+date1
site2 = "https://www.flipkart.com/travel/flights/search?trips="+loc1[:3]+"-"+loc2[:3]+"-"+date2+"&travellers=1-0-0&class=e&tripType=ONE_WAY&isIntl=false&source=Search%20Form"
site3 = "https://flight.yatra.com/air-search-ui/dom2/trigger?ADT=1&CHD=0&INF=0&class=Economy&destination="+loc2[:3]+"&destinationCountry=IN&flexi=0&flight_depart_date="+date2[:2]+"%2F"+date2[2:4]+"%2F"+date2[4:]+"&hb=0&noOfSegments=1&origin="+loc1[:3]+"&originCountry=IN&type=O"

driver1.get(site1)
driver2.get(site2)
driver3.get(site3)

time.sleep(35)

prices1 = driver1.find_element_by_xpath("/html/body/div/div/div[2]/div/div/div/div[2]/div/div[1]/div[3]/div")
prices2 = driver2.find_element_by_xpath("/html/body/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[1]/div[3]/div[1]/div")
prices3 = driver3.find_element_by_xpath("/html/body/section[2]/section/section[2]/section[1]/div[2]/div[2]/div/div[1]/div")

print(Convert(prices1.text))
print(Convert(prices2.text))
print(Convert(prices3.text))

driver1.quit()
driver2.quit()
driver3.quit()