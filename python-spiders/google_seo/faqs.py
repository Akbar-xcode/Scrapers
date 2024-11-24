import traceback
# from selenium import webdriver
from urllib.parse import urlencode, quote_plus
from bs4 import BeautifulSoup as BS
import pandas as pd
import time
from urllib.parse import urlparse
from fake_useragent import UserAgent
from playwright.async_api import async_playwright
from time import time ,sleep
import re
from keywords import medical_study_abroad_keywords
import json
import structlog
from datetime import datetime
import requests
import asyncio
import random
# import chromedriver_autoinstaller



log = structlog.getLogger()



class SuperRequests(requests.Session):
    """
    A custom HTTP client session with enhanced features such as automatic retries and rotating proxies.

    Attributes:
        max_retries (int): The number of retry attempts for failed requests.
        RPROXY (str): Rotating proxy endpoint from environment variables.
    """

    def __init__(self,**kwargs):
       
        super().__init__(**kwargs)
        self.max_retries = 5
        self.ua = UserAgent()
       
    @property
    def aheaders(self):
        return {
        
            "user-agent": self.ua.random,
        
        }
       

    def arequest(
        self, method: str, url: str, _format: str = "text", *args, **kwargs
    ):
        """
        Makes an asynchronous HTTP request with retry logic.

        Args:
            method (str): HTTP method (GET, POST, etc.).
            url (str): URL to request.
            _format (str): The format to return the response in ('text' or 'json').
            *args: Additional arguments for the request.
            **kwargs: Additional keyword arguments for the request.

        Returns:
            str or dict: Response content in specified format, or None if all retries fail.
        """

        for attempt in range(self.max_retries + 1):
            try:
               

                response = super().request(
                    method,
                    url,
                   
                    # timeout=30,
                    headers=self.aheaders,
                    *args,
                    **kwargs,
                   
                )
                
                if response.ok:
                    if _format == "json":
                        return  getattr(response, _format)()
                    

                    return getattr(response, _format)
                    
                log.info(f"Response no ok retrying ... status code ; {response.status_code}")

            except (requests.exceptions.ConnectTimeout) as e:
                # Log the failure and wait before retrying
                log.error(f"Attempt {attempt + 1}/{self.max_retries + 1} failed: {e} For URL : {url}")

            # Wait before the next retry attempt (exponential backoff)
            sleep(2)

        # If all retries fail, return None
        log.error( 
            (
                f"Failed to make a HTTP {method} request to "
                f"URL: {url} after {self.max_retries} attempts."
            )
        )
        return None


class Parser:

    def __init__(self) -> None:
        self.GOOGLE_BASE_URL = "https://www.google.com"

    @staticmethod
    def TXT(tag):

        return tag.get_text(" ", strip=True) if tag else ""
    

    def get_cite(self,tag):
        """get website link from google search"""
        if tag and tag.find("span"):
            return self.TXT(tag).replace(self.TXT(tag.find("span")), "").strip()
        return self.TXT(tag)

    def get_faq_info(self,soup, keyword):
       
        temp = []
       
    
     
        div = soup.find_all("div", {"class": "wQiwMc"})

      

        for qus in div:
            ans_tag = qus.find("div", {"id": re.compile("WEB_ANSWERS_RESULT")})
            h3_tag = qus.find("h3")
            content = {
                "Keyword": keyword,
                "People Also Search Question": self.TXT(qus.find("span", {"class": "CSkcDe"})),
                "People Also Search Short Answer": self.TXT(qus.find("div", {"role": "heading"})),
                "People Also Search Highlight Keywords": (
                    "; ".join([self.TXT(b) for b in ans_tag.find_all("b")]) if ans_tag else ""
                ),
                "People Also Search Answer": self.TXT(qus.find("span", {"class": "hgKElc"})),
                "People Also Search Heading": self.TXT(h3_tag),
                "People Also Search Heading Link": h3_tag.find_parent("a")["href"] if h3_tag else "",
                "People Also Search for": self.TXT(
                    qus.find("div", {"class": "hwqd7e d0fCJc BOZ6hd"})
                ),
                "People Also Search for link": (
                    self.GOOGLE_BASE_URL
                    + qus.find("div", {"class": "hwqd7e d0fCJc BOZ6hd"}).find("a")["href"]
                    if qus.find("div", {"class": "hwqd7e d0fCJc BOZ6hd"})
                    else ""
                ),
                "People Also Search Domain": self.get_cite(qus.find("cite")),
            }
            temp.append(content)
        return temp
    
    @staticmethod
    def generate_related_search_list(html, keyword):
        ctxt = lambda x, y="": x.get_text(strip=True, separator=y) if x else ""

        soup = BS(html, "lxml")

       
        raw_related_searches = [
            ctxt(x, y=" ")
            for x in soup.find("div",{'id':"bres"}).find_all("a",{'class':True})
        ]
        print(raw_related_searches)

        return [
            ["Related Search", keyword, x]
            for x in raw_related_searches
            if x.lower() not in ["", "try again", "more search results"]
            if len(x) > 1
        ]


    @staticmethod
    def get_suggest(content,_keyword):
        de_text, suggest = content.decode(), list()
        

        for sugg in de_text.split("],[")[1:]:

            sugg = re.sub(r"\\u003cb\\u003e|\\u003c\\/b\\u003e", "", sugg)
            sugg = re.findall('".*?"', sugg)
        

            data = [
                "Auto Suggest",
                _keyword,
                sugg[0][1:-1]
            ]
           
            suggest.append(data)


        return suggest



class Crawler:
    def __init__(self) -> None:
        self.parser= Parser()
        self.request = SuperRequests()
        self.results_1 = []
        self.results_2 = []



        self.QUERY_URL = "https://www.google.com/search?"
    
      
        self.proxy = "http://orxhszxi-rotate:hr2h6m4gokt1@p.webshare.io:80/"
       



    async def click_faq(self, driver, counter):
        class_name = "wQiwMc"
        elements = await driver.query_selector_all(f'.{class_name}')  # Ensure driver is a valid Page instance
     

       

        start, temp = counter, []

        for element in elements[start:]:
            soup = BS(await driver.content(), "html.parser")  # Get content from the page

            qus_len_before = len(soup.find_all("div", {"class": class_name}))

            print(qus_len_before)

            await element.click()  # Click on the element
            await driver.wait_for_timeout(3000)
            await self.check_loader(driver)

            soup = BS(await driver.content(), "html.parser")  # Update soup after the click
            qus_len_after = len(soup.find_all("div", {"class": class_name}))

            print(qus_len_before)

            temp += [[counter, i] for i in range(qus_len_before, qus_len_after)]
            counter += 1

        return counter, temp
    
     
    @staticmethod
    async def check_loader(driver):
        soup = BS(await driver.content(), "html.parser")  # Get content from the page

        count = 0
        while count < 5:
            count += 1
            load = soup.find("g-loading-icon")
            if load and load.has_attr("style") and "display: none;" in load["style"]:
                return "True"
            else:
                sleep(1)
        return None


    async def get_keyword_details(self, page, keyword):
        counter = 0
        track_qus = []
        for _ in range(2):
            try:
                log.info(f"Processing keyword: {keyword}")  # Log the current keyword
                counter, track_q = await self.click_faq(page, counter)  # Ensure page is a valid Playwright Page instance
                track_qus += track_q
                await self.check_loader(page)

                await page.wait_for_timeout(2000)
            except Exception as e:
                log.error(f"Exception occurred in generate_keyword_details Function: {e}")

                
        soup = BS(await page.content(), "html.parser")  # Get content from the page

        
        faq_data = self.parser.get_faq_info(soup,keyword)
         

        return faq_data
    
    async def process_keyword(self, keyword,context):
       
        log.info(f"\n\n Kyeword is {keyword}")

        payload = {"q": keyword}
        decoded_url = urlencode(payload, quote_via=quote_plus)
        full_url = self.QUERY_URL + decoded_url

        
        # Create a new page in the context
        page = await context.new_page()

        # Navigate to a URL
        try:
            await page.goto(full_url)  # Replace with your target URL

        except Exception as e:

            log.info(f"Failed to navigate: {e}")
        
            return []  # Return empty if navigation fails

        # Wait for some time (optional)
        await page.wait_for_timeout(5000)

        data_list = await self.get_keyword_details(page, keyword)

        # Close the browser
        await page.close()

       

        return data_list  # Return the retrieved data


    async def main(self):
        start = time()

       
     
      
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
          
            context = await browser.new_context()
            tasks = []
            for keyword in medical_study_abroad_keywords:

                log.info(f"\n\n Getting keyword -  {keyword}")
                # data_list = await self.process_keyword(keyword,context)
                task = asyncio.create_task(self.process_keyword(keyword,context))
                tasks.append(task)

                if len(tasks) == 5:
                   
                    final_results= await asyncio.gather(*tasks)
                    tasks= []
                    for data_list in final_results:
                        if not data_list:

                            continue
                        self.results_1.extend(data_list)


                   
            if tasks:
                final_results= await asyncio.gather(*tasks)
                for data_list in final_results:
                    if not data_list:

                        continue
                    self.results_1.extend(data_list)

        
              

                  
        await browser.close()

      
      
        log.info(f"Total results: {len(self.results_1)}")


        df1 = pd.DataFrame(self.results_1)

       
        for index, keyword in enumerate(all_keywords):
           
          
                
            log.info(f"\n\n Getting keyword - {index} - {keyword}")
            url = f"https://www.google.co.in/search?q={keyword}"


            related_search_kw_google = self.request.arequest("GET",url)
            if not related_search_kw_google:
                continue

            realted_search_list = self.parser.generate_related_search_list(related_search_kw_google,keyword)
            if not realted_search_list:

                continue
            self.results_2.extend(realted_search_list)

            auto_suggest_url = f"https://www.google.com/complete/search?q={keyword}&cp=23&client=gws-wiz-serp&xssi=t&hl=en&authuser=0&pq=temp%20agencies%20near%20me&psi=z_uFY5KoBNCL0PEPuJ6G6Ac.1669725138777&dpr=2"



            
            auto_suggest_content =self.request.arequest("GET",auto_suggest_url,_format="content")

            if not auto_suggest_content:

                continue

            suggenstion_list = self.parser.get_suggest(auto_suggest_content,keyword)

            if not suggenstion_list:

                continue

         
             
            self.results_2.extend(suggenstion_list)

      
        df2 = pd.DataFrame(self.results_2, columns=["Type", "Keyword", "Google Keyword"])

        # Save both DataFrames to the same Excel file with separate sheets

        with pd.ExcelWriter(f'Combined_Results_{str(datetime.today().strftime("%Y-%m-%d"))}.xlsx', engine='openpyxl') as writer:

            df1.to_excel(writer, sheet_name='Google_FAQ_', index=False)
            df2.to_excel(writer, sheet_name='Google_Info', index=False)

        end = time()

        log.info(f"Time taken to Scrape: {end-start}")

        time_taken = end - start
  

        with open("time_taken.txt", "w") as f:

            f.write(f"Time taken to scrape: {time_taken:.2f} seconds\n")
           


if __name__=="__main__":
    log.info("\n\n *****Initializing the Crawler*****\n\n")
    crawler = Crawler()
    asyncio.run(crawler.main())
  
  
                