from keywords import medical_study_abroad_keywords
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup as BS
from urllib.parse import urlencode, quote_plus, urlparse
from playwright.async_api import async_playwright, TimeoutError, Error  as PlaywrightError
from fake_useragent import UserAgent
import asyncio

import structlog




txt = lambda tag: tag.get_text(" ", strip=True) if tag else ""
get_domain = lambda website: (
    urlparse(website).netloc.replace("www.", "").strip() if website else ""
)
log = structlog.getLogger()


def name_the_file(title):
    current_date = "_".join([datetime.now().strftime(f"%{val}") for val in "dbyHMf"])
    return f'{title.replace(" ", "_").capitalize()}_{current_date}'




class ChaningTag:
    def __init__(self):
        self.ad_div = {"class": "uEierd"}
        self.ad_link = {"class": "x2VHCd OSrXXb ob9lvb"}
        self.ad_headline = {"role": "heading"}
        self.ad_desc_div = {"id": "tads"}
        self.ad_desc = {"class": "Va3FIb r025kc lVm3ye"}
        self.next_page = {"id": "pnnext"}


class GoogleAd(ChaningTag):
    def __init__(self):
        super().__init__()
        self.page_limit = 3
        self.ua =  UserAgent()

        self.query_url = "https://www.google.com/search?"
        self.base_url = "https://www.google.com"
        self.ads_report = []

    def key_to_url(self, keyword):
        payload = {"q": f"{keyword}"}
        decoded_url = urlencode(payload, quote_via=quote_plus)
        return self.query_url + decoded_url

    def get_keyword_ad_list(self, soup, keyword, page, ad_type, ad_position):
        tag = soup.find("div", {"id": ad_type})

        if tag and tag.find_all("div", {"class": "uEierd"}):

            for ad_index, ad_div in enumerate(tag.find_all("div", self.ad_div), 1):

                self.ads_report.append(
                    {
                        "Keyword": keyword,
                        "Ads Rank": ad_index,
                        "Ads Position": ad_position,
                        "Search Page": page,
                        "Website Link": txt(ad_div.find("span", self.ad_link)),
                        "Domain": get_domain(txt(ad_div.find("span", self.ad_link))),
                        "Headline": txt(ad_div.find("div", self.ad_headline)),
                        "Description": txt(ad_div.find("div", self.ad_desc)),
                    }
                )

    async def get_new_context(self,p):
        browser = await p.chromium.launch(headless=False)

     
        return  browser,await browser.new_context(
           
            user_agent = self.ua.random,
            viewport={'width': 1280, 'height': 720},
            locale='en-US',
            geolocation={'longitude': -122.084, 'latitude': 37.421998},
            permissions=['geolocation'],
            device_scale_factor=2,
            color_scheme='dark',
            ignore_https_errors=True


        )

    
    async def start_bot(self, keyword,context,p,browser):
      
            
        gs_url = self.key_to_url(keyword)
        print(gs_url)
        log.info(gs_url)
        ok, current_page = True, 1
    
    

        while ok:
            log.info(f"Fetching the {keyword} - page {current_page}")

            page = None


            try:
                page = await context.new_page()
               
             
            
                await page.goto(gs_url)  # Replace with your target URL

                 # Wait for some time (optional)
                await page.wait_for_timeout(5000)
            
        
                soup = BS(await page.content(), "html.parser")  # Get content from the page

                self.get_keyword_ad_list(soup, keyword, current_page, "taw", "Top Ads")
                self.get_keyword_ad_list(
                    soup, keyword, current_page, "bottomads", "Bottom Ads"
                )

                if soup.find("a", self.next_page) and current_page < self.page_limit:
                    gs_url = self.base_url + soup.find("a", self.next_page)["href"]
                    current_page += 1
                else:
                    ok = False


            except (OSError,TimeoutError,PlaywrightError) as e:

                log.info(f"Failed to navigate: {e}")
                if page:
                    await page.close()
                await context.close()

                if browser:
                    await browser.close()

             
                browser,context = await self.get_new_context(p)

            
                continue
            
            if page:
    
                await page.close()

          

    def save_to_file(self, df):
        df.to_excel(f"{name_the_file('SEO_resuls')}.xlsx", index=False)
        return "File Saved Succesfully!!!"

    def get_keyword_list(self, file):
        log.info(file)
        if ".xlsx" in file:

            data = pd.read_excel(
                file,
            )
        elif ".csv" in file:
            data = pd.read_csv(file)
        return data.iloc[:, 0].to_list()

    async def main(self):

        # Print the combined list of keywords
        log.info(len(medical_study_abroad_keywords))

        async with async_playwright() as p:
           
           
            browser,context = await self.get_new_context(p)
            
           

           
            log.info(f" Len of {len(medical_study_abroad_keywords)}")
            tasks = []
           
          
            for index , keyword in enumerate(medical_study_abroad_keywords):
                log.info(f"Searching for the {index} keyword : {keyword}")
                # await self.start_bot(keyword,context,p,browser)

                task = asyncio.create_task( self.start_bot(keyword,context,p,browser))
                tasks.append(task)
                if len(tasks)==3:
                    await asyncio.gather(*tasks)
                    tasks= []
            if tasks:
                await asyncio.gather(*tasks)
                                           


             
                    
          
            if self.ads_report:

                gsr_df = pd.DataFrame(self.ads_report)
                print(len(self.ads_report))
                self.save_to_file(gsr_df)
            else:
                log.info("invalid file path, using the keyords list in code")

            if browser:
                await browser.close()

          
        

if __name__ == "__main__":
    google_ads = GoogleAd()
    asyncio.run(google_ads.main())