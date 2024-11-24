import requests
from bs4 import BeautifulSoup as BS
from urllib.parse import urljoin
import structlog
import re , json
import pandas as pd
import time
from fake_useragent import UserAgent
from browserforge.headers import HeaderGenerator

log = structlog.get_logger()



class SuperRequests(requests.Session):

    def __init__(self):
        super().__init__()
        self.ua = UserAgent()
        self.h = HeaderGenerator()
        self.max_retries =5

    @property
    def get_headers(self):

        while True:
            try:

                return self.h.generate()
            except ValueError as e:
                log.info(f"Exeption occured while Genearteing Headers : {e}")

    
    def arequests(self,method,url,_format="text",**kwargs):

     

        for attempt in range(self.max_retries):
            try:
                response = super().request(method,url,headers=self.get_headers,**kwargs)
                log.info(response.status_code)
                if response.ok:

                    return getattr(response,_format)
                

                log.info(f"\n\n Response Status Not Ok : {response.status_code}")


            except (requests.RequestException,requests.Timeout) as exception:
                log.info(f"\n\n Exception occured : {exception}")

            time.sleep(2**attempt + 1 )

        return None


class Parser:
    def __init__(self):
        self.base_url = "https://www.amazon.in"


    def get_bestsellers_urls(self,html):
        soup = BS(html,"lxml")
        div_tag = soup.find("div",{'class':"_p13n-zg-nav-tree-all_style_zg-browse-group__88fbz"})
        a_tags  = div_tag.find_all("a")

        if not a_tags:

            return []

        return [{"dept":a_tag.get_text(strip=True),"URL":urljoin(self.base_url,a_tag.get("href"))} for a_tag in a_tags]

    @staticmethod
    def get_product_urls(html):
        soup = BS(html,"lxml")
        a_tags = soup.find_all("a",class_="product-tile__image-link productLink")

        if not a_tags:

            return []

        return [a_tag.get("href") for a_tag in a_tags]

    @staticmethod
    def get_upc_number(soup):
        scripts = soup.find_all("script")
        for script in scripts:
            script_content = script.text
            if "upcNumberWithPad" not in script_content:

            

                continue
        
            # Regular expression to find the value assigned to upcNumberWithPad
            pattern = re.compile(r"var upcNumberWithPad = '(.*?)'.padStart")
            
            # Search for the pattern in the JavaScript code
            match = re.search(pattern, script_content)
            
            # Extract the value if the pattern is found
            if match:
                upc_value = match.group(1)

                return {"UPC":upc_value}
        return {}

      
    def parse_html(self,html,url):
        data = {}
        soup = BS(html,"lxml")
      

       
        if  product_title := soup.find("h1",itemprop="name"):
            data["Product Name"] = product_title.get_text(strip=True).replace("u00a02","")

        if article := soup.find("meta",itemprop="sku"):
            data["Article"] = article.get("content","")

        if item :=soup.find("span",string=re.compile("item #",re.I)):
            data["Item"] = item.get_text(strip=True).replace("Item #000","")
        
        if model := soup.find("meta",itemprop="mpn"):
            data["Model"] = model.get("content","")
        
        if format := soup.find("span",class_="format"):
            data["Format"] = format.get_text(strip=True).removeprefix("Format ")

        if reviews := soup.find("span",class_="bvseo-ratingValue"):
            data["Reviews"] = reviews.get_text(strip=True)

        if reviews_count := soup.find("span",itemprop="reviewCount"):
            data["Review Count"] = reviews_count.get_text(strip=True)

        if price := soup.find("span",class_="price-box__price__amount"):
            data["Price"] = price.get_text(strip=True)
        
        data.update(self.get_upc_number(soup))

        if any(data.values()):
            data.update(self.get_static_data(url))

        return data
            

    @staticmethod
    def get_static_data(url):

        return {
            "URL":url,
        }





class Scraper:
    def __init__(self):
        self.base_url = "https://www.rona.ca/en"
      
        self.parser = Parser()

    def paginate(self,session):
        page =2
        page_url = "https://www.rona.ca/webapp/wcs/stores/servlet/RonaAjaxCatalogSearchResultView"
        max_iteration= 0
        breaking_loop = False
        urls_list = []
        while not breaking_loop:
            if page == 30:

                breaking_loop = True

                continue
           
            params = {
                'navDescriptors': '',
                'catalogId': '10051',
                'searchLanding': 'online-exclusive',
                'keywords': '',
                'sortList': '',
                'pageSize': '',
                'storeIdentifier': '82633',
                'navRangeFilters': '',
                'searchKey': '',
                'storeId': '10151',
                'langId': '-1',
                'content': 'Products',
                'infiniteScroll': 'true',
                'page':  page ,
            }

          

            index_page = session.arequests("GET",page_url,params=params)

            if not index_page:
                max_iteration+=1
                if max_iteration ==3:

                    breaking_loop = True

                    continue

                  


                continue


            urls= self.parser.get_product_urls(index_page)
            if not urls:

                breaking_loop = True
                continue

            log.info(f"\n\n Len of URLS : {len(urls)} for page : {page}")

            urls_list.extend(urls)

            page+=1

        return urls_list
    
    # Function to split the list into chunks of size n
    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]


            
    def process_urls(self,urls):
        data_list = []
        for i in range(0, len(urls), 50):
            chunk = urls[i:i + 50]  # Get the chunk of URLs

            log.info(f"len of chunk : {len(chunk)}")
            with SuperRequests() as session:

                for url in chunk:

                    detail_page = session.arequests("GET",url)

                    if not detail_page:

                        continue

                    data = self.parser.parse_html(detail_page,url)

                    if not data:

                        continue

                    data_list.append(data)
        return data_list

        

    def main(self):
        search_url = "https://www.rona.ca/en/home-improvement-categories-roducts-online-only"

        for retry in range(3):
            with SuperRequests() as session:

                # to statr the session 
                main_page = session.arequests("GET",self.base_url)

                if not main_page:

                    continue

              

                index_page = session.arequests("GET",search_url)

                if not index_page:

                    continue

                urls = self.parser.get_product_urls(index_page)
                log.info(f"len of URLS : {len(urls)} from index page ")

                page_urls = self.paginate(session)

                log.info(f"len of URLS : {len(page_urls)} for page_urls")

                urls.extend(page_urls)

                log.info(f"len of total URLS : {len(urls)}")

                data_list= self.process_urls(urls)
                
                df = pd.DataFrame(data_list)

                log.info(f"\n\n Len OF Data list : {len(data_list)}")

                df.to_excel(f"Rona_scraped_data.xlsx", index=False)


             
                json_o = json.dumps(data_list,indent=4)
                with open("rona.json","w") as o:
                    o.write(json_o)


                break
                
                

if __name__== "__main__":
    scraper = Scraper()
    scraper.main()

    