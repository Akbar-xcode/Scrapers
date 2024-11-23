import requests
from bs4 import BeautifulSoup as BS
from urllib.parse import urljoin
import structlog
import re , json
import time
from fake_useragent import UserAgent

log = structlog.get_logger()

class SuperRequests(requests.Session):
    """
    A custom requests.Session subclass that adds custom headers (including a random user-agent) and
    supports automatic retries for requests.
    """

    def __init__(self):
        """
        Initializes the SuperRequests session, sets up a random user-agent, 
        and sets the maximum retries to 5.
        """
        super().__init__()
        self.ua = UserAgent()
      
        self.max_retries =5

    @property
    def get_headers(self):
        """
        Returns a dictionary of headers including a random user-agent and a referer header.
        """
        
        return {
            'referer': 'https://www.amazon.in/',
            "user-agent" : self.ua.random
        }
    
    def super_requests(self,method,url,_format="text",**kwargs):
        """
        Performs an HTTP request with retry logic. Retries the request up to `max_retries` times 
        if there are exceptions or non-200 status codes. 

        Args:
            method (str): The HTTP method to use (GET, POST, etc.)
            url (str): The URL to request.
            _format (str): The response format to return (default is "text", can be "json").
            **kwargs: Additional arguments to pass to the `requests` method.

        Returns:
            str or dict: The response in the specified format (e.g., text or JSON), or None if request fails.
        """

     

        for attempt in range(self.max_retries):
            try:
                response = super().request(method,url,headers=self.get_headers,**kwargs)
                log.info(response.status_code)
                if response.ok:

                    return getattr(response,_format)
                

                log.info(f"\n\n Response Status Not Ok : {response.status_code}")


            except (requests.RequestException,requests.Timeout) as exception:
                log.info(f"\n\n Exception occured : {exception}")

            time.sleep(2 ** attempt + 1)  # Exponential backoff

        return None


class Parser:
    """
    A class responsible for parsing HTML content and extracting product data from Amazon's website.
    """
    def __init__(self):
        """
        Initializes the base URL for Amazon and the parser.
        """
        self.base_url = "https://www.amazon.in"


    def get_bestsellers_urls(self,html):
        """
        Parses the best-sellers page and returns a list of department URLs.

        Args:
            html (str): The HTML content of the best-sellers page.

        Returns:
            list: A list of dictionaries containing department names and their corresponding URLs.
        """
        soup = BS(html,"lxml")
        div_tag = soup.find("div",{'class':"_p13n-zg-nav-tree-all_style_zg-browse-group__88fbz"})
        a_tags  = div_tag.find_all("a")

        if not a_tags:

            return []

        return [{"dept":a_tag.get_text(strip=True),"URL":urljoin(self.base_url,a_tag.get("href"))} for a_tag in a_tags]

    def get_product_urls(self,html):
        """
        Extracts product URLs from a category or department page.

        Args:
            html (str): The HTML content of the department page.

        Returns:
            list: A list of product URLs found in the page.
        """
        soup = BS(html,"lxml")
        a_tags = soup.find_all("a",{'role':"link"})

        if not a_tags:

            return []

        return [urljoin(self.base_url,a_tag.get("href")) for a_tag in a_tags]

    def parse_html(self,html,url,dept):
        """
        Parses the product page HTML and extracts key product details.

        Args:
            html (str): The HTML content of the product page.
            url (str): The URL of the product page.
            dept (str): The department or category the product belongs to.

        Returns:
            dict: A dictionary containing the extracted product data.
        """
        data = {}
        soup = BS(html,"lxml")
       
       
        if  product_title := soup.find("h1",{"id":True}):
            data["Product Title"] = product_title.get_text(strip=True)

        if price := soup.find("span",{'class':"a-price-whole"}):
            data["Price"] = price.get_text(strip=True)

        if visit_store := soup.find("a",{'id':"bylineInfo"}):
            data["Store URL"] = urljoin(self.base_url,visit_store.get("href"))
        
        if ratings := soup.find("span",{'class':"a-icon-alt"}):
            data["Ratings"] = ratings.get_text(strip=True)
        
        if reviews := soup.find("span",{'id':"acrCustomerReviewText"}):
            data["Number of reviews"] = reviews.get_text(strip=True)

        if image_box := soup.find("div",{'id':"altImages"}):
            imgs = image_box.find_all("img",src=re.compile("media",re.I))

            data["Image URLS"] = [src.get("src") for src in imgs]
        if any(data.values()):
            data.update(self.get_static_data(url,dept))

        return data
            

    @staticmethod
    def get_static_data(url,dept):
        """
        Returns static information about the product, such as its URL and department.

        Args:
            url (str): The URL of the product.
            dept (str): The department the product belongs to.

        Returns:
            dict: A dictionary containing the static product data.
        """

        return {
            "Product URL":url,
            "Department":dept
        }


class Scraper:
    """
    A class that orchestrates the scraping of Amazon's best-sellers list and product details.
    """
    def __init__(self):
        """
        Initializes the base URL, the best sellers API URL, and the parser.
        """
        self.base_url = "https://www.amazon.in"
        self.best_sellers_api = "https://www.amazon.in/gp/bestsellers/"
        self.parser = Parser()

    
    def loop_sellers_list(self,session,best_sellers):
        """
        Loops through the best-sellers list, fetching product URLs and extracting data.

        Args:
            session (SuperRequests): The requests session to use for making HTTP requests.
            best_sellers (list): A list of best-sellers departments with their URLs.

        Returns:
            list: A list of dictionaries containing product data.
        """
        data_list = []
        
        for sellers in best_sellers:
            url = sellers.get("URL")
            dept = sellers.get("dept")

            log.info(f"\nFetching Product URLS for Deparrtment : {dept}")
            

            seller_page = session.super_requests("GET",url)

            product_urls = self.parser.get_product_urls(seller_page)

            for product_url in product_urls:
                with SuperRequests() as session:

                    time.sleep(5)

                    log.info(f"Fetching data from Product : {product_url}")

                    detail_page =  session.super_requests("GET",product_url)
                    
                    if not detail_page:

                        continue

                    data = self.parser.parse_html(detail_page,product_url,dept)
                    if data:

                        data_list.append(data)

        return data_list


           

        


    def main(self):
        """
        Main method to orchestrate the scraping process. Fetches the best-sellers data, loops through 
        departments, and collects product information. Saves the data to a JSON file.

        Retries up to 3 times in case of failure.
        """
        for retry in range(3):
            with SuperRequests() as session:
                main_page = session.super_requests("GET",self.base_url)

                if not main_page:

                    continue

                
                params = {
                    'ref_': 'nav_cs_bestsellers',
                }

                best_sellers = session.super_requests("GET",self.best_sellers_api,params=params)

                if not best_sellers:

                    continue

                sellers_urls = self.parser.get_bestsellers_urls(best_sellers)

                print(sellers_urls)

                sellers_data = self.loop_sellers_list(session,sellers_urls)

                json_o = json.dumps(sellers_data,indent=4)
                with open("bestsellers.json","w") as o:
                    o.write(json_o)


                break
       
if __name__== "__main__":
    log.info("\n\n Initializing the scraper \n\n")
    scraper = Scraper()
    scraper.main()

    