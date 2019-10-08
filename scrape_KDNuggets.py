import requests
from bs4 import BeautifulSoup

url = "https://www.kdnuggets.com/2019/01/active-blogs-ai-analytics-data-science.html"

page = requests.get(url, timeout = 5)

soup = BeautifulSoup(page.content, "html.parser")

x =  soup.div.ol.text

print(x)