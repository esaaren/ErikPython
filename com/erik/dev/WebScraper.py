import requests
import os
from xml.etree import ElementTree
from BeautifulSoup import BeautifulSoup
from urllib import urlretrieve
import re
import logging

logging.basicConfig()
logger = logging.getLogger('H&M SCRAPER')
logger.setLevel(logging.INFO)


def removeStringSpecialCharacters(s):
    # Replace special characters with " "
    stripped = re.sub("[^\w\s\-\_]", "", s)
    # Change any whitespace to no space
    stripped = re.sub("\s+", "", stripped)
    # Remove start and end whitespace
    stripped = stripped.strip()
    return stripped

def run():
    url = 'https://www2.hm.com/en_ca/women/shop-by-product/shirts-and-blouses.html?product-type=ladies_shirtsblouses&sort=stock&image-size=small&image=model&offset=0&page-size=1024'
    home_path = '/Users/erik.saarenvirta/Documents/Work/Projects/crawler/data/'
    response = requests.get(url)
    parsed_html = BeautifulSoup(response.content)
    for a in parsed_html.body.findAll('a'):
        if 'productpage' in a['href']:
            product_url = 'https://www2.hm.com' + a['href']
            product_code = product_url.split('productpage.')[1].split('.')[0]
            folder_create_path = home_path + product_code
            os.system('mkdir -p {}'.format(folder_create_path))
            print 'Product code is: ', product_code
            print "Grabbing URL: ", product_url
            response = requests.get(product_url)
            parsed_page = BeautifulSoup(response.content)
            for image in parsed_page.body.findAll('img'):
                if 'hm.com' in image.get('src'):
                    image_source = 'https:' + image.get('src')
                    image_name = removeStringSpecialCharacters(image.get('alt'))
                    print 'Page Image Source: ', image_source
                    print 'Getting Image...'
                    urlretrieve(image_source, "{}/{}.jpg".format(folder_create_path, image_name))



if __name__ == '__main__':
    run()