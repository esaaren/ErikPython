import requests
import os
from xml.etree import ElementTree
from BeautifulSoup import BeautifulSoup
from urllib import urlretrieve
import re
import logging
import pickle

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

    collect_all_at_once = False

    url_dict = {}

    if collect_all_at_once == True:
        url_dict['all'] = 'https://www2.hm.com/en_ca/women/shop-by-product/view-all.html?sort=stock&image-size=small&image=model&offset=0&page-size=8000'

    else:
        url_dict['blouse'] = 'https://www2.hm.com/en_ca/women/shop-by-product/shirts-and-blouses.html?product-type=ladies_shirtsblouses&sort=stock&image-size=small&image=model&offset=0&page-size=1024'
        url_dict['top'] = 'https://www2.hm.com/en_ca/women/shop-by-product/tops.html?product-type=ladies_tops&sort=stock&image-size=small&image=model&offset=0&page-size=496'
        url_dict['dress'] = 'https://www2.hm.com/en_ca/women/shop-by-product/dresses.html?product-type=ladies_dresses&sort=stock&image-size=small&image=model&offset=0&page-size=1024'
        url_dict['cardigan'] = 'https://www2.hm.com/en_ca/women/shop-by-product/cardigans-and-jumpers.html?product-type=ladies_cardigansjumpers&sort=stock&image-size=small&image=model&offset=0&page-size=1024'
        url_dict['jacket'] = 'https://www2.hm.com/en_ca/women/shop-by-product/jackets-and-coats.html?product-type=ladies_jacketscoats&sort=stock&image-size=small&image=model&offset=0&page-size=256'

    home_path = '/Users/erik.saarenvirta/Documents/Work/Projects/crawler/data/'

    products_saved = []

    try:
        with open('products_saved.pickle', 'rb') as handle:
            products_saved = pickle.load(handle)
    except Exception as e:
        logger.error(e)
        logger.warn('This is probably first time run ... instantiating save array')
        products_saved = []

    for url_key in url_dict:
        url = url_dict[url_key]
        logger.info('Working on products from: ' + url_key)
        response = requests.get(url)
        parsed_html = BeautifulSoup(response.content)
        for a in parsed_html.body.findAll('a'):
            if 'productpage' in a['href']:
                product_url = 'https://www2.hm.com' + a['href']
                if product_url not in products_saved:
                    product_code = product_url.split('productpage.')[1].split('.')[0]
                    folder_create_path = home_path + product_code
                    os.system('mkdir -p {}'.format(folder_create_path))
                    logger.info('Product code is: ' + product_code)
                    logger.info('Grabbing URL: ' + product_url)
                    try:
                        response = requests.get(product_url)
                        parsed_page = BeautifulSoup(response.content)
                        for product_desc in parsed_page.body.findAll("p", {"class": "pdp-description-text"}):
                            try:
                                desc_file = open('{}/{}'.format(folder_create_path, product_code+'_desc.txt'), 'w')
                                desc_file.write(str(product_desc.text.encode('utf-8')))
                                desc_file.close()
                            except Exception as e:
                                logger.error('Failed to write description for prod: ' + product_code)
                                logger.error(e)

                        for image in parsed_page.body.findAll('img'):
                            if 'hm.com' in image.get('src'):
                                image_source = 'https:' + image.get('src')
                                image_name = removeStringSpecialCharacters(image.get('alt'))
                                logger.info('Page Image Source: ' + image_source)
                                logger.info('Getting Image...')
                                try:
                                    urlretrieve(image_source, "{}/{}.jpg".format(folder_create_path, image_name))
                                    products_saved.append(product_url)
                                    with open('products_saved.pickle', 'wb') as handle:
                                        pickle.dump(products_saved, handle, protocol=pickle.HIGHEST_PROTOCOL)

                                except Exception as e:
                                    logger.error('Failed to retrieve image: ' + image_source)
                                    logger.error(e)

                    except Exception as e:
                        logger.error('Failed to get the product with url: ' + product_url)
                        logger.error(e)

                else:
                    logger.info('Already saved product: ' + product_url)


if __name__ == '__main__':
    run()