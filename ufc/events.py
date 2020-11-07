import requests
import bs4
import pandas as pd
from tqdm import tqdm

def get_event_bsObject(url_event:str):
    '''Obtem estrutura de dados do Bs4 para consultas'''
    return bs4.BeautifulSoup(requests.get(url_event).text , "lxml")
    
def get_all_event_links(bsObject):
    '''Obtem os links de todas as lutas do envento.'''
    links_a = bsObject.findAll( "a", class_ = "b-flag b-flag_style_green")
    urls = [i['href'] for i in links_a]
    return urls

def get_events_links():
    '''Obtem todos os links de todos os eventos'''
    result = requests.get("http://ufcstats.com/statistics/events/completed?page=all").text
    bsObject = bs4.BeautifulSoup(result, "lxml")
    list_link = [ i['href'] for i in bsObject.find("tbody").findAll("a") ]
    return list_link

def get_event_date_loc(bsObject):
    '''Obtem a data e localização do evento'''
    ul = bsObject.find("ul", class_="b-list__box-list")
    text_slice = [i.strip(" ") for i in ul.text.split("\n") if i != ""]
    keys, values = [], []
    for v in text_slice:
        if v.endswith(":"):
            keys.append(v.strip(":"))
        elif v != "":
            values.append([v])

    return pd.DataFrame(dict(zip(keys, values)))
