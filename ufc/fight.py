import requests
import bs4
import pandas as pd
import sqlalchemy

def bs4_to_df_fight_fighter( table: bs4.BeautifulSoup, type_data: str):
    '''Coleta as informações de cada um dos jogadores'''
    result = table.findAll("p", class_="b-fight-details__table-text")
    newdata = []
    for r in result:
        newdata.append(r.text.strip("\n").strip(" ").rstrip("\n"))

    if type_data == "totals":
        cols = ["FIGHTER", "KD", "SIG. STR.", "SIG. STR. %",
                "TOTAL STR.", "TD", "TD %", "SUB. ATT", "REV.", "CTRL"]
    else:
        cols = ["FIGHTER", "SIG. STR", "SIG. STR. %", "HEAD",
                "BODY", "LEG", "DISTANCE", "CLINCH", "GROUND"]

    df = pd.DataFrame()
    for i in range(1, 3):
        values = newdata[i-1::2]
        df_temp = pd.DataFrame([values], columns=cols)
        df = pd.concat([df, df_temp], axis=0).reset_index(drop=True)
    return df

def get_winner(bsObject):
    '''Identifica o vencedor da partida'''
    details_person = bsObject.findAll("div", class_="b-fight-details__person")
    for i in range(len(details_person)):
        if "W" in details_person[i].find("i").text:
            return i

def get_bs4Object(url):
    response = requests.get(url).text
    return bs4.BeautifulSoup(response, 'lxml')

def get_all_fight_fighter(bsObject):
    ''' Obtem todas informações referentes aos lutadores'''
    tables = bsObject.findAll("tbody", class_="b-fight-details__table-body")
    totalsDF = bs4_to_df_fight_fighter( tables[0], type_data="totals")
    strikesDF = bs4_to_df_fight_fighter( tables[-int(len(tables)/2)], type_data="strikes")
    df_full = pd.concat([totalsDF,strikesDF], axis=1)
    winner = get_winner(bsObject)
    df_full['WINNER'] = [1,0] if winner == 0 else [0,1]
    return df_full

def data_to_db(data, engine, table):
    '''Realiza o INSERT de dados conferindo se o ID já existe'''
    ids = ",".join([ f"'{i}'" for i in data['id'].values])
    try:
        engine.execute(f"DELETE FROM {table} AS t1 WHERE t1.id in ({ids});")
    except:
        pass
    data.to_sql(table, engine, if_exists="append", index=False)
    return None

def get_fight_detail(bsObject):
    '''Obtem as informações da luta'''
    div = bsObject.find("div", class_="b-fight-details__content")
    ps = div.findAll("p")

    full_data = {}
    for p in ps:
        text_slice = [i.strip(" ") for i in p.text.split("\n") if i!=""]
        keys, values = [], []
        for v in text_slice:
            if v.endswith(":"):
                keys.append(v.strip(":"))
            elif v != "":
                values.append([v])

        full_data.update(dict( zip(keys, values)))
    return pd.DataFrame(full_data)

def process_all_fight(url):
    '''Coleta as informações da partida e dos lutadores'''
    bsObject = get_bs4Object(url)

    # Coletando jogadores
    data_fighters = get_all_fight_fighter(bsObject)
    data_fighters['id'] = url.split("/")[-1]

    # Coletando info da partida
    data_fight = get_fight_detail(bsObject)
    data_fight['id'] = url.split("/")[-1]

    return {"tb_fight_fighter": data_fighters, "tb_fight": data_fight}