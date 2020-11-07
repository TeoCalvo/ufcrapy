from ufc import fight
from ufc import events
from tqdm import tqdm
import pandas as pd
import os
import sqlalchemy

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'ufc.db')

if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)

engine = sqlalchemy.create_engine(f"sqlite:///{DB_PATH}")

for i in tqdm(events.get_events_links()):
    bsObject = events.get_event_bsObject(i)
    date_loc_df = events.get_event_date_loc(bsObject)
    for fight_url in events.get_all_event_links(bsObject):
        for t, d in fight.process_all_fight(fight_url).items():
            if t == 'tb_fight':
                d = pd.concat( [d,date_loc_df], axis=1 )
            fight.data_to_db(d, engine, t)