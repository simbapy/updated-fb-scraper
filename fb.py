from facebook_scraper import get_posts
from datetime import datetime
import pandas as pd
import json
import logging
import string
import itertools as it

page_names = ['Baladna','lacasabusinessgroup','LifeWithCacaoKW',
             'Sazeli.QA','SmatRestaurant','BastaGourmetBar','builditburger',
             'ChaiHalibQA','remmancafe','DebsWRemman','Gahwetna',
             'Orient.Pearl.Rest','karakiqatar','JwalaQatar','mokarabiaqatar']
cols = ['post_id','url','text','media_url','time','likes','comment_count']

# Removes none ascii characters from string
def rem_nascii(d):
    printable = set(string.printable)
    d = ''.join(filter(lambda x: x in printable, d))
    return d


# Drops trailing characters from a string
def rem_c(d):
    d = "".join(it.dropwhile(lambda x: not x.isalpha(), d))
    return d


def scrape_posts(page_name):
    posts = get_posts(page_name, pages=1)
    data_list = list(posts)
    df = pd.DataFrame(data_list)
    try:
        df.drop(['post_text','shared_text','shares','link'],axis=1,inplace=True)
    except KeyError:
        pass
    df.columns = ['post_id','text','time','media_url','likes','comment_count','url']
    df = df[cols]
    df.text = df.text.apply(rem_nascii)
    df.text = df.text.apply(rem_c)
    df['page_name'] = page_name
    df['site'] = 'facebook'

    return df

def save_latest_time(data):
    """Converts data to JSON.
    """
    with open('latest_fb_time.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)


    try:
        with open('latest_fb_time.json', 'r') as f:
            latest_json = json.load(f)
    except FileNotFoundError:
        latest_json = None
    
    result_list = []
    post_id_list = []

    logging.info('Beginning facebook scraping.')

    for page_name in page_names:
        logging.info('Scraping {} page.'.format(page_name))

        result = scrape_posts(page_name)
        post_ids = list(result['post_id'])
        result_list.append(result)
        post_id_list.append(post_ids)
    
    results_super = pd.concat(result_list)
    if latest_json is not None:
        results_super[~results_super['post_id'].isin(latest_json['ids'])]
    else:
        pass
    results_super.drop(['post_id'],axis=1,inplace=True)

    logging.info('Saving csv!')
    results_super.to_csv('fb_results ({}).csv'.format(datetime.now().date().strftime("%d-%m-%Y")),index=False)

    post_id_list = list(it.chain.from_iterable(post_id_list))
    save_latest_time({'ids':post_id_list})



