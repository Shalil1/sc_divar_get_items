import psycopg2
import requests
import json
import schedule
import time

hostname = '***'
username = '***'
password = '***'
database = '***'


def get_last_post_date_from_db():
    connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = connection.cursor()
    cur.execute("""select * from last_lastpostdate""")
    rows = cur.fetchall()
    connection.close()
    return rows


def get_items_from_divar():
    rows = get_last_post_date_from_db()  # 'each row present a category for example phone,laptop ...'

    connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = connection.cursor()

    for row in rows:
        last_post_date = int(row[0])
        while True:
            request_data = {"jsonrpc": "2.0", "id": 0, "method": "getPostList", "params": [
                [["place2", 0, ["1"]], ["cat3", 0, [int(row[1])]], ["cat2", 0, [24]], ["cat1", 0, [1]]],
                last_post_date]}

            r = requests.post('https://search.divar.ir/json/', json=request_data)
            time.sleep(15)
            items = json.loads(r.text)
            new_last_post_date = items['result']['last_post_date']
            if new_last_post_date == -1:
                print('The end of items')
                return
            last_post_date = new_last_post_date
            for item in items['result']['post_list']:
                cmd = "select exists(select 1 from divar_items where item_token='{}');".format(item['token'])
                try:
                    cur.execute(cmd)
                except Exception as e:
                    print(str(e))
                    connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
                    cur = connection.cursor()
                    cur.execute(cmd)

                is__exist = cur.fetchall()
                connection.commit()
                connection.close()
                is__exist = is__exist[0]
                is__exist = is__exist[0]
                if is__exist:
                    continue
                else:
                    #req = requests.get('https://api.divar.ir/v1/posts/{}/contact/'.format(item['token']))
                    #contact = json.loads(req.text)
                    #item['phone'] = contact['widgets']['contact']['phone']
                    #item['email'] = contact['widgets']['contact']['email']
                    save_item_to_db(item)

def save_item_to_db(item):
    country = item['p']
    city = item['p2']
    district = item['p4']
    title = item['title'].replace("'", "")
    desc = item['desc'].replace("'", "")

    if city is None:
        city = -10

    if district is None:
        district = -10
    if country is None:
        country = -10
    if 'v09' not in item:
        item['v09']=-10


    string = """insert into divar_items values ('{0}','{1}','{2}',{3},{4},{5},{6},{7},{8},'{9}',{10},{11},{12},{13},{14},{15},{16});""".format(item['token'],
                               title,
                               desc,
                               item['c1'],
                               item['c2'],
                               item['c3'],
                               item['lm'],
                               item['v09'],
                               12,#item['phone'],
                               'empty yet',#item['email'],
                               country,
                               city,
                               district,
                               item['ic'],
                               item['ce'],
                               True,
                               item['hc']

                               )
    try:
        conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        cur = conn.cursor()
        cur.execute(string)
        conn.commit()
        print('the {} added to db'.format(item['token']))
        conn.close()
    except Exception as e:
        print('error ' + str(e))
        print('can not add {} to db the title is : {} and desc is : {}'.format(item['token'], item['title'],
                                                                               item['desc']))
        print('we are trying again ...')
        conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        cur = conn.cursor()
        cur.execute(string)
        conn.commit()
        print('the {} added to db'.format(item['token']))
        conn.close()


schedule.every(1).seconds.do(get_items_from_divar)

while True:
    schedule.run_pending()
    time.sleep(1)