import psycopg2
import requests
import json
import schedule

import time

hostname = 'localhost'
username = 'postgres'
password = 'admin'
database = 'stock-city-data-others'




def querySaveItems(conn):
    print('starting query_save_items')
    rows = getLastPostDate()  # 'each row present a category for example phone,laptop ...'

    for row in rows:
        last_post_date = int(row[0])
        while True:
            request_data = {"jsonrpc": "2.0", "id": 0, "method": "getPostList", "params": [
                [["place2", 0, ["1"]], ["cat3", 0, [int(row[1])]], ["cat2", 0, [24]], ["cat1", 0, [1]]],
                last_post_date]}

            r = requests.post('https://search.divar.ir/json/', json=request_data)
            items = json.loads(r.text)
            new_last_post_date = items['result']['last_post_date']
            if new_last_post_date == -1:
                print('The end of items')
                return
            last_post_date = new_last_post_date
            for item in items['result']['post_list']:
                cmd = "select exists(select 1 from divar_items where item_token='{}');".format(item['token'])
                cur = conn.cursor()
                cur.execute(cmd)
                

                is__exist = cur.fetchall()
                conn.close()

                is__exist = is__exist[0]
                is__exist = is__exist[0]
                if is__exist:
                    return
                else:
                    req = requests.get('https://api.divar.ir/v1/posts/{}/contact/'.format(item['token']))
                    contact = json.loads(req.text)
                    item['phone'] = contact['widgets']['contact']['phone']
                    item['email'] = contact['widgets']['contact']['email']
                    query_save_item(item)


def query_save_item(item):

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

    string = """
                DO
                $do$
                BEGIN
                IF Not Exists(select * from divar_items where item_token='{0}') THEN
                   insert into divar_items values ('{0}','{1}','{2}',{3},{4},{5},{6},{7},{8},'{9}',{10},{11},{12},{13},{14}, {15}, {16});
                ELSE
                    RAISE NOTICE 'the item "{0}" exist in db';
                END IF;
                END
                $do$
                """.format(item['token'],
                           title,
                           desc,
                           item['c1'],
                           item['c2'],
                           item['c3'],
                           item['lm'],
                           item['v09'],
                           item['phone'],
                           item['email'],
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
    except:
        print('can not add {} to db the title is : {} and desc is : {}'.format(item['token'], item['title'], item['desc']))
        print(string)
        print('waaaaay')


def getLastPostDate():
    conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = conn.cursor()
    cur.execute("""select * from last_lastpostdate""")
    rows = cur.fetchall()
    conn.close()
    return rows


conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
# 'querySaveItems(conn)
# 'conn.close()

schedule.every(10).seconds.do(querySaveItems, conn)

while True:
    schedule.run_pending()
    time.sleep(1)
