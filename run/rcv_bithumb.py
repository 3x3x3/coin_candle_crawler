# -*- coding: utf-8 -*-
import os
import json
import queue
from coin_candle_crawler.db_thd import DbThd
from coin_candle_crawler.bithumb_rcv_thd import BithumbRcvThd


CONFIG_FILE_PATH = 'config.json'
CONFIG_KEY_DB = 'db'
CONFIG_KEY_BITHUMB = 'bithumb'


def main():
    # Config
    basedir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(basedir)

    f = open(CONFIG_FILE_PATH)
    config = json.load(f)

    db_config = config[CONFIG_KEY_DB]
    bithumb_config = config[CONFIG_KEY_BITHUMB]

    db_host = db_config['host']
    db_port = int(db_config['port'])
    db_user = db_config['user']
    db_pw = db_config['password']
    db_schema = db_config['schema']

    db_tbl_nm = bithumb_config['db_tbl_nm']
    base_assets = bithumb_config['base_assets']

    f.close()
    #

    db_queue = queue.Queue()

    db_thd = DbThd(
        db_queue,
        db_host,
        db_port,
        db_user,
        db_pw,
        db_schema,
        db_tbl_nm
    )
    db_thd.create_db_tbl()
    db_thd.start()

    rcv_thd = BithumbRcvThd(
        db_queue,
        base_assets
    )
    rcv_thd.start()

    rcv_thd.join()
    db_thd.join()

if '__main__' == __name__:
    print('START !!')
    main()
    print('DONE !!')