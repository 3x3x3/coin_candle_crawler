# -*- coding: utf-8 -*-
import threading
import queue
import requests
import requests.adapters
import datetime
import time
from coin_candle_crawler.evt_type import EvtType


class BithumbRcvThd(threading.Thread):
    BASE_URL = 'https://api.bithumb.com'

    def __init__(
            self,
            db_queue: queue.Queue,
            base_assets: list
    ) -> None:
        threading.Thread.__init__(self)

        self.daemon = True

        self._db_queue = db_queue
        self._base_assets = base_assets

    @classmethod
    def _rcv_func(cls, db_queue, base_asset) -> None:
        symbol = f'KRW-{base_asset}'.upper()
        yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
        last_rcv_dt = datetime.datetime.strftime(yesterday, '%Y-%m-%dT%H:%M:%S')

        session: requests.Session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
        session.mount('https://', adapter)

        while True:
            req_params = {
                'market': symbol,
                'to': last_rcv_dt,
                'count': 200
            }

            resp = session.get(
                url=f'{BithumbRcvThd.BASE_URL}/v1/candles/days',
                params=req_params
            )
            resp.encoding = 'utf-8'
            rcv_datas = resp.json()

            if 0 == len(rcv_datas):
                break

            insert_datas = []

            for rcv in rcv_datas:
                last_rcv_dt = rcv.get('candle_date_time_kst', '')
                date = datetime.datetime.strptime(last_rcv_dt, '%Y-%m-%dT%H:%M:%S').date()

                insert_datas.append((
                    symbol,
                    date.strftime("%Y-%m-%d"),
                    rcv.get('opening_price', 0),
                    rcv.get('high_price', 0),
                    rcv.get('low_price', 0),
                    rcv.get('trade_price', 0),
                    rcv.get('candle_acc_trade_volume', 0),
                    rcv.get('candle_acc_trade_price', 0),
                ))

            db_queue.put((EvtType.INSERT_TO_DB, insert_datas))

            time.sleep(0.1)

    def run(self) -> None:
        rcv_thds = []

        for base_asset in self._base_assets:
            thd = threading.Thread(target=self._rcv_func, args=(self._db_queue, base_asset,))
            rcv_thds.append(thd)
            thd.start()

        for thd in rcv_thds:
            thd.join()

        self._db_queue.put((EvtType.RCV_FINISHED, None))
