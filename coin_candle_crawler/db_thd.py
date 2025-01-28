# -*- coding: utf-8 -*-
import threading
import pymysql
import pymysql.cursors
import queue
from coin_candle_crawler.evt_type import EvtType


class DbThd(threading.Thread):
    def __init__(
            self,
            db_queue: queue.Queue,
            db_host: str,
            db_port: int,
            db_user: str,
            db_pw: str,
            db_schema: str,
            db_tbl_nm: str
    ) -> None:
        threading.Thread.__init__(self)

        self.daemon = True

        self._db_queue = db_queue
        self._db_tbl_nm = db_tbl_nm

        self._db_con = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pw,
            database=db_schema,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def create_db_tbl(self) -> None:
        query = f'''
            CREATE TABLE IF NOT EXISTS `{self._db_tbl_nm}` (
                `idx_key` bigint(20) NOT NULL AUTO_INCREMENT,
                `symbol` varchar(16) NOT NULL,
                `date` date NOT NULL,
                `open` double NOT NULL COMMENT 'open price',
                `high` double NOT NULL COMMENT 'hight price',
                `low` double NOT NULL COMMENT 'low price',
                `close` double NOT NULL COMMENT 'close price',
                `volume` double NOT NULL COMMENT 'trade volume',
                `trd_val` double DEFAULT NULL COMMENT 'trade value',
                PRIMARY KEY (`idx_key`),
                UNIQUE KEY `u_key` (`date`,`symbol`) USING BTREE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
        '''

        with self._db_con.cursor() as cursor:
            cursor.execute(query)

    def _insert_daily_candle_data(self, datas: list) -> None:
        if 0 == len(datas):
            return

        query = f'INSERT IGNORE INTO `{self._db_tbl_nm}` (`symbol`, `date`, `open`, `high`, `low`, `close`, `volume`, `trd_val`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'

        with self._db_con.cursor() as cursor:
            cursor.executemany(query, datas)

        self._db_con.commit()

    def run(self) -> None:
        try:
            while True:
                msg: tuple = self._db_queue.get()
                ety = msg[0]

                if EvtType.INSERT_TO_DB == ety:
                    self._insert_daily_candle_data(msg[1])

                elif EvtType.RCV_FINISHED == ety:
                    break

        except Exception as exp:
            pass
