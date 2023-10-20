#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import logging
import concurrent.futures
import os.path
import sys
import pandas as pd

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)
import instock.lib.run_template as runt
import instock.core.tablestructure as tbs
import instock.lib.database as mdb
import instock.core.stockfetch as stf

__author__ = 'myh '
__date__ = '2023/3/10 '


# 每日股票龙虎榜
def save_nph_stock_top_data(date, before=True):
    if before:
        return

    try:
        data = stf.fetch_stock_top_data(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_TOP['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_TOP['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_stock_top_data处理异常：{e}")
    stock_spot_buy(date)


# 每日股票大宗交易
def save_stock_blocktrade_data(date):
    try:
        data = stf.fetch_stock_blocktrade_data(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_BLOCKTRADE['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_stock_blocktrade_data处理异常：{e}")


# 每日股票资金流向
def save_nph_stock_fundflow_data(date, before=True):
    if before:
        return

    try:
        times = tuple(range(4))
        results = run_check(times)
        if results is None:
            return

        for t in times:
            if t == 0:
                data = results.get(t)
            else:
                r = results.get(t)
                if r is not None:
                    r.drop(columns=['name', 'new_price'], inplace=True)
                    data = pd.merge(data, r, on=['code'], how='left')

        if data is None or len(data.index) == 0:
            return

        data.insert(0, 'date', date.strftime("%Y-%m-%d"))

        table_name = tbs.TABLE_CN_STOCK_FUND_FLOW['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_FUND_FLOW['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_nph_stock_fundflow_data处理异常：{e}")


def run_check(times):
    data = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(times)) as executor:
            future_to_data = {executor.submit(stf.fetch_stocks_fund_flow, k): k for k in times}
            for future in concurrent.futures.as_completed(future_to_data):
                _time = future_to_data[future]
                try:
                    _data_ = future.result()
                    if _data_ is not None:
                        data[_time] = _data_
                except Exception as e:
                    logging.error(f"basic_data_other_daily_job.run_check处理异常：代码{e}")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.run_check处理异常：{e}")
    if not data:
        return None
    else:
        return data


# 每日股票分红配送
def save_nph_stock_bonus(date, before=True):
    if before:
        return

    try:
        data = stf.fetch_stocks_bonus(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_BONUS['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BONUS['columns'])
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.save_nph_stock_bonus处理异常：{e}")


# 基本面选股
def stock_spot_buy(date):
    try:
        _table_name = tbs.TABLE_CN_STOCK_SPOT['name']
        if not mdb.checkTableIsExist(_table_name):
            return

        sql = f'''SELECT * FROM `{_table_name}` WHERE `date` = '{date}' and 
                `pe9` > 0 and `pe9` <= 20 and `pbnewmrq` <= 10 and `roe_weight` >= 15'''
        data = pd.read_sql(sql=sql, con=mdb.engine())
        data = data.drop_duplicates(subset="code", keep="last")
        if len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_SPOT_BUY['name']
        # 删除老数据。
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT_BUY['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
    except Exception as e:
        logging.error(f"basic_data_other_daily_job.stock_spot_buy处理异常：{e}")


def main():
    runt.run_with_args(save_nph_stock_top_data)
    runt.run_with_args(save_stock_blocktrade_data)
    runt.run_with_args(save_nph_stock_bonus)
    runt.run_with_args(save_nph_stock_fundflow_data)


# main函数入口
if __name__ == '__main__':
    main()
