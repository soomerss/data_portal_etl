
import pandas as pd
import requests
import json
import time
from datetime import date, timedelta

def get_weekday_dates(input_year,input_month,input_day):
    total_days = []
    day = timedelta(days=1)
    start_day = date(input_year,input_month,input_day)
    # weekday: 월요일 - 0, 일요일 - 6 vs cron : 일요일 - 0 , 토요일 - 6
    while start_day <= date.today():
        if start_day.weekday() <= 4 :
            add_day = start_day.strftime('%Y%m%d')
            total_days.append(add_day)
        start_day += day
    return total_days

def merge_stock_dataframes(total_days):
    if len(total_days) < 1:
        return None
    elif len(total_days) == 1:
        return get_daily_stock_data(total_days[0])
    else:
        df = get_daily_stock_data(total_days[0])
        for i in total_days[1:]:
            df = pd.concat([df,get_daily_stock_data(i)],ignore_index=True)
        return df

def get_daily_stock_data(date):
    with open('key.json','r') as f:
        json_data = json.load(f)
        key = json_data['key']
    url = f"https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getPreemptiveRightSecuritiesPriceInfo?serviceKey={key}&basDt={date}&resultType=json&numOfRows=10000&pageNo=1"
    req = requests.get(url)
    json_file = json.loads(req.text)
    parse_file = json_file['response']['body']['items']['item']
    df = pd.json_normalize(parse_file)
    return df

if __name__=="__main__":
    year, month,day = map(int,input("연 월 일을 공백으로 구분하여 시작날짜를 지정해주세요.").split())
    start = time.time()
    total_days = get_weekday_dates(year,month,day)
    final_data = merge_stock_dataframes(total_days)
    final_data.to_csv(f"preemtive_right_securities-from-{str(year)+str(month).zfill(2)+str(day).zfill(2)}-to-{date.today().strftime('%Y%m%d')}",index=False)
    end = time.time() - start
    print(end)
