import pandas as pd
from datetime import timezone, datetime

import oandapyV20
import oandapyV20.endpoints.instruments as instruments

from dotenv import load_dotenv
load_dotenv()

import os
OANDA_CODE = os.environ.get("OANDA_CODE")


class download_forex_as_csv:
    # start_datetime is a datetime object. Time frame is from oanda ex: 'h1' is one hour time frame.
    def __init__(self, start_datetime, timeframe):
        self.start_datetime = start_datetime
        self.timeframe = timeframe

    def OANDA_Connection_Latest(self, pair):
        client = oandapyV20.API(access_token=OANDA_CODE)
        params = {"count": 1, "granularity": self.timeframe}
        r = instruments.InstrumentsCandles(instrument=pair, params=params)
        client.request(r)
        r.response['candles'][0]['mid']
        r.response['candles'][0]['time']
        r.response['candles'][0]['volume']
        dat = []
        for oo in r.response['candles']:
            dat.append([oo['time']])
            df = pd.DataFrame(dat)
            df.columns = ['Time']
            #Convert To Float
            df["Time"] = pd.to_datetime(df["Time"], unit='ns')
            latest_datetime = int((df['Time'].iloc[0])
                            .replace(tzinfo=timezone.utc).timestamp())
        return latest_datetime

    def OANDA_Connection(self, active_datetime, pair):
        client = oandapyV20.API(access_token=OANDA_CODE)
        params = {"from": active_datetime, "count": 5000, "granularity": self.timeframe}
        r = instruments.InstrumentsCandles(instrument=pair, params=params)
        client.request(r)
        r.response['candles'][0]['mid']
        r.response['candles'][0]['time']
        r.response['candles'][0]['volume']
        dat = []
        for oo in r.response['candles']:
            dat.append([oo['time'], oo['mid']['o'], oo['mid']['h'], oo['mid']['l'], oo['mid']['c'], oo['volume'], oo['complete']])
            df = pd.DataFrame(dat)
            df.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Complete']
            #Convert To Float
            df["Time"] = pd.to_datetime(df["Time"], unit='ns')
            df["Open"] = pd.to_numeric(df["Open"], downcast="float")
            df["High"] = pd.to_numeric(df["High"], downcast="float")
            df["Low"] = pd.to_numeric(df["Low"], downcast="float")
            df["Close"] = pd.to_numeric(df["Close"], downcast="float")
        return df

    def DownloadData(self, pair):
        start_unix = int(self.start_datetime
                .replace(tzinfo=timezone.utc).timestamp())
        latest_datetime = self.OANDA_Connection_Latest(pair)
        active_datetime = start_unix
        all_data = pd.DataFrame([])
        while active_datetime != latest_datetime:
            df = self.OANDA_Connection(active_datetime, pair)
            last_row = df.tail(1)
            active_datetime = int((last_row['Time'].iloc[0])
                            .replace(tzinfo=timezone.utc).timestamp())
            all_data = all_data.append(df)
            all_data = all_data.reset_index()
            all_data = all_data.drop(['index'], axis=1)
        return all_data

    def DownloadAllPairs(self):
        #Download All Currency Pairs
        pairs = ['EUR_USD']
        for pair in pairs:
            pair_data = self.DownloadData(pair)
            pair_data.to_csv('Data_' + pair + '.csv', index=False)
        return

if __name__ == "__main__":
    print('Do not run this as an independent file. Import the class and then run it.')