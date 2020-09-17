import requests
import pandas as pd
import time


class BorsdataAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.params = {'authKey': self.api_key, 'maxYearCount': 20, 'maxR12QCount': 40, 'maxCount': 20}
        self.url_root = 'https://apiservice.borsdata.se/v1/'
        self._last_api_call = 0
        self._api_calls_per_second = 10

    def _call_api(self, url):
        """
        internal function for api-calls
        :param url: url to be added to url_root
        :return: json-encoded content if any
        """
        current_time = time.time()
        time_delta = current_time - self._last_api_call
        # introduce sleep if time-delta is too big to prevent error 429.
        if time_delta < 1 / self._api_calls_per_second:
            time.sleep(1 / self._api_calls_per_second - time_delta)
        response = requests.get(self.url_root + url, self.params)
        self._last_api_call = time.time()
        # status_code == 200 SUCCESS!
        if response.status_code != 200:
            print(f"API-Error, status code: {response.status_code}")
            return response
        return response.json()

    def get_instruments(self):
        """
        returns instrument data
        :return: pd.DataFrame
        """
        url = 'instruments'
        json_data = self._call_api(url)
        instruments = pd.json_normalize(json_data['instruments'])
        instruments['listingDate'] = pd.to_datetime(instruments['listingDate'])
        return instruments

    def get_markets(self):
        """
        returns market data
        :return: pd.DataFrame
        """
        json_data = self._call_api('markets')
        return pd.json_normalize(json_data['markets'])

    def get_countries(self):
        """
        returns countries data
        :return: pd.DataFrame
        """
        json_data = self._call_api('countries')
        return pd.json_normalize(json_data['countries'])

    def get_sectors(self):
        """
        returns sector data
        :return: pd.DataFrame
        """
        json_data = self._call_api('sectors')
        return pd.json_normalize(json_data['sectors'])

    def get_branches(self):
        """
        returns branch data
        :return: pd.DataFrame
        """
        json_data = self._call_api('branches')
        return pd.json_normalize(json_data['branches'])

    def get_instrument_stock_prices(self, ins_id):
        """
        get stock prices for ins_id
        :param ins_id:
        :return: pd.DataFrame()
        """
        url = f'instruments/{ins_id}/stockprices'
        json_data = self._call_api(url)
        stock_prices = pd.json_normalize(json_data['stockPricesList'])
        # re-naming the columns
        stock_prices.rename(columns={'d': 'date', 'c': 'close', 'h': 'high', 'l': 'low',
                                     'o': 'open', 'v': 'volume'}, inplace=True)
        # converting the date to a datetime-object
        stock_prices.date = pd.to_datetime(stock_prices.date)
        stock_prices.fillna(0, inplace=True)
        return stock_prices

    def get_instrument_stock_prices_last(self):
        """
        get last days' stock prices for all istruments
        :return: pd.DataFrame()
        """
        url = f'/instruments/stockprices/last'
        json_data = self._call_api(url)
        stock_prices = pd.json_normalize(json_data['stockPricesList'])
        stock_prices.rename(columns={'d': 'date', 'i': 'ins_id', 'c': 'close', 'h': 'high', 'l': 'low',
                                     'o': 'open', 'v': 'volume'}, inplace=True)
        stock_prices.fillna(0, inplace=True)
        return stock_prices

    def get_instrument_report_data(self, ins_id):
        """
        get instrument(ins_id) report data
        :param ins_id:
        :return: [pd.DataFrame(), pd.DataFrame(), pd.DataFrame()]
        """
        # constructing url for api-call, adding ins_id
        url = f'instruments/{ins_id}/reports'
        json_data = self._call_api(url)
        # creating dataframes from json-data
        reports_year = pd.DataFrame.from_dict(json_data['reportsYear'], orient='columns')
        reports_quarter = pd.DataFrame.from_dict(json_data['reportsQuarter'], orient='columns')
        reports_r12 = pd.DataFrame.from_dict(json_data['reportsR12'], orient='columns')
        # making the columns lower-case in all dataframes
        reports_year.columns = [x.lower() for x in reports_year.columns]
        reports_quarter.columns = [x.lower() for x in reports_quarter.columns]
        reports_r12.columns = [x.lower() for x in reports_r12.columns]
        # replacing all nans with a 0
        reports_year.fillna(0, inplace=True)
        reports_quarter.fillna(0, inplace=True)
        reports_r12.fillna(0, inplace=True)
        return reports_quarter, reports_year, reports_r12





