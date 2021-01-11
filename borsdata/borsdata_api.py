import requests
import pandas as pd
import time
from borsdata import constants as constants

# pandas options for string representation of data frames (print)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class BorsdataAPI:
    def __init__(self, _api_key, verbose=False):
        self._api_key = _api_key
        self._params = {'authKey': self._api_key, 'maxYearCount': 20, 'maxR12QCount': 40, 'maxCount': 20, 'date': None, 'version': 1}
        self._url_root = 'https://apiservice.borsdata.se/v1/'
        self._last_api_call = 0
        self._api_calls_per_second = 10
        # used for tracing (api-calls in terminal)
        self._verbose = verbose

    def _debug_trace(self, string):
        if self._verbose:
            print(string)

    def _call_api(self, url):
        """
        internal function for api-calls
        :param url: url to be added to _url_root
        :return: json-encoded content if any
        """
        current_time = time.time()
        time_delta = current_time - self._last_api_call
        # introduce sleep if time-delta is too big to prevent error 429.
        if time_delta < 1 / self._api_calls_per_second:
            time.sleep(1 / self._api_calls_per_second - time_delta)
        response = requests.get(self._url_root + url, self._params)
        self._debug_trace("BorsdataAPI >> calling API: " + self._url_root + url)
        self._last_api_call = time.time()
        # status_code == 200 SUCCESS!
        if response.status_code != 200:
            print(f"BorsdataAPI >> API-Error, status code: {response.status_code}")
            return response
        return response.json()

    """
    Instrument Meta
    """
    def get_branches(self):
        """
        returns branch data
        :return: pd.DataFrame
        """
        json_data = self._call_api('branches')
        return pd.json_normalize(json_data['branches'])

    def get_countries(self):
        """
        returns countries data
        :return: pd.DataFrame
        """
        json_data = self._call_api('countries')
        return pd.json_normalize(json_data['countries'])

    def get_markets(self):
        """
        returns market data
        :return: pd.DataFrame
        """
        json_data = self._call_api('markets')
        return pd.json_normalize(json_data['markets'])

    def get_sectors(self):
        """
        returns sector data
        :return: pd.DataFrame
        """
        json_data = self._call_api('sectors')
        return pd.json_normalize(json_data['sectors'])

    def get_translation_meta_data(self):
        """
        returns translation metadata
        :return: pd.DataFrame
        """
        url = 'translationmetadata'
        json_data = self._call_api(url)
        translation_data = pd.json_normalize(json_data['translationMetadatas'])
        return translation_data

    """
    Instruments
    """
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

    def get_instruments_updated(self):
        """
        returns all updated instruments
        :return: pd.DataFrame
        """
        url = 'instruments/updated'
        json_data = self._call_api(url)
        instruments = pd.json_normalize(json_data['instruments'])
        print(instruments.tail())
        return instruments

    """
    KPIs
    """
    def get_kpi_history(self, ins_id, kpi_id, report_type, price_type):
        url = f"instruments/{ins_id}/kpis/{kpi_id}/{report_type}/{price_type}/history"
        json_data = self._call_api(url)
        # creating dataframes from json-data
        kpi_history = pd.DataFrame.from_dict(json_data['values'], orient='columns')
        # the structure of the data-columns received are; 'y' year, 'p' period, 'v' value (kpi).
        # renaming the columns
        kpi_history.rename(columns={"y": "year", "p": "period", "v": "kpi_value"}, inplace=True)
        kpi_history.fillna(0, inplace=True)
        return kpi_history

    def get_kpi_summary(self, ins_id, report_type):
        """
        returns kpi summary for instrument
        :param ins_id: instrument id
        :param report_type: report type ['quarter', 'year', 'r12']
        :return: json object
        """
        url = f"instruments/{ins_id}/kpis/{report_type}/summary"
        json_data = self._call_api(url)
        return json_data

    def get_kpi_data_instrument(self, ins_id, kpi_id, calc_group, calc):
        """
        get screener data, for more information: https://github.com/Borsdata-Sweden/API/wiki/KPI-Screener
        :param ins_id: instrument id
        :param kpi_id: kpi id
        :param calc_group: ['1year', '3year', '5year', '7year', '10year', '15year']
        :param calc: ['high', 'latest', 'mean', 'low', 'sum', 'cagr']
        :return: json object
        """
        url = f"instruments/{ins_id}/kpis/{kpi_id}/{calc_group}/{calc}"
        json_data = self._call_api(url)
        print(json_data)
        return json_data

    def get_kpi_data_all_instruments(self, kpi_id, calc_group, calc):
        """
        get kpi data for all instruments
        :param kpi_id: kpi id
        :param calc_group: ['1year', '3year', '5year', '7year', '10year', '15year']
        :param calc: ['high', 'latest', 'mean', 'low', 'sum', 'cagr']
        :return: json object
        """
        url = f"instruments/kpis/{kpi_id}/{calc_group}/{calc}"
        json_data = self._call_api(url)
        print(json_data)
        return json_data

    def get_updated_kpis(self):
        """
        get latest calculation time for kpis
        :return: json object
        """
        url = f"instruments/kpis/updated"
        json_data = self._call_api(url)
        return json_data

    def get_kpi_metadata(self):
        """
        get kpi metadata
        :return: json object
        """
        url = f"instruments/kpis/metadata"
        json_data = self._call_api(url)
        return json_data

    """
    Reports
    """
    def get_instrument_report(self, ins_id, report_type):
        """
        get specific report data
        :param ins_id: instrument id
        :param report_type: ['quarter', 'year', 'r12']
        :return: pd.DataFrame of report data
        """
        url = f"instruments/{ins_id}/reports/{report_type}"
        json_data = self._call_api(url)
        reports = pd.DataFrame.from_dict(json_data['reports'], orient='columns')
        return reports

    def get_instrument_reports(self, ins_id):
        """
        get all report data
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
        # sort data ascending
        reports_year = reports_year.sort_values(['year', 'period'], ascending=True)
        reports_quarter = reports_quarter.sort_values(['year', 'period'], ascending=True)
        reports_r12 = reports_r12.sort_values(['year', 'period'], ascending=True)
        return reports_quarter, reports_year, reports_r12

    def get_reports_metadata(self):
        """
        get report metadata
        :return: pd.DataFrame with metadata
        """
        url = f"instruments/reports/metadata"
        json_data = self._call_api(url)
        metadata = pd.DataFrame.from_dict(json_data['reportMetadatas'], orient='columns')
        return metadata

    """
    Stockprices
    """
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
        # setting the 'date'-column in dataframe (table/spreadsheet) as index
        stock_prices.set_index('date', inplace=True)
        # sorting by the new index (date)
        stock_prices = stock_prices.sort_index()
        return stock_prices

    def get_instruments_stock_prices_last(self):
        """
        get last days' stock prices for all instruments
        :return: pd.DataFrame()
        """
        url = f'/instruments/stockprices/last'
        json_data = self._call_api(url)
        stock_prices = pd.json_normalize(json_data['stockPricesList'])
        stock_prices.rename(columns={'d': 'date', 'i': 'ins_id', 'c': 'close', 'h': 'high', 'l': 'low',
                                     'o': 'open', 'v': 'volume'}, inplace=True)
        stock_prices.fillna(0, inplace=True)
        return stock_prices

    def get_stock_prices_date(self, date):
        """
        get all instrument stock prices for passed date
        :param date: date in string format, e.g. '2000-01-01'
        :return:
        """
        url = f'/instruments/stockprices/date'
        self._params['date'] = date
        json_data = self._call_api(url)
        stock_prices = pd.json_normalize(json_data['stockPricesList'])
        stock_prices.rename(columns={'d': 'date', 'i': 'ins_id', 'c': 'close', 'h': 'high', 'l': 'low',
                                     'o': 'open', 'v': 'volume'}, inplace=True)
        return stock_prices

    """
    Stocksplits
    """
    def get_stock_splits(self):
        """
        get stock splits
        :return:
        """
        url = f'/instruments/stocksplits'
        json_data = self._call_api(url)
        stock_splits = pd.json_normalize(json_data['stockSplitList'])
        stock_splits['splitDate'] = pd.to_datetime(stock_splits['splitDate'] )
        return stock_splits
    
    """
    Helper Functions
    """
    def get_instrument_name(self, ins_id):
        """
        returns the instrument name (if found)
        :param ins_id:
        :return: name of instrument (string)
        """
        df = self.get_instruments()
        try:
            name = df[df['insId'] == ins_id]['name'].values[0]
        except Exception as e:
            print("BorsdataAPI >> get_instrument_name Error")
            print(e)
            name = "Name could not be found!"
        return name


if __name__ == "__main__":
    # Main, call functions here.
    api = BorsdataAPI(constants.API_KEY)
    #api.get_translation_meta_data()
    #api.get_instruments_updated()
    #api.get_kpi_summary(3, 'year')
    #api.get_kpi_data_instrument(3, 10, '1year', 'mean')
    #api.get_kpi_data_all_instruments(10, '1year', 'mean')
    #api.get_updated_kpis()
    #api.get_kpi_metadata()
    #api.get_instrument_report(3, 'year')
    #api.get_reports_metadata()
    #api.get_stock_prices_date('2020-09-25')
    #api.get_stock_splits()

