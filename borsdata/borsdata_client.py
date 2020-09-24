# importing the borsdata_api
from borsdata.borsdata_api import *
# pandas is a data-analysis library for python (data frames)
import pandas as pd
# matplotlib for visual-presentations (plots)
import matplotlib.pylab as plt
# datetime for date- and time-stuff
import datetime as dt
# api_data for system constants
from borsdata import constants as constants
import os

# pandas options for string representation of data frames (print)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class BorsdataClient:
    def __init__(self):
        self._borsdata_api = BorsdataAPI(constants.API_KEY)
        self._instruments_with_meta_data = pd.DataFrame()

    def instruments_with_meta_data(self):
        """
        creating a csv and xlsx of the APIs instrument-data (including meta-data)
        and saves it to path defined in constants (default ../file_exports/)
        :return: pd.DataFrame of instrument-data with meta-data
        """
        if len(self._instruments_with_meta_data) > 0:
            return self._instruments_with_meta_data
        else:
            self._borsdata_api = BorsdataAPI(constants.API_KEY)
            # fetching data from api
            countries = self._borsdata_api.get_countries()
            branches = self._borsdata_api.get_branches()
            sectors = self._borsdata_api.get_sectors()
            markets = self._borsdata_api.get_markets()
            instruments = self._borsdata_api.get_instruments()
            # instrument type dict for conversion (https://github.com/Borsdata-Sweden/API/wiki/Instruments)
            instrument_type_dict = {0: 'Aktie', 1: 'Pref', 2: 'Index', 3: 'Stocks2', 4: 'SectorIndex', 5: 'BranschIndex'}
            # creating an empty dataframe
            instrument_df = pd.DataFrame()
            # loop through the whole dataframe (table) i.e. row-wise-iteration.
            for index, instrument in instruments.iterrows():
                name = instrument['name']
                ins_id = instrument['insId']
                ticker = instrument['ticker']
                isin = instrument['isin']
                # locating meta-data in various ways
                # dictionary-lookup
                instrument_type = instrument_type_dict[instrument['instrument']]
                # .loc locates the rows where the criteria (inside the brackets, []) is fulfilled
                # located rows (should be only one) get the column 'name' and return its value-array
                # take the first value in that array ([0], should be only one value)
                market = markets.loc[markets['id'] == instrument['marketId']]['name'].values[0]
                country = countries.loc[countries['id'] == instrument['countryId']]['name'].values[0]
                sector = 'N/A'
                branch = 'N/A'
                # index-typed instruments does not have a sector or branch
                if market.lower() != 'index':
                    sector = sectors.loc[sectors['id'] == instrument['sectorId']]['name'].values[0]
                    branch = branches.loc[branches['id'] == instrument['branchId']]['name'].values[0]
                # appending current data to dataframe, i.e. adding a row to the table.
                instrument_df = instrument_df.append({'name': name, 'ins_id': ins_id, 'ticker': ticker, 'isin': isin, 'instrument_type': instrument_type,
                                                                    'market': market, 'country': country, 'sector': sector, 'branch': branch}, ignore_index=True)
            # showing the last elements (tail)
            #print(instrument_df.tail())
            # create directory if it do not exist
            if not os.path.exists(constants.EXPORT_PATH):
                os.makedirs(constants.EXPORT_PATH)
            # to csv
            instrument_df.to_csv(constants.EXPORT_PATH + 'instrument_with_meta_data.csv')
            # creating excel-document
            excel_writer = pd.ExcelWriter(constants.EXPORT_PATH + 'instrument_with_meta_data.xlsx')
            # adding one sheet
            instrument_df.to_excel(excel_writer, 'instruments_with_meta_data')
            # saving the document
            excel_writer.save()
            self._instruments_with_meta_data = instrument_df
            return instrument_df

    def plot_stock_prices(self, ins_id):
        """
        Plotting a matplotlib chart for ins_id
        :param ins_id: instrument id to plot
        :return:
        """
        # creating api-object
        # using api-object to get stock prices from API
        stock_prices = self._borsdata_api.get_instrument_stock_prices(ins_id)
        # calculating/creating a new column named 'sma50' in the table and
        # assigning the 50 day rolling mean to it
        stock_prices['sma50'] = stock_prices['close'].rolling(window=50).mean()
        # filtering out data after 2015 for plot
        filtered_data = stock_prices[stock_prices.index > dt.datetime(2015, 1, 1)]
        # plotting 'close' (with 'date' as index)
        plt.plot(filtered_data['close'], color='blue', label='close')
        # plotting 'sma50' (with 'date' as index)
        plt.plot(filtered_data['sma50'], color='black', label='sma50')
        # show legend
        plt.legend()
        # show plot
        plt.show()

    def top_performers(self, market, country, number_of_stocks=5, percent_change=1):
        """
        function that prints top performers for given parameters in the terminal
        :param market: which market to search in e.g. 'Large Cap'
        :param country: which country to search in e.g. 'Sverige'
        :param number_of_stocks: number of stocks to print, default 5 (top5)
        :param percent_change: number of days for percent change calculation
        :return: pd.DataFrame
        """
        # creating api-object
        # using defined function above to retrieve dataframe of all instruments
        instruments = self.instruments_with_meta_data()
        # filtering out the instruments with correct market and country
        filtered_instruments = instruments.loc[(instruments['market'] == market) & (instruments['country'] == country)]
        # creating new, empty dataframe
        stock_prices = pd.DataFrame()
        # looping through all rows in filtered dataframe
        for index, instrument in filtered_instruments.iterrows():
            # fetching the stock prices for the current instrument
            instrument_stock_price = self._borsdata_api.get_instrument_stock_prices(int(instrument['ins_id']))
            # calculating the current instruments percent change
            instrument_stock_price['pct_change'] = instrument_stock_price['close'].pct_change(percent_change)
            # getting the last row of the dataframe, i.e. the last days values
            last_row = instrument_stock_price.iloc[[-1]]
            # appending the instruments name and last days percent change to new dataframe
            stock_prices = stock_prices.append({'stock': instrument['name'], 'pct_change': round(last_row['pct_change'].values[0]*100, 2)}, ignore_index=True)
        # printing the top sorted by pct_change-column
        print(stock_prices.sort_values('pct_change', ascending=False).head(number_of_stocks))
        return stock_prices

    def history_kpi(self, kpi, market, country):
        """
        gathers and concatenates historical kpi-values for provided kpi, market and country
        :param kpi: kpi id see https://github.com/Borsdata-Sweden/API/wiki/KPI-History
        :param market: market to gather kpi-values from
        :param country: country to gather kpi-values from
        :return: pd.DataFrame of historical kpi-values
        """
        # creating api-object
        # using defined function above to retrieve data frame of all instruments
        instruments = self.instruments_with_meta_data()
        # filtering out the instruments with correct market and country
        filtered_instruments = instruments.loc[(instruments['market'] == market) & (instruments['country'] == country)]
        # creating empty array (to hold data frames)
        frames = []
        # looping through all rows in filtered data frame
        for index, instrument in filtered_instruments.iterrows():
            # fetching the stock prices for the current instrument
            instrument_kpi_history = self._borsdata_api.get_kpi_history(int(instrument['ins_id']), kpi, 'year', 'mean')
            instrument_kpi_history['name'] = instrument['name']
            # check to see if response holds any data.
            if len(instrument_kpi_history) > 0:
                # appending data frame to array
                frames.append(instrument_kpi_history.copy())
        # creating concatenated data frame with concat
        symbols_df = pd.concat(frames)
        # the data frame has the columns ['year', 'period', 'kpi_value', 'name']
        # get the last year ranked from highest to lowest, show top 5
        print(symbols_df[symbols_df['year'] == 2019].sort_values('kpi_value', ascending=False).head(5))
        return symbols_df

    def get_latest_pe(self, ins_id):
        """
        Prints the PE-ratio of the provided instrument id
        :param ins_id: ins_id which PE-ratio will be calculated for
        :return:
        """
        # creating api-object
        # fetching all instrument data
        reports_quarter, reports_year, reports_r12 = self._borsdata_api.get_instrument_report_data(3)
        # getting the last reported eps-value
        last_eps = reports_r12['earnings_per_share'].values[-1]
        # getting the stock prices
        stock_prices = self._borsdata_api.get_instrument_stock_prices(ins_id)
        # getting the last close
        last_close = stock_prices['close'].values[-1]
        # getting the last date
        last_date = stock_prices.index.values[-1]
        # using help-function to retrieve the name of the instrument
        instrument_name = self._borsdata_api.get_instrument_name(3)
        # printing the name and calculated PE-ratio with the corresponding date. (array slicing, [:10])
        print(f"PE for {instrument_name} is {round(last_close/last_eps, 1)} with data from {str(last_date)[:10]}")


if __name__ == "__main__":
    # Main, call functions here.
    # creating BorsdataClient-instance
    borsdata_client = BorsdataClient()
    # calling some methods
    borsdata_client.get_latest_pe(87)
    borsdata_client.instruments_with_meta_data()
    borsdata_client.plot_stock_prices(3)  # ABB
    borsdata_client.history_kpi(2, 'Large Cap', 'Sverige')  # 2 == Price/Earnings (PE)
    borsdata_client.top_performers('Large Cap', 'Sverige', 10, 5)  # showing top10 performers based on 5 day return (1 week) for Large Cap Sverige.



