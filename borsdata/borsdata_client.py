# importing the borsdata_api
from borsdata.borsdata_api import *
# pandas is a data-analysis library for python (dataframes)
import pandas as pd
# matplotlib for visual-presentations (plots)
import matplotlib.pylab as plt
# datetime for date- and time-stuff
import datetime as dt
# api_data for system constants
from borsdata import constants as constants


def instruments_with_meta_data():
    borsdata_api = BorsdataAPI(constants.API_KEY)

    countries = borsdata_api.get_countries()
    branches = borsdata_api.get_branches()
    sectors = borsdata_api.get_sectors()
    markets = borsdata_api.get_markets()

    instruments = borsdata_api.get_instruments()
    # instrument type dict for conversion (https://github.com/Borsdata-Sweden/API/wiki/Instruments)
    instrument_type_dict = {0: 'Aktie', 1: 'Pref', 2: 'Index', 3: 'Stocks2', 4: 'SectorIndex', 5: 'BranschIndex'}
    # creating an empty dataframe
    instrument_dataframe = pd.DataFrame()
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
        instrument_dataframe = instrument_dataframe.append({'name': name, 'ins_id': ins_id, 'ticker': ticker, 'isin': isin, 'instrument_type': instrument_type,
                                                            'market': market, 'country': country, 'sector': sector, 'branch': branch}, ignore_index=True)
    # showing the last elements (tail)
    print(instrument_dataframe.tail())
    # to csv
    instrument_dataframe.to_csv(constants.EXPORT_PATH + 'instrument_with_meta_data.csv')
    # creating excel-document
    excel_writer = pd.ExcelWriter(constants.EXPORT_PATH + 'instrument_with_meta_data.xlsx')
    # adding one sheet
    instrument_dataframe.to_excel(excel_writer, 'instruments_with_meta_data')
    # saving the document
    excel_writer.save()


def plot_stock_prices(ins_id):
    # creating api-object
    borsdata_api = BorsdataAPI(constants.API_KEY)
    # using api-object to get stock prices from API
    stock_prices = borsdata_api.get_instrument_stock_prices(ins_id)
    # setting the 'date'-column in received dataframe (think table/spreadsheet) as index
    stock_prices.set_index('date', inplace=True)  # set date to be the index
    # calculating/creating a new column named 'sma50' in the table and
    # assigning the rolling mean to it
    stock_prices['sma50'] = stock_prices['close'].rolling(window=50).mean()
    # filtering out data before 2015 for plot
    filtered_data = stock_prices[stock_prices.index > dt.datetime(2015, 1, 1)]
    # plotting 'close' (with 'date' as index)
    plt.plot(filtered_data['close'], color='blue', label='close')
    # plotting 'sma50' (with 'date' as index)
    plt.plot(filtered_data['sma50'], color='black', label='sma50')
    # show legend
    plt.legend()
    # show plot
    plt.show()
    # console print of the last five elements of the dataframe
    print(stock_prices.tail())



if __name__ == "__main__":
    instruments_with_meta_data()
    # plot_stock_prices(3)  # ABB



