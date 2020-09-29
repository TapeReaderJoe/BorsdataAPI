from borsdata.borsdata_api import *
import pandas as pd
import os
import datetime as dt
from borsdata import constants as constants


class ExcelExporter:
    """
    A small example class that uses the BorsdataAPI to fetch and concatenate
    instrument data into excel-files.
    """
    def __init__(self):
        self._api = BorsdataAPI(constants.API_KEY)
        self._instruments = self._api.get_instruments()
        self._markets = self._api.get_markets()
        self._countries = self._api.get_countries()

    def create_excel_files(self):
        # looping through all instruments
        for index, instrument in self._instruments.iterrows():
            stock_prices = self._api.get_instrument_stock_prices(instrument['insId'])
            reports_quarter, reports_year, reports_r12 = self._api.get_instrument_reports(instrument['insId'])
            # map the instruments market/country id (integer) to its string representation in the market/country-table
            market = self._markets.loc[self._markets['id'] == instrument['marketId']]['name'].values[0].lower().replace(' ', '_')
            country = self._countries.loc[self._countries['id'] == instrument['countryId']]['name'].values[0].lower().replace(' ', '_')
            export_path = constants.EXPORT_PATH + f"{dt.datetime.now().date()}/{country}/{market}/"
            instrument_name = instrument['name'].lower().replace(' ', '_')
            # creating necessary folders if they do not exist
            if not os.path.exists(export_path):
                os.makedirs(export_path)
            # creating the writer with export location
            excel_writer = pd.ExcelWriter(export_path + instrument_name + ".xlsx")
            stock_prices.to_excel(excel_writer, 'stock_prices')
            reports_quarter.to_excel(excel_writer, 'reports_quarter')
            reports_year.to_excel(excel_writer, 'reports_year')
            reports_r12.to_excel(excel_writer, 'reports_r12')
            excel_writer.save()
            print(f'Excel exported: {export_path + instrument_name + ".xlsx"}')


if __name__ == "__main__":
    excel = ExcelExporter()
    excel.create_excel_files()
