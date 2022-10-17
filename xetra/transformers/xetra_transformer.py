"""Xetra ETL Component"""
import logging
import pandas as pd
import datetime as dt
from typing import NamedTuple
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess

class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data

    src_first_extract_date: determins the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for isin in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_volume: column name for traded volume in source
    """

    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_volume: str

class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data

    tgt_col_isin: column name for isin in target
    tgt_col_date: column name for date in target
    tgt_col_opening_price: column name for opening price in target
    tgt_col_closing_price: column name for closing price in target
    tgt_col_min_price: column name for minimum price in target
    tgt_col_max_price: column name for maximum price in target
    tgt_col_dly_traded_volume: column name for daily traded volume in target
    tgt_col_chg_prev_closing: column name for change to previous day's closing price in target
    tgt_key: basic key of target file
    tgt_key_date: date format of target file key
    tgt_format: file format of the target file
    """

    tgt_col_isin: str
    tgt_col_date: str
    tgt_col_opening_price: str
    tgt_col_closing_price: str
    tgt_col_min_price: str
    tgt_col_max_price: str
    tgt_col_dly_traded_volume: str
    tgt_col_chg_prev_closing: str
    tgt_key: str
    tgt_key_date_format: str
    tgt_format: str

class XetraETL():
    """
    Reads the Xetra data, transforms and writes the transformed data to target
    """

    def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_tgt: S3BucketConnector, meta_key: str,
                 src_args: XetraSourceConfig, tgt_args: XetraTargetConfig):
        """
        Constructor for XetraTransformer

        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_tgt: connection to target S3 bucket
        :param meta_key: used as self.meta_key -> key of meta_file
        :param src_args: NamedTuple class with source configuration data
        :param tgt_args: NamedTuple class with target configuration data
        """

        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_tgt = s3_bucket_tgt
        self.meta_key = meta_key
        self.src_args = src_args
        self.tgt_args = tgt_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(self.s3_bucket_tgt, 
                                                                                 self.src_args.src_first_extract_date,
                                                                                 self.meta_key)
        self.meta_update_list = [date for date in self.extract_date_list if date >= self.extract_date]
    
    @profile
    def extract(self):
        """
        Read the source and concatenate all respective files to one data frame

        returns:
            df -> the DataFrame with all the data for the respective selected dates
        """
        self._logger.info("Extracting Xetra source files started...")
        files = [key for date in self.extract_date_list\
                 for key in self.s3_bucket_src.list_files_in_prefix(date)]
        if not files:
            df = pd.DataFrame()
        else:
            df = pd.concat([self.s3_bucket_src.read_csv_to_df(obj) for obj in files], ignore_index=True)
        self._logger.info("Extracting Xetra source files completed.")
        return df

    @profile
    def transform_report1(self, df: pd.DataFrame):
        """
        Applies the necessaty transformations to create report 1

        :param: df -> the data frame from the exract layer to run through the report 1 transformations
        """
        if df.empty:
            self._logger.info("The dataframe is empty, no transformations will be executed.")
            return df
        self._logger.info("Transformations for Xetra report 1 started...")
        # Filtering necessary columns
        df = df.loc[:, self.src_args.src_columns]
        # Removing rows with missing values
        df.dropna(inplace=True)
        # Calculating openin price per ISIN and day
        df[self.tgt_args.tgt_col_opening_price] = \
            df.sort_values(by=[self.src_args.src_col_time]).\
               groupby([self.src_args.src_col_isin, self.src_args.src_col_date])[self.src_args.src_col_start_price].\
               transform("first")
        df[self.tgt_args.tgt_col_closing_price] = \
            df.sort_values(by=[self.src_args.src_col_time]).\
               groupby([self.src_args.src_col_isin, self.src_args.src_col_date])[self.src_args.src_col_start_price].\
               transform("last")
        # Renaming columns
        df.rename(columns={
            self.src_args.src_col_min_price: self.tgt_args.tgt_col_min_price,
            self.src_args.src_col_max_price: self.tgt_args.tgt_col_max_price,
            self.src_args.src_col_traded_volume: self.tgt_args.tgt_col_dly_traded_volume
        }, inplace=True)
        # Aggregating per ISIN and day -> opening price, closing price, minimum price, maximum price and traded volume
        df = df.groupby([self.src_args.src_col_isin, self.src_args.src_col_date], as_index=False).\
                agg({
                    self.tgt_args.tgt_col_opening_price: "min",
                    self.tgt_args.tgt_col_closing_price: "min",
                    self.tgt_args.tgt_col_min_price: "min",
                    self.tgt_args.tgt_col_max_price: "max",
                    self.tgt_args.tgt_col_dly_traded_volume: "sum"
                })
        # Change of current day's closing price compared to previous day's closing price in %
        df[self.tgt_args.tgt_col_chg_prev_closing] = df.sort_values(by=[self.src_args.src_col_date]).\
                                                        groupby([self.src_args.src_col_isin])[self.tgt_args.tgt_col_opening_price].\
                                                        shift(1)
        df[self.tgt_args.tgt_col_chg_prev_closing] = (df[self.tgt_args.tgt_col_opening_price] 
                                                      -
                                                      df[self.tgt_args.tgt_col_chg_prev_closing]) \
                                                        / df[self.tgt_args.tgt_col_chg_prev_closing] * 100
        # Rounding values to 2 decimal points
        df = df.round(decimals=2)
        # Removing the day before extract date
        df = df[df.Date >= self.extract_date].reset_index(drop=True)
        self._logger.info("Transformations for Xetra report 1 completed...")
        return df

    @profile
    def load(self, df: pd.DataFrame):
        """
        Saves transformed dataframe to target S3

        :param: df -> dataframe resulted from the transform layer
        """
        # Creating the tarket key
        target_key = (
            f"{self.tgt_args.tgt_key}"
            f"{dt.datetime.today().strftime(self.tgt_args.tgt_key_date_format)}."
            f"{self.tgt_args.tgt_format}"
        )
        # Writing to target S3
        self.s3_bucket_tgt.write_df_to_s3(df, target_key, self.tgt_args.tgt_format)
        self._logger.info("Xetra data successfuly written to target S3")
        # Updating meta file
        MetaProcess.update_meta_file(self.s3_bucket_tgt, self.meta_key, self.meta_update_list)
        self._logger.info("Xetra meta file successfuly updated")
        return True

    @profile
    def etl_report1(self):
        """
        Extract, Transform and Load to create report 1
        """
        # Extraction
        df = self.extract()
        # Transformation
        df = self.transform_report1(df)
        # Load
        self.load(df)
        return True