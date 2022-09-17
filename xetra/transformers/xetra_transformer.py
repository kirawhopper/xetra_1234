"""Xetra ETL Component"""
import logging
from typing import NamedTuple
from xetra.common.s3 import S3BucketConnector

class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data

    src_first_extract_date: determins the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    rsc_col_time: column name for isin in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_volume: column name for traded volume in source
    """

    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    rsc_col_time: str
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
    tgt_key_date: str
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
        self.extract_date = 
        self.extract_date_list = 
        self.meta_update_list = 
    
    def extract(self):
        pass

    def transform_report1(self):
        pass

    def load(self):
        pass

    def etl_report1(self):
        pass