"""
Methods for processing the meta file
"""
import collections
import datetime as dt
import pandas as pd
from xetra.common.custom_exceptions import WrongMetaFileException
from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat

class MetaProcess():
    """
    Class for working with the meta file
    """

    @staticmethod
    def update_meta_file(bucket: S3BucketConnector, meta_key: str, extract_date_list: list):
        """
        Updates the meta file with the dates processed from Xetra and today's date as processed date

        :param bucket: S3 bucket where the meta_file.csv is stored
        :param meta_key: the key for the file (its file name: meta_file.csv)
        :param extract_date_list: the list of dates to be extracted from Xetra
        """
        df_new = pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COL.value,
                                       MetaProcessFormat.META_PROCESS_COL.value])
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = dt.datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            # If meta file exists -> union DatFrame of old and new meta data is created
            df_old = bucket.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
        except bucket.session.client("s3").exceptions.NoSuchKey:
            # No meta file exists -> only the new data is used
            df_all = df_new
        # Writing to S3
        bucket.write_df_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True

    @staticmethod
    def return_date_list(bucket: S3BucketConnector, arg_date: str, meta_key: str):
        """
        Creating a list of dates from the arg_date parameter and the dates already processed in the meta_file.csv

        :param: bucket -> S3BucketConnector for the bucket with the meta file
        :param: arg_date -> the earlist date Xetra data should be processed
        :param: meta_key -> key of the meta file on the S3 bucket (file name)

        returns:
            min_date: first date that should be processed
            return_date_list: list of all dates from min_date till today
        """
        start = dt.datetime.strptime(arg_date, MetaProcessFormat.META_DATE_FORMAT.value).date() - pd.Timedelta(days=1)
        today = dt.datetime.today().date()
        try:
            # If meta file exists, create return_date_list using the content of it
            # Reading meta_file.csv
            df_meta = bucket.read_csv_to_df(meta_key)
            # Creating list of dates from arg_date till today
            dates = [start + dt.timedelta(days=x) for x in range(0, (today-start).days + 1)]
            # Creating set of all dates in meta_file
            src_dates = set(pd.to_datetime(df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]).dt.date)
            dates_missing = set(dates[1:]) - src_dates
            if dates_missing:
                # Determines the earlist date that should be processed
                min_date = min(set(dates[1:]) - src_dates) - dt.timedelta(days=1)
                # Creating a list of dates from min_date till today
                return_dates = [date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) for date in dates if date >= min_date]
                return_min_date = (min_date + dt.timedelta(days=1)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
            else:
                # Setting value for the earliest date and the list of dates
                return_dates = []
                return_min_date = dt.datetime(2200, 1, 1).date().strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        except bucket.session.client("s3").exceptions.NoSuchKey:
            # No meta file found -> creating a date list from arg_date - 1 till today
            return_dates = [(start + dt.timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)\
                for x in range(0, (today-start).days + 1)]
            return_min_date = arg_date
        return return_min_date, return_dates