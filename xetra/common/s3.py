"""Connector and methods accessing S3"""
import os
import logging
import boto3
import pandas as pd
from io import StringIO, BytesIO
from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException

class S3BucketConnector():
    """
    Class for interacting with S3 Buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secrek_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(service_name="s3", endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)
    
    @profile
    def list_files_in_prefix(self, prefix: str):
        """
        Listing all files with a prefix on the S3 bucket

        :param prefix: Prefix on the S3 bucket that should be filtered with

        returns:
            files: list of all the file names containing the prefix in the key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files
    
    @profile
    def read_csv_to_df(self, key: str, decoding: str = "utf-8", sep: str = ","):
        """
        Read csv file from S3 into Data Frame

        :param: bucket -> the boto3 S3 bucket object
        :param: key -> the S3 csv file key (file name)
        :param: decoding -> the csv file encoding (default as "utf-8)
        :param: sep -> the csv file separator (default as ",")

        returns:
            df: the data frame created from the S3 csv file
        """
        self._logger.info("Reading file %s/%s/%s", self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get("Body").read().decode(decoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=sep)
        return df

    @profile
    def write_df_to_s3(self, df: pd.DataFrame, key: str, file_format: str):
        """
        Write data frame into S3 bucket as parquet

        :param df: Data frame to be converted to parquet and uploaded to S3
        :param key: Target key (file name) for the parquet file
        :param file_format: Format of the file to be stored in S3 (expected between CSV and PARQUET)
        """
        if df.empty:
            self._logger.info("The dataframe is empty! No file will be written!")
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info("The file format %s is not supported to be written to S3!", file_format)
        raise WrongFormatException
    
    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        :param out_buffer: StringIO | BytesIO that should be written
        :param key: target key of the saved file (file name)
        """

        self._logger.info("Writing file to %s/%s/%s", self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True