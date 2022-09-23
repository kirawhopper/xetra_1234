"""Test MetaProcess methods"""
import os
import unittest
import boto3
import pandas as pd
import datetime as dt
from moto import mock_s3
from io import StringIO
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException

class TestMetaProcessMethods(unittest.TestCase):
    """
    Testing the MetaProcess class
    """

    def setUp(self):
        """
        Setting up the environment
        """
        # mocking S3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining the class arguments
        self.s3_access_key = "AWS_ACCESS_KEY_ID"
        self.s3_secret_key = "AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url = "https://s3.sa-east-1.amazonaws.com"
        self.s3_bucket_name = "test-bucket"
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = "KEY1"
        os.environ[self.s3_secret_key] = "KEY2"
        # Creating a bucket on the mocked s3
        self.s3 = boto3.resource(service_name="s3", endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                "LocationConstraint": "sa-east-1"
                              })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self.s3_bucket_connector = S3BucketConnector(self.s3_access_key,
                                                     self.s3_secret_key,
                                                     self.s3_endpoint_url,
                                                     self.s3_bucket_name)
        self.dates = [(dt.datetime.today().date() - dt.timedelta(days=day))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]
    
    def tearDown(self):
        """
        Executing after unittests
        """
        # mocking S3 connection stop
        self.mock_s3.stop()
    
    def test_update_meta_file_no_meta_file(self):
        """
        Tests the update_meta_file method when there is no meta file
        """
        # Expected results
        date_list_exp = ["2021-04-16", "2021-04-17"]
        proc_date_list_exp = [dt.datetime.today().date()] * 2
        # Test init
        meta_key = "meta.csv"
        # Method execution
        MetaProcess.update_meta_file(self.s3_bucket_connector, meta_key, date_list_exp)
        # Read meta file
        data = self.s3_bucket.Object(key=meta_key).get().get("Body").read().decode("utf-8")
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(
            pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]).dt.date
        )
        # Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                "Objects": [
                    {
                        "Key": meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_empty_date_list(self):
        """
        Tests the update_meta_file method when the argumento extract_date_list is empty
        """
        # Expected results
        return_exp = True
        log_exp = "The dataframe is empty! No file will be written!"
        # Test init
        date_list = []
        meta_key = "meta.csv"
        # Method execution
        with self.assertLogs() as logm:
            result = MetaProcess.update_meta_file(self.s3_bucket_connector, meta_key, date_list)
            # Log test after method execution
            self.assertIn(log_exp, logm.output[1])
        # Test after method execution
        self.assertEqual(return_exp, result)

    def test_update_meta_file_ok(self):
        """
        Tests the update_meta_file method when there is already a meta_file on S3
        """
        # Expected results
        date_list_old = ["2021-04-12", "2021-04-13"]
        date_list_new = ["2021-04-16", "2021-04-17"]
        date_list_exp = date_list_old + date_list_new
        proc_date_list_exp = [dt.datetime.today().date()] * 4
        # Test init
        meta_key = "meta.csv"
        meta_content = (
            f"{MetaProcessFormat.META_SOURCE_DATE_COL.value},"
            f"{MetaProcessFormat.META_PROCESS_COL.value}\n"
            f"{date_list_old[0]},"
            f"{dt.datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n"
            f"{date_list_old[1]},"
            f"{dt.datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        # Method execution
        MetaProcess.update_meta_file(self.s3_bucket_connector, meta_key, date_list_new)
        # Read meta file
        data = self.s3_bucket.Object(key=meta_key).get().get("Body").read().decode("utf-8")
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[
            MetaProcessFormat.META_SOURCE_DATE_COL.value
        ])
        proc_date_list_result = list(pd.to_datetime(df_meta_result[
            MetaProcessFormat.META_PROCESS_COL.value
        ]).dt.date)
        # Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                "Objects": [
                    {
                        "Key": meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_wrong_meta_file(self):
        """
        Test the update_meta_file method when there is a wring meta file
        """
        # Expected results
        date_list_old = ["2021-04-12", "2021-04-13"]
        date_list_new = ["2021-04-16", "2021-04-17"]
        # Test init
        meta_key = "meta.csv"
        meta_content = (
            f"wrong_column,"
            f"{MetaProcessFormat.META_PROCESS_COL.value}\n"
            f"{date_list_old[0]},"
            f"{dt.datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n"
            f"{date_list_old[1]},"
            f"{dt.datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        # Method execution
        with self.assertRaises(WrongMetaFileException):
            MetaProcess.update_meta_file(self.s3_bucket_connector, meta_key, date_list_new)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                "Objects": [
                    {
                        "Key": meta_key
                    }
                ]
            }
        )

    def test_return_date_list_no_meta_file(self):
        """
        Tests the return_date_list method when there is no meta file
        """
        # Expected results
        date_list_exp = [
            (dt.datetime.today().date() - dt.timedelta(days=day))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(4)
            ]
        min_date_exp = (dt.datetime.today().date() - dt.timedelta(days=2))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        # Test init
        first_date = min_date_exp
        meta_key = "meta.csv"
        # Method execution
        min_date_return, date_list_return = MetaProcess.return_date_list(self.s3_bucket_connector, first_date, meta_key)
        # Test after method execution
        self.assertEqual(set(date_list_exp), set(date_list_return))
        self.assertEqual(min_date_exp, min_date_return)

    def test_return_date_list_meta_file_ok(self):
        """
        Tests the return_date_list method when there is a meta file
        """
        # Expected results
        min_date_exp = [
            (dt.datetime.today().date() - dt.timedelta(days=1))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value),
            (dt.datetime.today().date() - dt.timedelta(days=2))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value),
            (dt.datetime.today().date() - dt.timedelta(days=7))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        ]
        date_list_exp = [
            [(dt.datetime.today().date() - dt.timedelta(days=day))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(3)],
            [(dt.datetime.today().date() - dt.timedelta(days=day))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(4)],
            [(dt.datetime.today().date() - dt.timedelta(days=day))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(9)]
        ]
        # Test init
        meta_key = "meta.csv"
        meta_content = (
            f"{MetaProcessFormat.META_SOURCE_DATE_COL.value},"
            f"{MetaProcessFormat.META_PROCESS_COL.value}\n"
            f"{self.dates[3]},{self.dates[0]}\n"
            f"{self.dates[4]},{self.dates[0]}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date_list = [
            self.dates[1],
            self.dates[4],
            self.dates[7]
        ]
        # Method execution
        for count, first_date in enumerate(first_date_list):
            min_date_return, date_list_return = MetaProcess.return_date_list(self.s3_bucket_connector, first_date, meta_key)
        # Test after method execution
        self.assertEqual(set(date_list_exp[count]), set(date_list_return))
        self.assertEqual(min_date_exp[count], min_date_return)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                "Objects": [
                    {
                        "Key": meta_key
                    }
                ]
            }
        )

    def test_return_date_list_wrong_meta_file(self):
        """
        Tests the return_date_list metho when there is a wrong meta file
        """
        # Test init
        meta_key = "meta.csv"
        meta_content = (
            f"wrong_column,"
            f"{MetaProcessFormat.META_PROCESS_COL.value}\n"
            f"{self.dates[3]},{self.dates[0]}\n"
            f"{self.dates[4]},{self.dates[0]}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[1]
        # Method execution
        with self.assertRaises(KeyError):
            MetaProcess.return_date_list(self.s3_bucket_connector, first_date, meta_key)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                "Objects": [
                    {
                        "Key": meta_key
                    }
                ]
            }
        )

    def test_return_date_list_empty_date_list(self):
        """
        Tests the return_date_list method when there are no dates to be returned
        """
        # Expected results
        min_date_exp = "2200-01-01"
        date_list_exp = []
        # Test init
        meta_key = "meta.csv"
        meta_content = (
            f"{MetaProcessFormat.META_SOURCE_DATE_COL.value},"
            f"{MetaProcessFormat.META_PROCESS_COL.value}\n"
            f"{self.dates[0]},{self.dates[0]}\n"
            f"{self.dates[1]},{self.dates[0]}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[0]
        # Method execution
        min_date_return, date_list_return = MetaProcess.return_date_list(self.s3_bucket_connector, first_date, meta_key)
        # Test after method execution
        self.assertEqual(date_list_exp, date_list_return)
        self.assertEqual(min_date_exp, min_date_return)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                "Objects": [
                    {
                        "Key": meta_key
                    }
                ]
            }
        )

if __name__ == "__main__":
    unittest.main()