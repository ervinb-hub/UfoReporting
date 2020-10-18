import unittest
import datetime
import pandas as pd
import numpy as np
import pymongo
from io import StringIO
from pandas.testing import assert_frame_equal
from pymongo.errors import BulkWriteError
from data_munging import JsonData, DataBase, Coordinates, TimeSerie
import os


class TestSuite(unittest.TestCase):
    def setUp(self):
        self.jd = JsonData('data/data.json')
        self.test_case = '{"City":{"0":"St. Louis"},"Date \\/ Time":{"0":"11\\/9\\/17 04:30"},\
                        "Duration":{"0":"1 hour"},"Posted":{"0":"11\\/9\\/17"},\
                        "Shape":{"0":"Unknown"},"State":{"0":"MO"}}'

    def test_JsonData(self):
        # Test of object creation from MY JSON data
        tmp = self.jd.get_dataframe()
        tmp.drop(['Summary'], axis=1, inplace=True)
        df = tmp.head(1)
        test_df = pd.read_json(self.test_case, orient='index').T
        assert_frame_equal(df, test_df)

        # Test of saving a valid and clean json to file
        self.jd.to_json_file('unittest.json')
        self.assertTrue(os.path.isfile('unittest.json'))
        tmp = pd.read_json('unittest.json', orient='records')
        df = tmp.head(1)
        assert_frame_equal(df, test_df)

    def test_DataBase(self):
        # Test for successfully connecting to DB
        test_config = {
            'host': 'mongodb://172.17.0.3',
            'port': 27017,
            'db': 'assignment2',
                  'collection': 'unittest'
        }
        test_db = DataBase(test_config)
        test_conn = test_db.get_connection()
        self.assertTrue(isinstance(test_conn, pymongo.collection.Collection),
                        "The instance created is not a Collection")

        self.assertEqual(test_conn.full_name, 'assignment2.unittest')

        # Test for reading data drom DB
        buff = StringIO()
        tmp = pd.DataFrame(np.arange(4).reshape(2, 2), columns=list('AB'))
        tmp.to_json(buff, orient='records')
        test_db.save_from_json(buff)
        cursor = test_conn.find()
        self.assertTrue(isinstance(cursor, pymongo.cursor.Cursor),
                        "The data returned is not a document")

        # Test that data returned from query is identical
        doc = pd.DataFrame(list(cursor))
        doc.drop(['_id'], axis=1, inplace=True)
        assert_frame_equal(tmp, doc)

    def test_Coordinates(self):
        with self.assertRaises(ValueError):
            coord = Coordinates('fake_file.txt')

        with self.assertRaises(TypeError):
            coord = Coordinates()

        test_df = pd.read_json(self.test_case, orient='index').T
        test_df.columns = test_df.columns.str.lower()
        coord = Coordinates('data/US_Counties.csv')

        with self.assertRaises(ValueError):
            coord.combine_with('rubbish', 1, 'aa')

        coord.combine_with(test_df.copy(), 'reports', 'size')
        coord = coord.get_coord_obj()
        self.assertEqual(coord['reports'][0].item(), 1)

    def test_TimeSerie(self):
        begin = pd.Timestamp(datetime.date(2017, 9, 1))
        end = pd.Timestamp(datetime.date(2017, 12, 31))

        with self.assertRaises(ValueError):
            ts = TimeSerie(1, 'aa')

        with self.assertRaises(ValueError):
            ts = TimeSerie(end, begin)

        ts = TimeSerie(begin, end)
        test_df = pd.read_json(self.test_case, orient='index').T
        test_df.columns = ['city', 'datetime',
                           'duration', 'posted', 'shape', 'state']
        test_df['datetime'] = pd.to_datetime(test_df['datetime'])
        test_df['posted'] = test_df['posted'].str.replace('/', '-')

        self.assertTrue(isinstance(test_df['datetime'][0], datetime.date),
                        "The datetime column is not a valid datetime.date format")
        ts.combine_with(test_df.copy(), 'reports', 'size')
        self.assertEqual(test_df['datetime'][0].to_pydatetime().date(), datetime.date(2017, 11, 9),
                         "Failed to handle an empty input argument")


# Run the tests
if __name__ == '__main__':
    unittest.main(argv=[""], exit=False, verbosity=2)
