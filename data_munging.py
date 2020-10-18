import pandas
from pymongo.errors import PyMongoError
import numpy as np
import pandas as pd
import logging
import os
import json
import pymongo
import re
import datetime
import pprint
import unittest

from pymongo import MongoClient
from io import StringIO
from pymongo.errors import BulkWriteError


def get_logger(name=None):
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    fh = logging.FileHandler('application.log')
    frmt = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(frmt)
    log.addHandler(fh)
    return log


class JsonData:
    """This object allows to create and handle JSON data by reading a .json file.
    The format it can handle has the following form {index: {key1: value1},{key2:
    value2}}
    Args:
        path: The path to the JSON file
    """

    def __init__(self, path):
        self.log = logging.getLogger(self.__class__.__name__)
        self._df = pd.DataFrame()

        if ((not os.path.isfile(path)) or (type(path) != str)):
            raise TypeError('arg should be a valid path/file')

        try:
            file_name = os.path.basename(path)
            with open(path, 'r') as file:
                content = file.read()
            data = json.loads(content)
        except (OSError, FileNotFoundError):
            self.log.exception('Cannot read from ' +
                               os.getcwd() + os.sep + file_name)

        for record in data:
            tmp = pd.DataFrame(record).T
            self._df = self._df.append(tmp, ignore_index=True)

    def drop_columns(self, columns):
        """
        Method for deleting a list of columns from the dataframe
        associated to the object.
        Args:
            columns: The list of columns to be removed
        """

        if (type(columns) != list):
            raise TypeError("The input arg should be a list")

        try:
            for column in columns:
                self._df.drop([column], axis=1, inplace=True)
        except (ValueError):
            self.log.error('Column not present in the dataframe')

    def to_json_file(self, output_file):
        """
        Method for writing a cleanand flat output to a
        JSON file
        Example: [{name1: value1},{name2: value2}]
        Args:
            output_file: The file to write the result in json form
        """

        try:
            self._df.to_json(output_file, orient='records')
        except OSError as ose:
            self.log.error('Cannot write in this file')

    def to_database(self, db):
        """
        Method for writing a cleanand flat output to a
        Mongo database.
        Example: [{name1: value1},{name2: value2}]
        Args: The database name where to save the data
        """

        clean_data_json = StringIO()
        self._df.to_json(clean_data_json, orient='records')
        db.save_from_json(clean_data_json)

    def get_dataframe(self):
        """
        Method for returning an instance to the object.
        """
        return self._df


class DataBase:
    """
    Class that encaplulates the necessary information for connecting to a DB
    and perform the necessary typical operations, like insert and query.
    Args:
        config: A python dict data structure containing: host, port,
        database name and collection name
    """

    def __init__(self, config):
        self.log = logging.getLogger(self.__class__.__name__)
        try:
            client = MongoClient(config['host'], config['port'])
            self._db = client[config['db']]
            self._collection = self._db[config['collection']]
        except ConnectionFailure:
            self.log.error('Failure to connect to ' + config['host'] + config['port'])
        except PyMongoError:
            self.log.error('Error performing the operation')

    def save_from_json(self, streamObj):
        """
        Public method exposed by the object which allows to write a json
        stream into the database
        Args:
            streamObj: The json data structure as stream object (StringIO)
        """
        try:
            result = self._collection.count()
            if (result == 0):
                result = self._collection.insert_many(
                    json.loads(streamObj.getvalue()))
                self.log.info("DB populated with: %s documents",
                              len(result.inserted_ids))
            else:
                self.log.info(
                    'The collection already contains data. Nothing inserted')
        except (BulkWriteError, PyMongoError):
            self.log.error('Error writing to the DB')
        return result

    def get_connection(self):
        """
        Returns a reference to this object
        """
        return self._collection


class Coordinates:
    """
    Class object meant to contain information about US counties coordinates
    provided with a csv file. The main property of this class is a pandas
    dataframe object (uscnts).
    Args:
        data_file: A cvs file with US counties coordinates as Polygons.
    """

    def __init__(self, data_file):
        self.log = logging.getLogger(self.__class__.__name__)
        if ((data_file == '') or (not data_file)):
            raise TypeError('Constructor needs a valid CSV file')

        try:
            self._uscnts = pd.read_csv(data_file)
            self._tmp = None
        except (OSError, FileNotFoundError, NameError):
            raise ValueError('Cannot read from ' + os.getcwd() + os.sep + data_file)

        self._uscnts.drop(['State-County', 'state abbr', 'value', 'GEO_ID', 'GEO_ID2',
                           'FIPS formula', 'Has error', 'STATE num', 'COUNTY num'],
                          axis=1, inplace=True)
        self._uscnts.columns = ['city', 'state', 'geometry', 'gName']

    def arrange_coord(self):
        """
        Private method for munging the coordinates provided in the file. It sets each
        cell into a more suitable datatype
        """
        self._tmp['longitude'] = self._tmp[['geometry']].apply(
            lambda r: self.get_coordinates(r, 0), axis=1)
        self._tmp['latitude'] = self._tmp[['geometry']].apply(
            lambda r: self.get_coordinates(r, 1), axis=1)
        self._tmp.drop(['geometry'], axis=1, inplace=True)
        self._tmp['longitude'] = self._tmp['longitude'].apply(
            lambda r: pd.to_numeric(r, errors='coerce'))
        self._tmp['latitude'] = self._tmp['latitude'].apply(
            lambda r: pd.to_numeric(r, errors='coerce'))
        # self.log.info('Coordinates created as %s', pprint.pformat(self._tmp.head()))

    def combine_with(self, other_df, new_col, function):
        """
        Public method with allows to combine a given dataframe which contains
        fields such as city and state to the coordinates dataframe. The match is
        done on the fields mentioned above.
        Args:
            other_df: An external dataframe to merge the data with
            new_col: The name of the new column introduced by the aggregate function
            function: The name of the aggregate function to group by.
            return: A new dataframe integrating the geo coordinates and applying
            the aggregate function.
        """
        if ((type(other_df) != pandas.core.frame.DataFrame) or (type(new_col) != str) or (type(function) != str)):
            raise ValueError(
                'Function signature is (DataFrame, string, string')

        self._uscnts = pd.merge(other_df, self._uscnts, how='right',
                                sort=True, on=['city', 'state'])
        self._uscnts['state'].replace('', np.nan, inplace=True)
        self._tmp = self._uscnts.groupby(['city', 'state', 'geometry']).agg(
            function).reset_index(name=new_col)
        self.arrange_coord()

    def get_coordinates(self, current, pos):
        """
        Private method for re-arranging the coordinates in a suitable form.
        Originally cooordinates are provided as [{longitute,latitude}]. They are
        turned into the form longitude=[x,y,..] and latitude=[x,y,..]
        Args:
            current = current row in the dataframe
            pos = the indexed data
        """

        current = re.sub('<.*?>', '', current[0])
        coordinates = current.split(' ')
        result = list()

        for coord in coordinates:
            temp = coord.split(',')
            result.append(temp[pos])
        return result

    def get_coord_obj(self):
        """
        Returns a reference to the object created
        """
        return self._tmp


class TimeSerie:
    """
    Class which provides an abstraction of time series. Its main property is
    a pandas DataFrame object.
    Args:
        begin: The initial date
        end: The final date
    """

    def __init__(self, begin, end):
        self.log = logging.getLogger(self.__class__.__name__)
        if ((type(begin) != pd.Timestamp) or (type(end) != pd.Timestamp)):
            raise ValueError(
                'Begin and end should be valid datetime.date objects')

        if (begin > end):
            raise ValueError(
                'End of the interval cannot be greater than begin')

        self._begin = begin
        self._end = end
        self._ts = None

    def combine_with(self, other_df, new_col, function):
        """
        Public method with allows to combine a given dataframe which contains
        fields such as city and state to the TimeSerie dataframe. The match is
        done on the fields mentioned above.
        Args:
            other_df: An external dataframe to merge the data with
            new_col: The name of the new column introduced by the aggregate function
            function: The name of the aggregate function to group by.
            return: A new dataframe integrating the geo coordinates and applying
            the aggregate function.
        """
        if ((type(other_df) != pandas.core.frame.DataFrame) or (type(new_col) != str) or (type(function) != str)):
            raise ValueError(
                'Function signature is (DataFrame, string, string')

        self._ts = other_df
        self._ts['state'].replace('', np.nan, inplace=True)
        self._ts.sort_values(by=['datetime'], inplace=True)
        self._ts.loc[(self._ts['datetime'] > self._begin) & (self._ts['datetime'] < self._end)]
        self._ts['datetime'] = self._ts['datetime'].apply(lambda r: r.strftime('%Y-%m-%d'))
        self._ts = self._ts.groupby(['datetime', 'state']).agg('size').reset_index(name=new_col)
        self._ts.dropna(axis=0, how='any', inplace=True)

        self.log.info('TimeSerie built as: %s', pprint.pformat(self._ts.head()))

    def get_ts(self):
        """
        Returns a reference to the object created
        """
        return self._ts


# MAIN
def main():
    log = get_logger(__name__)
    log.info("BEGIN+")

    # Creates a JsonData object and prepared a DataFrame related to
    jd = JsonData('data/data.json')
    log.info('Creating DataFrame object %s from JSON file', jd)
    cols = ['Summary']
    jd.drop_columns(cols)
    df = jd.get_dataframe()

    # Data munging operations on the dataframe. Eventually a correct format is saved into a JSON to file
    df.columns = ['city', 'datetime', 'duration', 'posted', 'shape', 'state']
    df['posted'] = df['posted'].str.replace('/', '-')
    # jd.to_json_file('myCleanData.json')
    # log.info('DataFrame ready as {}'.format(pprint.pprint(df.head())))

    # Connects to an instance of MongoDB and populates it with data from file
    config = {
        'host': 'mongodb://172.17.0.3',
        'port': 27017,
        'db': 'assignment2',
        'collection': 'ufo_reports'
    }

    log.info('DB config: host: %s port: %s', config['host'], config['port'])
    mongo_db = DataBase(config)
    jd.to_database(mongo_db)

    # Performs a query against the DB in order to collect the necessary data
    cursor = mongo_db.get_connection()
    cursor = cursor.find({"state": {"$nin": ['AK', 'HI', 'PR', 'MP', 'VI', 'AS', 'GU']}},
                         {"city": 1, "state": 1, "datetime": 1})

    # Build a dataframe from the query results. Then remove _id field
    df = pd.DataFrame(list(cursor))
    df.drop(['_id'], axis=1, inplace=True)
    df['datetime'] = pd.to_datetime(df['datetime'])
    log.info('DataFrame retrieved from DB as %s', pprint.pformat(df.head()))

    # Creates an object Coordinates and merges it with our df
    coord = Coordinates('data/US_Counties.csv')
    coord.combine_with(df, 'reports', 'size')
    num_rep = coord.get_coord_obj()
    coord_output_file = 'geo_reports.json'
    num_rep.to_json(coord_output_file)
    log.info("Coordinates data as JSON in {}/{}".format(os.getcwd(), coord_output_file))

    # Creates an object TimeSerie and builds its related df
    begin = pd.Timestamp(datetime.date(2017, 1, 1))
    end = pd.Timestamp(datetime.date(2017, 12, 31))
    ts = TimeSerie(begin, end)
    ts.combine_with(df, 'reports', 'size')
    ts_output_file = 'ts_reports.json'
    ts.get_ts().to_json(ts_output_file)
    log.info("Time series data as JSON in {}/{}".format(os.getcwd(), ts_output_file))
    log.info('DataFrame retrieved from TimeSerie object as %s', pprint.pformat(ts.get_ts().head()))

    log.info("END-\n")


if __name__ == '__main__':
    main()
