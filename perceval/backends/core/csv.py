# -*- coding: utf-8 -*-
#
# Copyright (C) 2018-2019 Fintech Open Source Foundation
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335, USA.
#
# Authors:
#     Maurizio Pillitu <maoo@finos.org>
#

import logging

import csv
import datetime
import base64
import urllib3
http = urllib3.PoolManager()
import urllib.request
import tempfile

from grimoirelab_toolkit.datetime import str_to_datetime

from ...backend import (Backend,
                        BackendCommand,
                        BackendCommandArgumentParser)

CATEGORY_ENTRY = "csv-entry"

logger = logging.getLogger(__name__)

class CSV(Backend):
    """CSV backend for Perceval.

    This class retrieves the entries from a CSV file.
    To initialize this class the CSV file path must be provided.
    The `file_path` will be set as the origin of the data.

    :param file_path: File Path
    :param csv_header: Columns included in the CSV file
    :param separator: CV separator char
    :param date_formats: Comma-separated list of date formas to use to extract the timestamp of a CSV entry
    :param skip_header: 'true' if the first CSV row contains the column header
    :param id_columns: the columns that compose the ID hash
    :param date_column: the column containing the date for metadata_updated_on
    """
    version = '0.0.1'

    CATEGORIES = [CATEGORY_ENTRY]

    separator = ','

    def __init__(self, file_path, csv_header, separator, date_formats, skip_header, id_columns, date_column, tag=None, archive=None):
        origin = file_path

        super().__init__(origin)
        self.file_path = file_path
        self.csv_header = csv_header
        self.separator = separator
        self.date_formats = date_formats
        self.skip_header = (skip_header == 'true')
        self.id_columns = id_columns
        self.date_column = date_column
        self.client = None

    def fetch(self, category=CATEGORY_ENTRY):
        """Fetch the rows from the CSV.

        :returns: a generator of entries
        """
        kwargs = {}
        items = super().fetch(category, **kwargs)

        return items

    @staticmethod
    def dateToTs(date,formats):
        if not formats:
            print("skipping entry due to wrong date format: '"+date+"'")
        else:
            head, *tail = formats
            try:
                date_time_obj = datetime.datetime.strptime(date, head)
                return date_time_obj.timestamp()
            except:
                return CSV.dateToTs(date, tail)

    def fetch_items(self, category, **kwargs):
        """Fetch the entries

        :param kwargs: backend arguments

        :returns: a generator of items
        """
        logger.info("Looking for csv rows at feed '%s'", self.file_path)

        nentries = 0  # number of entries

        entries = self.client.get_entries()

        for item in entries:
            ret = {}

            # Need to pass which columns are IDs to metadata_id static function
            ret['_id_columns'] = self.id_columns
            for i, column in enumerate(self.csv_header.split(',')):
                value = item[i]
                if isinstance(item[i], str):
                    value = item[i].strip()

                # If it's the date column, parse value and add it as 'timestamp' in the item
                if (column == self.date_column):
                    timestamp = CSV.dateToTs(value, self.date_formats.split(','))
                    if timestamp:
                        ret['timestamp'] = timestamp
                ret[column.strip()] = value
            if 'timestamp' in ret:
                yield ret
            nentries += 1

        logger.info("Total number of entries: %i", nentries)

    @classmethod
    def has_archiving(cls):
        """Returns whether it supports archiving entries on the fetch process.

        :returns: this backend does not support entries archive
        """
        return False

    @classmethod
    def has_resuming(cls):
        """Returns whether it supports to resume the fetch process.

        :returns: this backend does not supports entries resuming
        """
        return False

    @staticmethod
    def metadata_id(item):
        """Extracts the identifier from a CSV row, using the hash of the concatenation of values, that can be configured using the id_columns configuration parameter."""
        string = ""
        for column in item['_id_columns'].split(','):
            string = string + item[column] + "-"
        return str(base64.b64encode(string.encode()))

    @staticmethod
    def metadata_category(item):
        """Extracts the category from a CSV item.

        This backend only generates one type of item which is
        'csv-entry'.
        """
        return CATEGORY_ENTRY

    @staticmethod
    def metadata_updated_on(item):
        """Extracts the update time from a CSV row.

        The timestamp is extracted from 'published' field.
        This date is a datetime string that needs to be converted to
        a UNIX timestamp float value.

        :param item: item generated by the backend

        :returns: a UNIX timestamp
        """
        return item['timestamp']

    def _init_client(self, from_archive=False):
        """Init client"""
        return CSVClient(self.file_path, self.separator, self.skip_header)

class CSVClient():
    """CSV API client.

    :param file_path: CSV file path

    :raises TODO - raise a Runtime exception if file path is not correct
    """

    def __init__(self, uri, separator, skip_header, archive=None, from_archive=False):
        self.separator = separator
        self.skip_header = skip_header

        if uri.startswith('file://'):
            self.file_path = uri.split('file://',1)[1]
        else:
            self.file_path = tempfile.mkdtemp() + "/perceval-csv-backend-" + str(datetime.datetime.now())+".csv"
            urllib.request.urlretrieve(uri, self.file_path)

    def parse_entries(self,csv_content):
        reader = csv.reader(csv_content, delimiter=self.separator)
        ret = []
        for i, row in enumerate(reader):
            if (self.skip_header) and (i == 0):
                print("skipping header")
            else:
                ret.append(row)
        # Useful for debugging
        # if i == 100:
        #     break
        return ret

    def get_entries(self):
        """ Retrieve all entries from a CVS file"""
        self.session = None
        with open(self.file_path, newline='') as csv_content:
            return self.parse_entries(csv_content)

class CSVCommand(BackendCommand):
    """Class to run CSV backend from the command line."""

    BACKEND = CSV

    @staticmethod
    def setup_cmd_parser():
        """Returns the CSV argument parser."""

        parser = BackendCommandArgumentParser()

        # Required arguments
        parser.parser.add_argument('file_path',
                                   help="Path to the CSV file")

        parser.parser.add_argument('csv_header',
                                   help="Comma-separated list of file headers")

        parser.parser.add_argument('id_columns',
                                   help="Specifies which columns should compose the ID hash")

        parser.parser.add_argument('date_column',
                                   help="Specifies which column contains the date of the entry")

        # Optional arguments
        group = parser.parser.add_argument_group('CSV file format options')
        group.add_argument('--separator',
                            nargs='+', type=str, dest='separator', default=",",
                            help="CSV separator, defaults to ','")

        group.add_argument('--date_formats',
                            nargs='+', type=str, dest='date_formats', 
                            default="%a %b %d %H:%M:%S EDT %Y, %Y-%m-%d",
                            help="Comma-separated list of supported date formats")

        group.add_argument('--skip_header',
                            default=True, nargs='+', type=bool, dest='skip_header',
                            help="Skips first line if true; defaults to true")

        return parser