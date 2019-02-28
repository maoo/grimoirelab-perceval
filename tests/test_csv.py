#!/usr/bin/env python3
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

import httpretty
import os
import pkg_resources
import unittest

pkg_resources.declare_namespace('perceval.backends')

from perceval.backend import BackendCommandArgumentParser
from perceval.backends.core.csv import CSV, CSVCommand, CSVClient
from base import TestCaseBackendArchive


CSV_URL = 'http://example.com/csv_entries'

requests_http = []


def read_file(filename, mode='r'):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename), mode) as f:
        content = f.read()
    return content


def configure_http_server():
    bodies_entries_job = read_file('data/csv/csv_entries.csv')

    http_requests = []

    def request_callback(method, uri, headers):
        last_request = httpretty.last_request()

        if uri.startswith(CSV_URL):
            body = bodies_entries_job
        else:
            body = ''

        requests_http.append(httpretty.last_request())

        http_requests.append(last_request)

        return (200, headers, body)

    httpretty.register_uri(httpretty.GET,
                           CSV_URL,
                           responses=[
                               httpretty.Response(body=request_callback)
                               for _ in range(2)
                           ])

    return http_requests


class TestCSVBackend(unittest.TestCase):
    """CSV backend tests"""

    def test_initialization(self):
        """Test whether attributes are initializated"""

        csv = CSV(
            CSV_URL,
            csv_header='street,city,zip,state,beds,baths,sq__ft,type,sale_date,price,latitude,longitude',
            separator=',',
            date_formats='%a %b %d %H:%M:%S EDT %Y',
            skip_header='true',
            id_columns='street,city,zip,state',
            date_column='sale_date')

        self.assertEqual(csv.url, CSV_URL)
        self.assertEqual(csv.csv_header, 'street,city,zip,state,beds,baths,sq__ft,type,sale_date,price,latitude,longitude')
        self.assertEqual(csv.separator, ',')
        self.assertEqual(csv.date_formats, '%a %b %d %H:%M:%S EDT %Y')
        self.assertEqual(csv.skip_header, True)
        self.assertEqual(csv.id_columns, 'street,city,zip,state')
        self.assertEqual(csv.date_column, 'sale_date')
        self.assertIsNone(csv.client)

    def test_has_archiving(self):
        """Test if it returns True when has_archiving is called"""

        self.assertEqual(CSV.has_archiving(), False)

    def test_has_resuming(self):
        """Test if it returns False when has_resuming is called"""

        self.assertEqual(CSV.has_resuming(), False)

    @httpretty.activate
    def test_fetch(self):
        """Test whether a list of entries is returned"""

        http_requests = configure_http_server()

        # Test fetch entries from feed
        csv = CSV(
            CSV_URL,
            csv_header='street,city,zip,state,beds,baths,sq__ft,type,sale_date,price,latitude,longitude',
            separator=',',
            date_formats='%a %b %d %H:%M:%S EDT %Y',
            skip_header='true',
            id_columns='street,city,zip,state',
            date_column='sale_date')

        entries = [entry for entry in csv.fetch()]
        self.assertEqual(len(entries), 3)
        self.assertEqual(len(http_requests), 1)

        # Test metadata
        expected = [('3526 HIGH ST',95838,2,836,59222,'38.631913','-121.434879'),
                    ('51 OMAHA CT',95823,3,1167,68212,'38.478902','-121.431028'),
                    ('2796 BRANCH ST',95815,2,796,68880,'38.618305','-121.443839')]

        for x in range(len(expected)):
            entry = entries[x]
            self.assertEqual(entry['city'], 'Sacramento')
            self.assertEqual(entry['state'], 'CA')
            self.assertEqual(entry['street'], expected[x][0])
            self.assertEqual(entry['zip'], expected[x][1])
            self.assertEqual(entry['beds'], expected[x][2])
            self.assertEqual(entry['baths'], '1')
            self.assertEqual(entry['sq__ft'], expected[x][3])
            self.assertEqual(entry['type'], 'Residential')
            self.assertEqual(entry['sale_date'], 'Wed May 21 00:00:00 EDT 2008')
            self.assertEqual(entry['price'], expected[x][4])
            self.assertEqual(entry['latitude'], expected[x][5])
            self.assertEqual(entry['longitude'], expected[x][6])

    @httpretty.activate
    def test_fetch_empty(self):
        """Test whether it works when no entries are fetched"""

        body = """"""
        httpretty.register_uri(httpretty.GET,
                               CSV_URL,
                               body=body, status=200)

        csv = CSV(
            CSV_URL,
            csv_header='street,city,zip,state,beds,baths,sq__ft,type,sale_date,price,latitude,longitude',
            separator=',',
            date_formats='%a %b %d %H:%M:%S EDT %Y',
            skip_header='true',
            id_columns='street,city,zip,state',
            date_column='sale_date')

        entries = [entry for entry in csv.fetch()]

        self.assertEqual(len(entries), 0)

    @httpretty.activate
    def test_parse(self):
        """Test whether the parser works """

        csv_file = read_file('data/csv/csv_entries.csv')
        json_feed = CSV.parse_feed(csv_file)
        entry = json_feed[0]

        """
        3526 HIGH ST,SACRAMENTO,95838,CA,2,1,836,Residential,Wed May 21 00:00:00 EDT 2008,59222,38.631913,-121.434879
        """
        self.assertEqual(entry['city'], 'Sacramento')
        self.assertEqual(entry['state'], 'CA')
        self.assertEqual(entry['street'], '3526 HIGH ST')
        self.assertEqual(entry['zip'], '95838')
        self.assertEqual(entry['beds'], '2')
        self.assertEqual(entry['baths'], '1')
        self.assertEqual(entry['sq__ft'], '836')
        self.assertEqual(entry['type'], 'Residential')
        self.assertEqual(entry['sale_date'], 'Wed May 21 00:00:00 EDT 2008')
        self.assertEqual(entry['price'], '59222')
        self.assertEqual(entry['latitude'], '38.631913')
        self.assertEqual(entry['longitude'], '-121.434879')

class TestCSVCommand(unittest.TestCase):
    """CSVCommand unit tests"""

    def test_backend_class(self):
        """Test if the backend class is CSV"""

        self.assertIs(CSVCommand.BACKEND, CSV)

    def test_setup_cmd_parser(self):
        """Test if it parser object is correctly initialized"""

        parser = CSVCommand.setup_cmd_parser()
        self.assertIsInstance(parser, BackendCommandArgumentParser)

        args = [
            'street,city,zip,state,beds,baths,sq__ft,type,sale_date,price,latitude,longitude',
            ',',
            '%a %b %d %H:%M:%S EDT %Y',
            'true',
            'street,city,zip,state',
            'sale_date',
            CSV_URL]

        parsed_args = parser.parse(*args)
        self.assertEqual(parsed_args.url, CSV_URL)
        self.assertEqual(parsed_args.csv_header, 'street,city,zip,state,beds,baths,sq__ft,type,sale_date,price,latitude,longitude')
        self.assertEqual(parsed_args.separator, ',')
        self.assertEqual(parsed_args.date_formats, '%a %b %d %H:%M:%S EDT %Y')
        self.assertEqual(parsed_args.skip_header, True)
        self.assertEqual(parsed_args.id_columns, 'street,city,zip,state')
        self.assertEqual(parsed_args.date_column, 'sale_date')


class TestCSVClient(unittest.TestCase):
    """CSV API client tests

    These tests not check the body of the response, only if the call
    was well formed and if a response was obtained. Due to this, take
    into account that the body returned on each request might not
    match with the parameters from the request.
    """
    @httpretty.activate
    def test_init(self):
        """Test initialization"""
        client = CSVClient(CSV_URL,',',True)

    @httpretty.activate
    def test_get_entries(self):
        """Test get_entries API call"""

        # Set up a mock HTTP server
        body = read_file('data/csv/csv_entries.csv')
        httpretty.register_uri(httpretty.GET,
                               CSV_URL,
                               body=body, status=200)

        client = CSVClient(CSV_URL,',',True)
        response = client.get_entries()

        self.assertEqual(len(response), 3)


if __name__ == "__main__":
    unittest.main(warnings='ignore')
