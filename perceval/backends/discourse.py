# -*- coding: utf-8 -*-
#
# Copyright (C) 2016 Bitergia
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
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#    J. Manrique López de la Fuente <jsmanrique@bitergia.com>
#    Santiago Dueñas <sduenas@bitergia.com>
#    Alvaro del Castillo San Felix <acs@bitergia.com>
#

import json
import logging
import os.path

import requests

from ..backend import Backend, BackendCommand, metadata
from ..cache import Cache
from ..errors import CacheError
from ..utils import (DEFAULT_DATETIME,
                     datetime_to_utc,
                     str_to_datetime,
                     urljoin)


MAX_topics = 100  # Maximum number of posts per query

logger = logging.getLogger(__name__)


class Discourse(Backend):
    """Discourse backend for Perceval.

    This class retrieves the posts stored in a Discourse board.
    To initialize this class the URL must be provided.

    :param url: Discourse URL
    :param token: Discourse API access token
    :param max_topics: maximum number of topics to fetch on a single request
    :param cache: cache object to store raw data
    :param origin: identifier of the repository; when `None` or an
        empty string are given, it will be set to `url` value
    """
    version = '0.1.0'

    def __init__(self, url, token=None, max_topics=None,
                 cache=None, origin=None):
        origin = origin if origin else url

        super().__init__(origin, cache=cache)
        self.url = url
        self.max_topics = max_topics
        self.client = DiscourseClient(url, token, max_topics)

    @metadata
    def fetch(self, from_date=DEFAULT_DATETIME):
        """Fetch the posts from the Discurse board.

        The method retrieves, from a Discourse board the
        posts updated since the given date.

        :param from_date: obtain topics updated since this date

        :returns: a generator of posts
        """
        if not from_date:
            from_date = DEFAULT_DATETIME
        else:
            from_date = datetime_to_utc(from_date)

        logger.info("Looking for topics at '%s', updated from '%s'",
                    self.url, str(from_date))

        self._purge_cache_queue()

        nposts = 0
        raw_posts = self.client.get_posts(from_date)

        for raw_post in raw_posts:
            self._push_cache_queue(raw_post)
            self._flush_cache_queue()
            post = json.loads(raw_post)
            nposts += 1
            yield post

        logger.info("Fetch process completed: %s posts fetched",
                    nposts)

    @metadata
    def fetch_from_cache(self):
        """Fetch the posts from the cache.

        :returns: a generator of posts

        :raises CacheError: raised when an error occurs accessing the
            cache
        """
        if not self.cache:
            raise CacheError(cause="cache instance was not provided")

        logger.info("Retrieving cached posts: '%s'", self.url)

        cache_items = self.cache.retrieve()

        nposts = 0

        for item in cache_items:
            logger.info(item)
            post = json.loads(item)
            nposts += 1
            yield post

        logger.info("Retrieval process completed: %s postss retrieved from cache",
                    nposts)

    @staticmethod
    def metadata_id(item):
        """Extracts the identifier from a Post item."""

        return str(item['id'])

    @staticmethod
    def metadata_updated_on(item):
        """Extracts the update time from a Post item.

        The timestamp used is extracted from 'updated_at' field.
        This date is converted to UNIX timestamp format taking into
        account the timezone of the date.

        :param item: item generated by the backend

        :returns: a UNIX timestamp
        """
        ts = item['updated_at']
        ts = str_to_datetime(ts)

        return ts.timestamp()


class DiscourseClient:
    """Discourse API client.

    This class implements a simple client to retrieve topics from
    any Discourse board.

    :param url: URL of the Discourse site
    :param token: Discourse API access token
    :param max_topics: maximun number of topics to fetch on a single query

    :raises HTTPError: when an error occurs doing the request
    """



    def __init__(self, url, token, max_topics):
        self.url = url
        self.token = token
        self.max_topics = max_topics

    def get_posts(self, from_date=DEFAULT_DATETIME):
        """Retrieve all the posts updated since a given date.

        :param from_date: obtain topics updated since this date
        """
        logger.debug("Fetching posts from %s", self.url)

        topics_id_list = self.get_topics_id_list(from_date)

        for topic_id in topics_id_list:
            req = requests.get(self.__build_base_url('t', topic_id),
                               params=self.__build_payload(None))
            req.raise_for_status()
            data = req.json()

            # topic category
            category_id = data['category_id']

            """
            Topic json contains 'chunk_size' posts data, no more
            The rest of the posts, if there are more, need to be
            requested through their ids.
            """
            chunk_size = data['chunk_size']
            posts_stream = data['post_stream']['stream']

            for post in data['post_stream']['posts']:
                if str_to_datetime(post['updated_at']) >= from_date:
                    # Add _category_id
                    post['_category_id'] = category_id
                    yield json.dumps(post)

            # For topics that post stream is bigger than chunk size
            if chunk_size < len(posts_stream):
                posts_stream_ids = posts_stream[chunk_size:]
                for post_id in reversed(posts_stream_ids):
                    req_post = requests.get(self.__build_base_url('posts', post_id),
                                            params=self.__build_payload(None))
                    req_post.raise_for_status()
                    data_post = req_post.json()
                    if str_to_datetime(data_post['updated_at']) >= from_date:
                        # Add _category_id
                        data_post['_category_id'] = category_id
                        yield json.dumps(data_post)
                    else:
                        break

    def get_topics_id_list(self, from_date=DEFAULT_DATETIME):
        """Retrieve all the topics ids updated since a given date.

        :param from_date: obtain topics updated since this date
        """
        logger.debug("Fetching topics ids from %s", self.url)

        url = urljoin(self.url, 'latest.json')
        payload = self.__build_payload()

        req = requests.get(url, payload)
        req.raise_for_status()

        data = req.json()
        for topic in data['topic_list']['topics']:
            if str_to_datetime(topic['last_posted_at']) >= from_date:
                yield topic['id']

        page = 0

        while 'more_topics_url' in data['topic_list']:
            page = page + 1
            req = requests.get(self.url+'/latest.json', self.__build_payload(page))
            req.raise_for_status()
            data = req.json()
            for topic in data['topic_list']['topics']:
                if str_to_datetime(topic['last_posted_at']) >= from_date:
                    yield topic['id']

    def __build_base_url(self, item_type, item_id):
        id_json = str(item_id) + '.json'
        base_api_url = urljoin(self.url, item_type, id_json)
        return base_api_url

    def __build_payload(self, page=None):
        payload = {'page': page,
                   'api_key': self.token}
        return payload


class DiscourseCommand(BackendCommand):
    """Class to run Discourse backend from the command line."""

    def __init__(self, *args):
        super().__init__(*args)
        self.url = self.parsed_args.url
        self.backend_token = self.parsed_args.backend_token
        self.max_topics = self.parsed_args.max_topics
        self.outfile = self.parsed_args.outfile
        self.origin = self.parsed_args.origin
        self.from_date = str_to_datetime(self.parsed_args.from_date)

        if not self.parsed_args.no_cache:
            if not self.parsed_args.cache_path:
                base_path = os.path.expanduser('~/.perceval/cache/')
            else:
                base_path = self.parsed_args.cache_path

            cache_path = os.path.join(base_path, self.url)

            cache = Cache(cache_path)

            if self.parsed_args.clean_cache:
                cache.clean()
            else:
                cache.backup()
        else:
            cache = None

        self.backend = Discourse(self.url, self.backend_token, self.max_topics,
                                 cache=cache, origin=self.origin)

    def run(self):
        """Fetch and print the posts.

        This method runs the backend to fetch the posts of a given
        Discourse URL. Posts are converted to JSON objects and printed
        to the defined output.
        """
        if self.parsed_args.fetch_cache:
            posts = self.backend.fetch_from_cache()
        else:
            posts = self.backend.fetch(from_date=self.from_date)

        try:
            for post in posts:
                obj = json.dumps(post, indent=4, sort_keys=True)
                self.outfile.write(obj)
                self.outfile.write('\n')
        except requests.exceptions.HTTPError as e:
            raise requests.exceptions.HTTPError(str(e.response.json()))
        except IOError as e:
            raise RuntimeError(str(e))
        except Exception as e:
            if self.backend.cache:
                self.backend.cache.recover()
            raise RuntimeError(str(e))

    @classmethod
    def create_argument_parser(cls):
        """Returns the Discourse argument parser."""

        parser = super().create_argument_parser()

        # Discourse options
        group = parser.add_argument_group('Discourse arguments')
        group.add_argument('--max-topics', dest='max_topics',
                           type=int, default=MAX_topics,
                           help="Max number of topics to be requested")

        # Required arguments
        parser.add_argument('url',
                            help="URL of the Discourse server")

        return parser
