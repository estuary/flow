#!/usr/bin/env python

import argparse
import getpass
import gzip
import json
import logging
import os.path
import re
import requests
import sys
import urlparse


class StreamDownloader(object):
    """StreamDownloader fetches new data from streams, producing a gzip'd batch
    file in the output directory with all stream content produced since the
    last invocation. An additional JSON METADATA file is stored in the
    directory, which captures metadata used to perform incremental downloads
    across invocations."""

    # Headers returned by Pippio, which are parsed by the client and allow for
    # direct retrieval of content from a cloud-storage provider.
    CONTENT_RANGE_HEADER = 'Content-Range'
    CONTENT_RANGE_REGEXP = 'bytes\s+(\d+)-\d+/\d+'
    FRAGMENT_LOCATION_HEADER = 'X-Fragment-Location'
    FRAGMENT_NAME_HEADER = 'X-Fragment-Name'

    # Buffer size for bulk copy.
    BUFFER_SIZE = 1 << 14  # 16384

    def __init__(self, output_dir, metadata_path, session):
        self.output_dir = output_dir
        self.metadata_path = metadata_path
        self.session = session

        self._metadata = self._load_metadata()

    def fetch_some(self, stream_url):
        """Retrieves all new content from the last-processed & stored offset.
        """

        offset = self._metadata['offsets'].get(stream_url, 0)

        # Perform a HEAD request to check for a directly fetch-able fragment.
        full_url = "%s?offset=%d&block=false" % (stream_url, offset)
        response = self.session.head(full_url)

        logging.debug("HEAD %s (%s)\n\t%s", full_url, response.status_code,
                      response.headers)

        if response.status_code == requests.codes.range_not_satisfiable:
            # No futher content is available. We're done.
            return False
        else:
            # Expect a 20X response.
            response.raise_for_status()

        offset = self._parse_response_offset(response.headers)
        fragment = self._parse_fragment_name(response.headers)
        location = response.headers[self.FRAGMENT_LOCATION_HEADER]

        basename = stream_url.split('/')[-1]
        path_tmp = os.path.join(self.output_dir,
                                ".%s.%016x.CURRENT.gz" % (basename, offset))
        output = gzip.open(path_tmp, 'w')

        # Check if the fragment is available to be directly downloaded (eg,
        # from cloud storage. Omit file:// URLs (returned in some Pippio test
        # environments).
        if location is not None and not location.startswith('file://'):
            delta = self._transfer_from_location(offset, fragment, location,
                                                 output)
        else:
            # Repeat the request as a GET to directly transfer.
            full_url = "%s?offset=%d&block=false" % (stream_url, offset)
            response = self.session.get(full_url, stream=True)

            logging.debug("GET %s (%s)\n\t%s", full_url, response.status_code,
                          response.headers)

            # Expect a 20X response.
            response.raise_for_status()

            delta = self._transfer(response.raw, output)

        # Close and move to final location.
        output.close()
        path_final = os.path.join(self.output_dir, "%s.%016x.%016x.gz" % (
                                  basename, offset, offset+delta))
        os.rename(path_tmp, path_final)

        self._metadata['offsets'][stream_url] = offset + delta
        self._store_metadata()

        logging.info("wrote %s (offset %d-%d)", path_final, offset,
                     offset+delta)
        return True

    def _transfer_from_location(self, offset, fragment, location, stream_out):
        """Transfers to |stream_out| starting at |offset| from the named
        |location| and |fragment|."""

        skip_delta = offset - fragment[0]
        if skip_delta < 0:
            raise RuntimeError("Unexpected offset: %d (%r)", offset, fragment)

        stream = self.session.get(location, stream=True)
        stream.raise_for_status()

        self._discard(stream.raw, delta)
        return self._transfer(stream.raw, stream_out)

    def _transfer(self, stream_in, stream_out):
        """Transfers from |stream_in| to |stream_out|, returning the number of
        bytes transferred before EOF of |stream_in|."""

        delta = 0
        while True:
            buf = stream_in.read(self.BUFFER_SIZE)
            if buf == '':
                return delta

            stream_out.write(buf)
            delta += len(buf)

        return delta

    def _discard(self, stream_in, count):
        """Discards |count| bytes from |stream_in|. Raises if |stream_in|
        returns EOF prior to |count| bytes being read."""

        while count > 0:
            chunk = stream_in.read(min(count, self.BUFFER_SIZE))
            if chunk == '':
                raise RuntimeError("unexpected EOF (%d remaining to read)",
                                   count)
            count -= len(chunk)

    def _parse_fragment_name(self, headers):
        """Parses a stream fragment name (as begin-offset, end-offset,
        content-sum)."""

        first, last, sha_sum = headers[self.FRAGMENT_NAME_HEADER].split('-')
        first, last = int(first, 16), int(last, 16)
        return (first, last, sha_sum)

    def _parse_response_offset(self, headers):
        content_range = headers[self.CONTENT_RANGE_HEADER]

        m = re.match(self.CONTENT_RANGE_REGEXP, content_range)
        if m is None:
            raise RuntimeError("invalid range %s" % content_range)

        return int(m.group(1), 10)

    def _load_metadata(self):
        """Reads and returns a metadata bundle, or returns a newly-initialized
        bundle if none exists."""
        if not os.path.isfile(self.metadata_path):
            logging.debug("%s not a file: returning empty metadata",
                          self.metadata_path)
            return {'offsets': {}}

        return json.load(open(self.metadata_path))

    def _store_metadata(self):
        """Atomically writes the current metadata bundle."""
        path_tmp = self.metadata_path + '.TMP'

        out = open(path_tmp, 'w')
        json.dump(self._metadata, out)
        out.close()

        os.rename(path_tmp, self.metadata_path)
        logging.debug("wrote metadata: %s", self.metadata_path)


def new_authenticated_session(auth_url, user, password):
    """Constructs a requests Session pre-configured with authentication tokens
    for |user| and |password|. If no password is set, one is read via stdin."""
    session = requests.Session()

    # If credentials are provided, obtain a signed authentication cookie.
    if user is not None:
        # Support optionally reading password directly from stdin.
        if password is None:
            password = getpass.getpass("%r password: " % user)

        payload = {'username': user, 'password': password}
        response = session.post(auth_url, data=json.dumps(payload))
        response.raise_for_status()

    return session


def main(argv):

    parser = argparse.ArgumentParser(description='Provides batch record '
                                     'download from a Pippio stream')
    parser.add_argument('--url', required=True, help='Stream URL to '
                        'download (ex, https://pippio.com/api/stream/records)')
    parser.add_argument('--user', help='Username to authenticate as')
    parser.add_argument('--password', help='Optional password to authenticate '
                        'with. %s will prompt for a password if one is not '
                        'provided' % os.path.basename(argv[0]))
    parser.add_argument('--output-dir',
                        help='Optional output directory for downloads. '
                        'Defaults to the current directory',
                        default='.')
    parser.add_argument('--metadata',
                        help='Optional path for storing download metadata '
                        'between invocations. Defaults to METADATA in '
                        '--output-dir if not set')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args(argv[1:])

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if not args.metadata:
        args.metadata = os.path.join(args.output_dir, 'METADATA')

    # Obtain an authenticated session.
    parsed_url = urlparse.urlparse(args.url)
    auth_url = "%s://%s/api/auth" % (parsed_url.scheme, parsed_url.netloc)
    session = new_authenticated_session(auth_url, args.user, args.password)

    # Download while content remains.
    downloader = StreamDownloader(args.output_dir, args.metadata, session)
    while downloader.fetch_some(args.url):
        pass

if __name__ == '__main__':
    sys.exit(main(sys.argv))
