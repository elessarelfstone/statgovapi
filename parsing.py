import json
import os
import urllib3
from collections import deque
from dataclasses import dataclass

import attr
import requests
from box import Box
from requests import ConnectionError, HTTPError

from structures import JuridicalInfo
from utils import requests_retry_session, read_file, append_file, load_lines


status_forcelist = (429, 500, 502, 504)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ParseError(Exception):
    pass


class NetworkError(Exception):
    pass


def parse(idx, url, retries=None, backoff=None,
          timeout=None, session=None):

    try:
        session = requests_retry_session(retries, backoff,
                                         status_forcelist=status_forcelist,
                                         session=session)
        response = session.get(url.format(idx), timeout=timeout, verify=False)
        jsdata = Box(json.loads(response.text))
        if not jsdata.success:
            raise ParseError('Status success is not true')

        return jsdata.obj

    except (ConnectionError, HTTPError):
        raise NetworkError(f'Failed to process {idx}')


def prepare(row_obj):
    # cast to lowercase
    # all fields of JuridicalInfo in lowercase
    _p_dict = {k.lower(): v for k, v in row_obj.items() if k.lower()}

    # wrap in JuridicalInfo
    # so we can convert values and correct them
    data = JuridicalInfo(**_p_dict)

    # return only values
    return attr.astuple(data)


class JuridicalInfoParser():
    url = "https://stat.gov.kz/api/juridical/?bin={}&lang=ru"
    headers = {}
    # 429 - too many requests
    status_forcelist = (429, 500, 502, 504)

    def __init__(self, in_fpath, limit_fsize, retries=None,
                 backoff=None, timeout=None):
        self.in_fpath = in_fpath
        self.limit_fsize = limit_fsize
        self.retries = retries
        self.backoff = backoff
        self.timeout = timeout
        self._output_files = []
        self._add_output_files()
        self._failed_bins = deque([])
        self._failed_bins_count = 0
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _curr_output_file(self):
        return self._output_files[len(self._output_files)-1]

    @property
    def fails(self):
        return self._failed_bins

    def add_fail(self, bn):
        self._failed_bins.append(bn)

    def pop_failed(self):
        return self._failed_bins.popleft()

    @property
    def output_file(self):
        return self._curr_output_file()

    def _add_output_files(self):
        """
        Gather all existed output files paths and
        if it's needed add new
        """
        self._output_files = []
        base = os.path.join(os.path.dirname(self.in_fpath),
                            os.path.splitext(os.path.basename(self.in_fpath))[0])

        output_path = f'{base}_out.csv'

        suffix = 2
        while os.path.exists(output_path):
            self._output_files.append(output_path)
            if os.path.getsize(output_path) < self.limit_fsize:
                return
            output_path = f'{base}_out_{suffix}.csv'
            suffix += 1

        open(output_path, 'a').close()
        self._output_files.append(output_path)

    def process(self, idx):

        # size limit for output csv file
        if os.path.getsize(self._curr_output_file()) >= self.limit_fsize:
            self._add_output_files()

        row = ''
        _data = tuple()
        try:
            _data = parse(idx, self.url, retries=self.retries, backoff=self.backoff,
                          timeout=self.timeout, session=self.session)

        except Exception:
            raise
        if _data:
            row = ';'.join(prepare(_data))
            append_file(self.output_file, row)

        return row
