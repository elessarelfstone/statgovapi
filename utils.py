import socket
import subprocess as subp
from urllib3 import Retry

import requests
from requests.adapters import HTTPAdapter


def load_lines(fpath):
    """ Return rows of file as list"""
    with open(fpath, "r", encoding="utf-8") as f:
        lines = [b.rstrip() for b in f.readlines()]

    return lines


def read_file(fpath):
    """ Return all rows of file as string"""
    with open(fpath, 'r', encoding="utf8") as f:
        data = f.read()

    return data


def append_file(fpath, data):
    """ Add new line to file"""
    with open(fpath, 'a+', encoding="utf8") as f:
        f.write(data + '\n')


def requests_retry_session(retries, backoff,
                           status_forcelist, session=None):
    """ Make request with timeout and retries """
    session = session or requests.Session()
    retry = Retry(total=retries,read=retries, connect=retries,
                  backoff_factor=backoff, status_forcelist=status_forcelist,
                  method_whitelist=frozenset(['GET', 'POST']))

    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session

def call_and_report(args, **kwargs):
    try:
        out_bytes = subp.check_output(args, stderr=subp.STDOUT, **kwargs)
        return out_bytes.decode('utf-8')
    except CalledProcessError as e:
        return e.output.decode('utf-8')

def run_command(args, encoding="utf-8", **kwargs):
    p = subp.Popen(args, stdout=subp.PIPE, stderr=subp.PIPE, **kwargs)
    result, err = p.communicate()
    if p.returncode > 1:
        raise IOError(err)

    if p.returncode == 1 and result:
        return result.decode(encoding).strip().split()[0]

    return None


def is_server_up(ip, port):
    is_up = True
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((ip, int(port)))
    if result != 0:
        is_up = False

    return is_up
