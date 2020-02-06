import os
from collections import deque
from os.path import expanduser

from parsing import JuridicalInfoParser
from utils import load_lines, append_file

HOME = expanduser('~')
FILE_PATH = os.path.join(HOME, 'data', 'ids_test.txt')
MAX_FILE_OUTPUT_SIZE = 500000000
RETRIES = 4
TIMEOUT = 1
BACKOFF = 0.8


def processed_ids_fpath(fpath):
    """ Return path to file with processed BINs """
    return os.path.splitext(fpath)[0] + '.prsd'


def main():

    prsd_ids = []

    # if file with parsed IDS exists
    # that means we've processed some part of them before
    # also it might be all of them...
    # but with fails(connection, network)
    if not os.path.exists(processed_ids_fpath(FILE_PATH)):
        open(processed_ids_fpath(FILE_PATH), 'a').close()
    else:
        prsd_ids = load_lines(processed_ids_fpath(FILE_PATH))

    src_ids = load_lines(FILE_PATH)

    if prsd_ids:
        s = set(src_ids)
        s.difference_update(set(prsd_ids))
        ids = list(s)
    else:
        ids = src_ids

    if not ids:
        exit(1)

    ids = deque(ids)
    parser = JuridicalInfoParser(FILE_PATH, MAX_FILE_OUTPUT_SIZE, retries=RETRIES,
                                 backoff=BACKOFF, timeout=TIMEOUT)
    cnt = 0
    print('-' * 20)
    while ids:
        try:
            idx = ids.popleft()

            _row = parser.process(idx)
            cnt += 1

            print(f'{cnt} - {_row}')
            print('-' * 20)
        except Exception:
            print('Failed to process {}'.format(idx))
        else:
            append_file(processed_ids_fpath(FILE_PATH), idx)


if __name__ == '__main__':
    main()
