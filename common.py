import os
from dataclasses import dataclass

from utils import load_lines, get_base_fpath


@dataclass
class FileInfo:
    fpath: str
    size: int
    num: int


class ParseFilesManager:

    """ Helper for handling all
        input and output files manipulations
    """
    def __init__(self, ids_fpath, limit_fsize=100, ext='csv'):

        self._output_fpaths = []
        self._ids_fpath = ids_fpath
        self._limit_fsize = limit_fsize
        self._ext = ext

        # file dirname and file name without extension
        self._fpath_base = get_base_fpath(ids_fpath)

        # file path for logging parsed ids
        self._parsed_fpath = f'{self._fpath_base}.prsd'

        # at the beginning we need gather
        # all info about existing out files
        self._prepare()

    def _prepare(self):

        # first out file
        output_fpath = f'{self._fpath_base}_out.{self._ext}'
        num = 2

        # gather all existed out files
        while os.path.exists(output_fpath):
            f_info = FileInfo(fpath=output_fpath,
                              size=os.path.getsize(output_fpath),
                              num=num - 1)
            self._output_fpaths.append(f_info)
            output_fpath = f'{self._fpath_base}_out_{num}.{self._ext}'
            num += 1

        # if we haven't created any out file
        # initialize first out file
        if not self._output_fpaths:
            open(output_fpath, 'a').close()
            f_info = FileInfo(fpath=output_fpath,
                              size=os.path.getsize(output_fpath),
                              num=num - 1)
            self._output_fpaths.append(f_info)

    def _curr(self):

        # current out file will always
        # be last item of _output_fpaths
        return self._output_fpaths[-1]

    def update(self, size):
        curr = self._curr()

        # check if size of current out file
        # more than given limitation
        if curr.size >= self._limit_fsize:
            # increase number suffix for filename
            num = curr.num+1

            # build filename
            _new_fpath = f'{self._fpath_base}_out_{num}.{self._ext}'

            # create empty out file
            open(_new_fpath, 'a').close()

            # initialization info for new out file
            f_info = FileInfo(fpath=_new_fpath, size=0, num=num)
            self._output_fpaths.append(f_info)
        else:

            # just increase current file size
            curr.size += size

    @property
    def curr_file(self):
        return self._curr().fpath

    @property
    def parsed_file(self):
        return self._parsed_fpath

    @property
    def lines(self):
        return load_lines(self._ids_fpath)
