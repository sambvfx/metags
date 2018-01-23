import os
import six
import hashlib
from kids.cache import cache

from typing import *


def _b58encode(bytes):
    """
    Base58 Encode bytes to string
    
    Relevant code taken from: 
        https://github.com/Avalanche-io/pyc4
    """
    __b58chars = '123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ'
    __b58base = len(__b58chars)
    if six.PY2:
        long_value = int(bytes.encode("hex_codec"), 16)
    else:
        long_value = int(bytes.hex(), 16)

    result = ''
    while long_value >= __b58base:
        div, mod = divmod(long_value, __b58base)
        result = __b58chars[mod] + result
        long_value = div

    result = __b58chars[long_value] + result

    return result


@cache
def createC4hash(filepath, **kwargs):
    """
    Caluculate a c4 hash from a filepath.
    
    Relevant code taken from: 
        https://github.com/Avalanche-io/pyc4
    
    Parameters
    ----------
    filepath : str 
    kwargs : Dict
        Unused kwargs for `kids.cache` to re-trigger the hash caluculation.

    Returns
    -------
    str
    """
    sha512_hash = hashlib.sha512()
    with open(filepath, 'r') as f:
        block_size = 100 * (2 ** 20)
        cnt_blocks = 0
        while True:
            try:
                block = f.read(block_size)
            except UnicodeDecodeError:
                break
            if not block:
                break
            if six.PY3:
                block = block.encode('utf-8')
            sha512_hash.update(block)
            cnt_blocks = cnt_blocks + 1
        f.close()

    hash_sha512 = sha512_hash.digest()

    c4_id_length = 90
    b58_hash = _b58encode(hash_sha512)

    # pad with '1's if needed
    padding = ''
    if len(b58_hash) < (c4_id_length - 2):
        padding = ('1' * (c4_id_length - 2 - len(b58_hash)))

    # combine to form C4 ID
    string_id = 'c4' + padding + b58_hash
    return string_id


def c4hash(filepath):
    """
    Calculate a c4 hash from a filepath.
    
    Parameters
    ----------
    filepath : str

    Returns
    -------
    str
    """
    filepath = os.path.realpath(filepath)
    statinfo = os.stat(filepath)
    return createC4hash(
        filepath, st_size=statinfo.st_size, st_mtime=statinfo.st_mtime)


def tracktime(func):

    import time
    import functools

    @functools.wraps(func)
    def _wrap(*args, **kwargs):
        start = time.time()
        results = func(*args, **kwargs)
        print('{} took {:06f}s'.format(func.__name__, time.time() - start))
        return results

    return _wrap
