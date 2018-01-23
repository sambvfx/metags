"""
Factories are helpers for populating items on a storage engine.
"""
from __future__ import print_function
import os
import six
import metags.core
from metags.utils import tracktime


class AbstractFactory(object):
    """
    Abstract class for populating records on a storage engine.
    """
    def __init__(self, storage):
        self.storage = storage


class FilepathFactory(AbstractFactory):
    """
    Filepath factory where all items are expected to be filepaths.
    """
    @classmethod
    def from_filepath(cls, filepath, metadata=None):
        """
        Item constructor that populates stat info into the instances metadata.

        Parameters
        ----------
        filepath : str
        metadata : Optional[Dict[str, List[Any]]]

        Returns
        -------
        metags.core.Item
        """
        import datetime
        import metags.utils
        filepath = os.path.realpath(filepath)
        assert os.path.isfile(filepath), 'Only existing files are valid.'
        statinfo = os.stat(filepath)
        metadata = metadata or {}
        metadata['st_mtime'] = [datetime.datetime.fromtimestamp(
            statinfo.st_mtime)]
        metadata['st_size'] = [statinfo.st_size]
        c4id = metags.utils.createC4hash(
            filepath, st_size=statinfo.st_size, st_mtime=statinfo.st_mtime)
        return metags.core.Item(url=filepath, c4=c4id, metadata=metadata)

    @tracktime
    def generate_syncronously(self, filepath, pattern=None):
        """
        Generate FilepathItem instances from the passed filepath.

        Parameters
        ----------
        filepath : str
        pattern : Union[str, _sre.SRE_Pattern]

        Returns
        -------
        List[metags.core.Item]
        """
        import re

        if pattern and isinstance(pattern, six.string_types):
            pattern = re.compile(pattern)

        filepath = os.path.realpath(filepath)

        results = []

        def walk(path):
            for x in os.listdir(path):
                x = os.path.join(path, x)
                if os.path.isdir(x):
                    for f in walk(x):
                        yield f
                else:
                    if pattern:
                        if pattern.match(x):
                            yield x
                    else:
                        yield x

        for path in walk(filepath):
            results.append(self.from_filepath(path))

        return results

    generate = generate_syncronously

    if six.PY3:

        @tracktime
        def generate_asyncronously(self, filepath, pattern=None):
            """
            Generate FilepathItem instances from the passed filepath.

            Parameters
            ----------
            filepath : str
            pattern : Union[str, _sre.SRE_Pattern]

            Returns
            -------
            List[metags.core.Item]
            """
            import os
            import re
            import asyncio

            results = []

            if pattern and isinstance(pattern, six.string_types):
                pattern = re.compile(pattern)

            dirqueue = asyncio.Queue()
            filequeue = asyncio.Queue()

            @asyncio.coroutine
            def async_walk(q):
                while not q.empty():
                    path = yield from q.get()
                    for x in os.scandir(path):
                        if x.is_dir():
                            q.put_nowait(x.path)
                        else:
                            if pattern:
                                if pattern.match(x.path):
                                    filequeue.put_nowait(x.path)
                            else:
                                filequeue.put_nowait(x.path)
                    yield from asyncio.sleep(0)

            @asyncio.coroutine
            def async_from_filepath(q):
                while not q.empty():
                    path = yield from q.get()
                    print('[{}] {}'.format(q.qsize(), path))
                    results.append(self.from_filepath(path))
                    yield from asyncio.sleep(0)

            dirqueue.put_nowait(filepath)
            loop = asyncio.get_event_loop()

            tasks = [
                asyncio.Task(async_walk(dirqueue)),
                asyncio.Task(async_from_filepath(filequeue)),
            ]

            loop.run_until_complete(asyncio.wait(tasks))
            # loop.close()

            return results

        generate = generate_asyncronously

    def add(self, filepath, pattern=None):
        """
        Add a filepath to the storage registry. Recurses into any directories
        and adds any of those paths as well.
        
        Parameters
        ----------
        filepath : str
        pattern : Union[str, _sre.SRE_Pattern]
        """
        for item in self.generate(filepath, pattern=pattern):
            self.storage.add(item)
