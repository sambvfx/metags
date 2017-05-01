"""
Factories are helpers for populating items on a storage engine.
"""
import os
import six
import metags.core


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
    def __init__(self, storage, recurse=True):
        super(FilepathFactory, self).__init__(storage)
        self.recurse = recurse

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
        for stat in ('st_atime', 'st_mtime', 'st_ctime'):
            if stat not in metadata:
                metadata[stat] = [datetime.datetime.fromtimestamp(
                    getattr(statinfo, stat))]
        metadata['st_size'] = [statinfo.st_size]
        c4id = metags.utils.createC4hash(filepath, statinfo)
        return metags.core.Item(url=filepath, c4=c4id, metadata=metadata)

    def generate(self, filepath, pattern=None):
        """
        Generate FilepathItem instances from the passed filepath.
        
        Parameters
        ----------
        filepath : str
        pattern : Union[str, _sre.SRE_Pattern]

        Returns
        -------
        Iterable[metags.core.Item]
        """
        import re

        if pattern and isinstance(pattern, six.string_types):
            pattern = re.compile(pattern)

        filepath = os.path.realpath(filepath)

        if os.path.isdir(filepath):
            if not self.recurse:
                raise ValueError(
                    'Cannot add a directory. This can be changed by enabling '
                    '`recurse` on the factory.')
            for fname in os.listdir(filepath):
                for itm in self.generate(
                        os.path.join(filepath, fname), pattern=pattern):
                    yield itm
        elif os.path.isfile(filepath):
            if pattern:
                if pattern.match(filepath):
                    yield self.from_filepath(filepath)
            else:
                yield self.from_filepath(filepath)
        else:
            raise ValueError('Invalid filepath {!r}'.format(filepath))

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
