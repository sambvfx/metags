"""
Memory storage model.
"""
from metags.events import event
from metags.storage.base import AbstractStorageEngine

from typing import TYPE_CHECKING, Optional, Dict, Any


if TYPE_CHECKING:
    import metags.core


class MemoryStorageEngine(AbstractStorageEngine):
    """
    Simple memory-only storage engine.
    """
    def __init__(self):
        self._data = []

    @event('storage_add')
    def add(self, item):
        """
        Store an item.

        Parameters
        ----------
        item : metags.core.Item

        Returns
        -------
        metags.core.Item
        """
        if item not in self._data:
            self._data.append(item)
        return item

    def all(self):
        return list(self._data)

    def get(self, c4=None, url=None, metadata=None):
        """
        Get `Item`s from either a c4 id, a url or a metadata value(s).

        Parameters
        ----------
        c4 : Optional[str]
        url : Optional[str]
        metadata : Optional[Dict[str, Any]]

        Returns
        -------
        List[metags.core.Item]
        """
        import fnmatch
        if c4 is not None:
            return [x for x in self._data if fnmatch.fnmatch(x.c4, c4)]
        elif url is not None:
            return [x for x in self._data if fnmatch.fnmatch(x.url, url)]
        elif metadata is not None:
            return [x for x in self._data if any(fnmatch.fnmatch(
                x.metadata[k], v) for k, v in metadata.iteritems())]
