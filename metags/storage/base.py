"""
Base storage module.
"""
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import metags.core


class AbstractStorageEngine(object):
    """
    Abstracted class for defining the interface of storage engines.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError
