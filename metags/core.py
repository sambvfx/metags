import os
import six
import datetime
import collections
import attr
import metags.utils
import metags.events

from typing import Dict, Optional, Any


@attr.s
class Item(object):
    """
    Represents data at a given url.
    """
    url = attr.ib()
    c4 = attr.ib(default=None)
    metadata = attr.ib(default=attr.Factory(
        lambda: collections.defaultdict(list)))

    def tag(self, key, value):
        """
        Add a metadata tag.
        
        Parameters
        ----------
        key : str
        value : List[Any]
        """
        if isinstance(value, six.string_types):
            self.metadata[key].append(value)
        elif isinstance(value, (tuple, list, set)):
            self.metadata[key].extend(value)

    def c4id(self):
        """
        Calculate a c4 hash for the filepath's url.
        
        Returns
        -------
        str
        """
        return metags.utils.c4hash(self.url)
