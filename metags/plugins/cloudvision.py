import os
import metags.events

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import metags.core
    import metags.storage.database


IMAGE_EXT = ['.jpg', '.png']


# FIXME: This system needs more thought on how it should work.


@metags.events.listen('db_storage_add')
def add_cloudvision_labels(storage, item):
    """
    Callback for adding labels to an Item through google's vision API.
    
    Parameters
    ----------
    storage : metags.storage.database.DatabaseStorageEngine
    item : metags.core.Item

    Returns
    -------
    metags.core.Item
    """
    if os.path.splitext(item.url)[-1] in IMAGE_EXT:
        # FIXME: populate metadata from google image tag request
        item.tag('labels', ['image'])
        storage.update_meta(item)
    return item


@metags.events.listen('db_storage_add')
def add_image_dimensions_labels(storage, item):
    """
    Callback for adding labels to an Item through google's vision API.

    Parameters
    ----------
    storage : metags.storage.database.DatabaseStorageEngine
    item : metags.core.Item

    Returns
    -------
    metags.core.Item
    """
    if os.path.splitext(item.url)[-1] in IMAGE_EXT:
        try:
            from PIL import Image
        except ImportError:
            pass
        else:

            storage.update_meta(item)
    return item