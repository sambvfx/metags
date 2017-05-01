Create relationships between data represented via a url and a [c4 hash](http://www.cccc.io/) with metadata all within queryable storage.

Examples
--------

```python
import os
import datetime
import metags.core
import metags.storage.database

storage = metags.storage.database.DatabaseStorageEngine()

item = metags.core.Item(url='/Users/samb/Pictures/macbeth.png')
item = storage.add(item)

storage.get(url='*.png')
# [Item(url='/Users/samb/Pictures/macbeth.png')]

item.tag('labels', ['macbeth', 'color', 'chart'])
storage.update_meta(item)

storage.get(labels='color')
# [Item(url='/Users/samb/Pictures/macbeth.png')]

statinfo = os.stat(item.url)
for stat in ('st_atime', 'st_mtime', 'st_ctime'):
    item.tag(stat, datetime.datetime.fromtimestamp(getattr(statinfo, stat)))
item.tag('st_size', statinfo.st_size)

storage.update_meta(item)

storage.get(st_mtime='2017-04*')
# [Item(url='/Users/samb/Pictures/macbeth.png')]
```

You can use helpers to add things enmasse. For example the `metags.factory.FilepathFactory` will include stat info in the metadata for all items it generates. 

```python
import metags.factory
import metags.storage.database

storage = metags.storage.database.DatabaseStorageEngine()

factory = metags.factory.FilepathFactory(storage)
# recursively add files within this directory
factory.add('/Users/samb/Pictures')

storage.get(st_mtime='2017-04*')
# [Item(url='/Users/samb/Pictures/macbeth.png'), ...]
```