"""
Database storage model.
"""
from metags.core import Item
from metags.events import event
from metags.storage.base import AbstractStorageEngine
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, joinedload, aliased
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base

from typing import TYPE_CHECKING, Optional, Dict, Any, List


if TYPE_CHECKING:
    import metags.core
    import sqlalchemy.orm.session


Base = declarative_base()


class Entity(Base):
    __tablename__ = 'entity'
    id = Column(Integer, autoincrement=True, primary_key=True)
    # FIXME: add a unique index for url + c4
    url = Column(String)
    c4 = Column(String)

    meta = relationship('LinkMeta')

    def __repr__(self):
        return '{}(id={!r}, url={!r}, c4={!r})'.format(
            self.__class__.__name__, self.id, self.url, self.c4)


class Meta(Base):
    __tablename__ = 'meta'
    id = Column(Integer, autoincrement=True, primary_key=True)
    content = Column(String)


class LinkMeta(Base):
    __tablename__ = 'link_meta'
    id = Column(Integer, autoincrement=True, primary_key=True)
    entity_id = Column(Integer, ForeignKey(Entity.id))
    key_id = Column(Integer, ForeignKey(Meta.id))
    key = relationship(Meta, primaryjoin='LinkMeta.key_id == Meta.id')
    value_id = Column(Integer, ForeignKey(Meta.id))
    value = relationship(Meta, primaryjoin='LinkMeta.value_id == Meta.id')


class Transaction(object):

    # FIXME: support nested transactions

    def __init__(self, session):
        """
        Parameters
        ----------
        session : sqlalchemy.orm.session.Session
        """
        self.session = session

    def __enter__(self):
        """
        Returns
        -------
        sqlalchemy.orm.session.Session
        """
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise


class DatabaseStorageEngine(AbstractStorageEngine):
    """
    Database storage engine.
    """
    def __init__(self, db='sqlite://'):
        """
        Parameters
        ----------
        db : str
            Database url.
        """
        self._engine = create_engine(db)
        Base.metadata.create_all(self._engine)
        self.session = sessionmaker(bind=self._engine)()

    def transaction(self):
        """
        Returns
        -------
        Transaction
        """
        return Transaction(self.session)

    def to_item(self, entity):
        """
        Create an Item instance from an entity.
        
        Parameters
        ----------
        entity : Entitiy

        Returns
        -------
        metags.core.Item
        """
        item = Item(url=entity.url, c4=entity.c4)
        for meta in entity.meta:
            item.tag(meta.key.content, meta.value.content)
        return item

    def to_entity(self, item):
        """
        Fetches the cooresponding Entitiy instance from the database.
        
        Parameters
        ----------
        item : metags.core.Item

        Returns
        -------
        Entity
        """
        return self.session.query(Entity) \
            .filter_by(url=item.url, c4=item.c4) \
            .options(joinedload('meta')) \
            .one()

    def get_meta(self, content):
        """
        Get existing or create new meta.
        
        Parameters
        ----------
        content : Any

        Returns
        -------
        Meta
        """
        with self.transaction() as session:
            try:
                meta = session.query(Meta).filter_by(content=content).one()
            except NoResultFound:
                meta = Meta(content=content)
                session.add(meta)
        return meta

    def link_meta(self, entity, metadata):
        """
        Create relationships between entity and metadata.
        
        Parameters
        ----------
        entity : Entity
        metadata : dict
        """
        with self.transaction() as session:
            for k, v in metadata.iteritems():
                key = self.get_meta(k)
                for content in v:
                    value = self.get_meta(content)
                    kwargs = dict(entity_id=entity.id, key_id=key.id,
                                  value_id=value.id)
                    try:
                        link = session.query(LinkMeta)\
                            .filter_by(**kwargs)\
                            .one()
                    except NoResultFound:
                        link = LinkMeta(**kwargs)
                        session.add(link)

    def update_meta(self, item):
        """
        Update metadata for the given item.
        
        Parameters
        ----------
        item : metags.core.Item
        """
        self.link_meta(self.to_entity(item), item.metadata)

    @event('db_storage_add')
    def add(self, item):
        """
        Add an item to storage.
        
        Parameters
        ----------
        item : metags.core.Item

        Returns
        -------
        metags.core.Item
        """
        # FIXME: Handle this by emitting an event instead. The goal would be
        # to offload calculating this to workers in parallel. Just use
        # sqlalchemy's event system to monitor new Entity rows?
        if not item.c4:
            item.c4 = item.c4id()

        with self.transaction() as session:
            try:
                entity = self.to_entity(item)
            except NoResultFound:
                entity = Entity(c4=item.c4, url=item.url)
                session.add(entity)

            self.link_meta(entity, item.metadata)

        return self, item

    def __iter__(self):
        for entity in self.session.query(Entity):
            yield self.to_item(entity)

    def all(self):
        """
        Returns
        -------
        List[metags.core.Item]
        """
        return list(self)

    def get(self, c4=None, url=None, **metadata):
        """
        Get `Item`s from either a c4 id, a url or metadata value(s).
        
        Note: The metadata logic implements a AND logic rather than OR.
        If more sophisticated querying is needed it's probably better to just
        use sqlalchemy directly.

        Parameters
        ----------
        c4 : Optional[str]
        url : Optional[str]
        metadata : Optional[Dict[str, Any]]

        Returns
        -------
        List[metags.core.Item]
        """
        if c4 is not None:
            if '*' in c4 or '%' in c4:
                query = self.session.query(Entity)\
                    .filter(Entity.c4.like(c4.replace('*', '%')))
            else:
                query = self.session.query(Entity).filter(Entity.c4 == c4)
        elif url is not None:
            if '*' in url or '%' in url:
                query = self.session.query(Entity)\
                    .filter(Entity.url.like(url.replace('*', '%')))
            else:
                query = self.session.query(Entity).filter(Entity.url == url)
        elif metadata:
            metakeys = aliased(Meta)
            metavalues = aliased(Meta)
            query = self.session.query(Entity)\
                .join(LinkMeta)\
                .join(metakeys, LinkMeta.key_id == metakeys.id)\
                .join(metavalues, LinkMeta.value_id == metavalues.id)
            for key, values in metadata.iteritems():
                if '*' in key or '%' in key:
                    query = query.filter(
                        metakeys.content.like(key.replace('*', '%')))
                else:
                    query = query.filter(metakeys.content == key)

                if not isinstance(values, (list, tuple, set)):
                    values = [values]

                for value in values:
                    if '*' in value or '%' in value:
                        query = query.filter(metavalues.content.like(
                            value.replace('*', '%')))
                    else:
                        query = query.filter(metavalues.content == value)
        else:
            return self.all()

        return [self.to_item(x) for x in query]
