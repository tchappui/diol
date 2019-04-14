# -*- coding: utf-8 -*-

"""Core module of the dababase IO layer package"""

import re
import threading
from dataclasses import dataclass, asdict
from inspect import signature

import records

_db_connections = {}


class Repository:
    """Generic repository class suitable for basic database handling."""

    connection_lock = threading.Lock()

    def __init__(self, model):
        """Initializes the repository.

        Args:
            model (type): dataclass representing the database entity.

        """
        # Initialisation of the connection to the database
        self._connection_key = None
        with self.connection_lock:
            if not self._connection_key:
                self._connection_key = 'default'
                _db_connections[self._connection_key] = records.Database()
            self._db = _db_connections[self._connection_key]
        # Entity-related attributes
        self.model = model
        self.model_name = model.__name__
        if hasattr(model, 'table_name'):
            self.table_name = model.table_name
        else:
            parts = re.findall('[A-Z][^A-Z]*', self.model_name)
            self.table_name = "_".join(parts).lower().strip()


    @property
    def _last_id(self):
        """Returns the last auto-generated ID."""
        rows = self._db.query("""
            SELECT LAST_INSERT_ID() as id
        """).all(as_dict=True)
        if len(rows) and 'id' in rows[0]:
            return rows[0]['id']
        return None

    def _columns(self, data):
        """Generates SQL line column selection in select or insert queries."""
        return ", ".join([col for col in data])

    def _placeholders(self, data):
        """Generates SQL line for placeholders for values in insert queries."""
        return ", ".join([f":{col}" for col in data])

    def _where(self, data):
        """Generates SQL line for where conditions in select queries."""
        return " AND ".join([f"{col}=:{col}" for col in data])

    def _create(self, data):
        """Creates a new database entry using the given dictionnary."""
        self._db.query(f"""
            INSERT INTO {self.table_name}({self._columns(data)})
            VALUES ({self._placeholders(data)})
        """, **data)

    def _get_all(self):
        """Selects all the entries in the considered table."""
        return self._db.query(f"""
            SELECT * FROM {self.table_name}
        """).all(as_dict=True)

    def _get_last(self):
        """Selects the last inserted query."""
        return self._db.query(f"""
            SELECT * FROM {self.table_name} WHERE id=:id
        """, id=self._last_id).all(as_dict=True)

    def _get_all_by(self, data):
        """Selects all the database entries that match the given data."""
        return self._db.query(f"""
            SELECT * FROM {self.table_name} WHERE {self._where(data)}
        """, **data).all(as_dict=True)

    def create(self, **data):
        """Creates a new entry and returns the corresponding instance."""
        for key, value in data.items():
            if is_model(type(value)):
                value.get_or_save()
                del data[key]
                data[f"{key}_id"] = value.id
        self._create(data)
        if 'id' not in data:
            data['id'] = self.last_id
        return self.model(**data)


    def get_or_create(self, **data):
        """Selects an entry based on the given data and creates one if nothing
        is found.
        """
        for key, value in data.items():
            if is_model(type(value)):
                value.get_or_save()
                del data[key]
                data[f"{key}_id"] = value.id
        rows = self._get_all_by(data)
        if not rows:
            self._create(data)
            rows = self._get_last()

        return self.model(**rows[0])

    def filter(self, **data):
        """Selects all database entries that match the given data or an empty
        list if nothing is found.
        """
        rows = self._get_all_by(data)
        return [self.model(**elem) for elem in rows]

    def get(self, **data):
        """Selects the first data entry that match the given data or None if
        nothing is found.
        """
        rows = self._get_all_by(data)
        if rows:
            return self.model(**rows[0])
        return None


    def save(self, instance):
        """Saves a new instance in the database."""
        if not is_model(type(instance)):
            return instance
        data = {
            k: v
            for k, v in asdict(instance).items()
            if v is not None
        }
        for key, value in instance.__dict__.items():
            if is_model(type(value)):
                value.get_or_save()
                del data[key]
                data[f"{key}_id"] = value.id
        self._create(data)
        if not instance.id:
            instance.id = self._last_id
        return instance

    def get_or_save(self, instance):
        """Fills in the id of instance or save it if not already in database.
        """
        data = {
            k: v
            for k, v in asdict(instance).items()
            if v is not None
        }
        for key, value in instance.__dict__.items():
            if is_model(type(value)):
                value.get_or_save()
                del data[key]
                data[f"{key}_id"] = value.id
        rows = self._get_all_by(data)
        if not rows:
            self._create(data)
            rows = self._get_last()
        diffs = {
            k: v
            for k, v in rows[0].items()
            for k2, v2 in data.items()
            if v != v2
        }
        for key, val in diffs.items():
            setattr(instance, key, val)
        return instance


    def save_all(self, collection):
        """Saves a collection of new instances in the database."""
        if collection:
            for instance in collection:
                self.save(instance)

    def get_all(self):
        """Selects all the entries of the corresponding entity in the database.
        """
        rows = self._get_all()
        return [self.model(**elem) for elem in rows]

class Model:
    """Class decorator used to create models.

    The decorator transforms the entity into a dataclass and injects a
    repository instance as class attribute objects as well as a save method.

    """
    def __new__(cls, entity):
        """Builds the decorator.

        Args:
            entity: entity klass sent to the decorator.

        """
        return entity

    def __call__(self, **args, **kwargs):
        self.entity = dataclass(self.entity)

        # Injection of a direct link to the Repository instance
        if not hasattr(self.entity, 'objects'):
            self.entity.objects = Repository(self.entity)

        # Injection of shortcuts to the save methods of the Repository
        self.entity.save = lambda this: entity.objects.save(this)
        entity.get_or_save = lambda this: entity.objects.get_or_save(this)

        entity.__init__ = self._init_wrapper
        return entity

    def _init_wrapper(self, this, *args, **kwargs):
        self.entity.__init__(this, *args, **kwargs)
        sig = signature(entity.__init__)
        for param in sig.parameters:
            if is_model(param.annotation):
                setattr(
                    self,
                    param.name, p
                    param.annotation.objects.get(id=getattr(self, param.name))
                )


def is_model(entity):
    if hasattr(entity, 'objects') and isinstance(entity.objects, Repository):
        return True
