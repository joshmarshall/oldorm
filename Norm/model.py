"""
NORM model.py

Josh Marshall 2010
This file contains the Model class.
"""

from fields import Field, PrimaryField, ReferenceField, ReferenceManyField
from connection import connection
from results import Results
import types

class Model(object):
    """
    The Model class is designed to simplify row -> model -> row logic
    without crimping the speed benefit of going with a low-level
    interface like MySQLdb. Whether it succeeds... we'll see. :)
    """
    def __init__(self, **kwargs):
        """
        Checks the model at instance time. If there's no PrimaryField,
        it throws an Assertion error. (Will be better later.)
        It then ensures that each Model instance has its own
        Fields for values.
        """
        assert self.__class__.get_primary() != None
        for field in self.fields()+self.tables():
            f = object.__getattribute__(self, field)
            instance = f.__class__(*f.args, **f.kwargs)
            instance.model = self
            object.__setattr__(self, field, instance)
            
        for key,val in kwargs.iteritems():
            self.__setattr__(key, val)
            
    @classmethod
    def where(cls, limiter={}):
        """
        Wrapper for the Results.where() method
        """
        results = Results(cls)
        return results.where(limiter)   
                
    @classmethod
    def all(cls):
        """
        Gets the Results object for all entries.
        """
        return cls.where()
            
    @classmethod
    def __iter__(cls, limiter={}):
        """
        Wrapper for the Results.__iter__() method
        """
        results = cls.where(limiter)
        return results.__iter__()
        
    @classmethod
    def fetch_one(cls, limiter={}):
        """
        Wrapper for the Results.fetch_one() method
        """
        return cls.where(limiter).fetch_one()
        
    @classmethod
    def get(cls, id_value):
        """
        Simple grab for a single primary value
        """
        primary = cls.get_primary()
        if isinstance(id_value, cls):
            id_value = getattr(id_value, primary)
        return cls.fetch_one({ primary: id_value })
        
    def __getattribute__(self, attr_k):
        """
        This just returns the value of a Field, instead of 
        the Field itself. Probably not the cleanest / best
        way to do it...
        """
        # Getting the value of the field
        attr = object.__getattribute__(self, attr_k)
        attr_type = type(attr)
        if issubclass(attr_type, Field) or \
            issubclass(attr_type, ReferenceManyField):
            return attr.value
        else:
            return attr
            
    def __setattr__(self, attr_k, val):
        """
        Same as __getattribute__, this sets the value of a 
        Field instead of changing the actual Model attribute.
        """
        # Dynamically setting the value of the Field
        try:
            attr = object.__getattribute__(self, attr_k)
        except AttributeError:
            attr = None
        if issubclass(attr.__class__, Field):
            attr.value = val
        else:
            return object.__setattr__(self, attr_k, val)
            
    @classmethod
    def fields(cls):
        """
        A class method that returns all the attributes which
        are Fields.
        """
        if not hasattr(cls, '_fields'):
            cls.parse_attributes()
        return cls._fields
        
    @classmethod
    def tables(cls):
        """
        A class method that returns all the attributes which 
        are ReferenceManyField.
        """
        if not hasattr(cls, '_tables'):
            cls.parse_attributes()
        return cls._tables
        
    @classmethod
    def parse_attributes(cls):
        """
        Kludgy way of determining fields and tables (ReferenceManyField)
        """
        cls._fields = []
        cls._tables = []
        for attr_k in dir(cls):
            try:
                attr = object.__getattribute__(cls, attr_k)
            except AttributeError:
                continue
            if issubclass(attr.__class__, ReferenceManyField):
                cls._tables.append(attr_k)
            elif issubclass(attr.__class__, Field):
                cls._fields.append(attr_k)
        
    @classmethod
    def table(cls):
        """
        Is Model.table() more semantic than cls.__name__? I guess...
        """
        return cls.__name__
        
    @classmethod
    def create_table(cls):
        """
        Generates and executes the SQL to create a table.
        """
        if not connection.connected:
            raise Exception('Not connected to the database.')
        sql = u'CREATE TABLE IF NOT EXISTS %s (\n' % cls.table()
        rows = []
        indexes = {}
        for f in cls.fields():
            field = object.__getattribute__(cls, f)
            params = field.create_syntax()
            row = u'\t%s %s' % (f, params)
            rows.append(row)
            if hasattr(field, 'index') and field.index:
                indexes[f] = field.index
        index_strings = []
        for f,i in indexes.iteritems():
            if type(i) is types.IntType:
                index_string = u'%s(%d)' % (f,i)
            else:
                index_string = u'%s' % f
            index_strings.append(index_string)
        if len(index_strings) > 0:
            rows.append(u'\tINDEX(%s)' % u', '.join(index_strings))
        sql += u'%s\n);' % u',\n'.join(rows)
        cursor = connection.execute(sql)
        
    @classmethod
    def drop_table(cls):
        """
        Generates and executes the SQL to DROP a table.
        """
        if not connection.connected:
            raise Exception('Not connected to the database.')
        sql = u'DROP TABLE IF EXISTS %s' % cls.table()
        cursor = connection.execute(sql)
     
    @classmethod   
    def get_primary(cls):
        """
        Hunts for the PrimaryField in the attributes.
        TODO: Cache this value so it only hunts once.
        """
        if not hasattr(cls, '_primary'):
            for f in cls.fields():
                attr = object.__getattribute__(cls, f)
                if type(attr) is PrimaryField:
                    return f
            cls._primary = primary
        return cls._primary
                
    @property
    def primary(self):
        primary_k = self.__class__.get_primary()
        return getattr(self, primary_k)
        
    def save(self):
        """
        This is SUPPOSED to be intelligent and insert / update
        as necessary. :)
        """
        if not connection.connected:
            raise Exception('Not connected to the database.')
        if not getattr(self, '_retrieved', False):
            self.insert()
            self._retrieved = True
        else:
            self.update()
            
    def update(self):
        """
        Updates an existing entry in the table.
        """
        values = {}
        for f in self.fields():
            attr = object.__getattribute__(self, f)
            if not attr.auto_value and attr._updated:
                values[f] = getattr(self, f)
                object.__setattr__(attr, '_updated', False)
        result = self.where({self.__class__.get_primary():self.primary})
        return result.update(values)[0]
        
        
    def insert(self):
        """
        Inserts a new entry into the table, and
        assigns the "auto incremented" ID. It's probably
        a dangerous assumption that the PrimaryField is
        using that, so I may need to change this in the future.
        """
        sql = u'INSERT INTO %s' % self.table()
        fields = self.fields()
        keys = []
        values = []
        format_values = []
        for f in self.fields():
            attr = object.__getattribute__(self, f)
            if attr.auto_value:
                continue
            keys.append(f)
            format_values.append(attr.format)
            values.append(attr._value)
        keys_str = u'( %s )' % u', '.join(keys)
        values_str = u'VALUES( %s )' % u', '.join(format_values)
        sql = '%s %s %s;' % (sql, keys_str, values_str)
        cursor = connection.execute(sql, values)
        primary_k = self.__class__.get_primary()
        primary = object.__getattribute__(self, primary_k)
        primary.value = connection.connection.insert_id()
        
    def delete(self):
        """
        Deletes the object from the MySQL table.
        """
        result = self.where({self.__class__.get_primary():self.primary})
        return result.delete()[0]
        
    def __eq__(self, other):
        """
        For comparing two objects of the same model
        """
        if self.__class__ != other.__class__:
            return False
        if self.primary != other.primary:
            return False
        return True
        
    def __ne__(self, other):
        """
        Inverse of __eq___
        """
        return self.__eq__(other) == False
