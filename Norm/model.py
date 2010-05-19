"""
NORM model.py

Josh Marshall 2010
This file contains the Model class.
"""

from fields import Field, PrimaryField, ReferenceField, PrimaryField
from connection import connection
from results import Results

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
        assert self.__class__.primary() != None
        for field in self.fields():
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
        primary = cls.primary()
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
        if issubclass(type(attr), Field):
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
        are Fields. Sort of kludgy, needs to be cached somewhere.
        """
        fields = []
        for attr_k in dir(cls):
            try:
                attr = object.__getattribute__(cls, attr_k)
            except AttributeError:
                continue
            if issubclass(attr.__class__, Field):
                fields.append(attr_k)
        return fields
        
    @classmethod
    def table(cls):
        """
        Not really sure why I did this. Will pull it out eventually.
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
        for f in cls.fields():
            field = object.__getattribute__(cls, f)
            params = field.create_syntax()
            row = u'\t%s %s' % (f, params)
            rows.append(row)
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
        
    def save(self):
        """
        This is SUPPOSED to be intelligent and insert / update
        as necessary. :) Not working quite yet.
        """
        if not connection.connected:
            raise Exception('Not connected to the database.')
        if not getattr(self, '_retrieved', False):
            self.insert()
        else:
            self.update()
            
    def update(self):
        """
        Updates an existing entry in the table.
        """
        sets = []
        values = []
        for f in self.fields():
            attr = object.__getattribute__(self, f)
            if attr.auto_value:
                continue
            sets.append(u'%s = %s' % (f, attr.format))
            values.append(attr._value)
        set_sql = u'SET %s' % u', '.join(sets)
        primary_k = self.__class__.primary()
        primary = object.__getattribute__(self, primary_k)
        where_sql = u'WHERE %s = %d;' % (primary_k, primary.value)
        sql = 'UPDATE %s %s %s;' % (self.table(), set_sql, where_sql)
        cursor = connection.execute(sql, values)
        
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
        primary_k = self.__class__.primary()
        primary = object.__getattribute__(self, primary_k)
        primary.value = connection.connection.insert_id()
        
    def delete(self):
        """
        Deletes the object from the MySQL table.
        """
        primary_k = self.__class__.primary()
        primary = object.__getattribute__(self, primary_k)
        sql = u'DELETE FROM %s' % self.table()
        sql += u' WHERE %s=%s LIMIT 1;' % (primary_k, '%s');
        cursor = connection.execute(sql, (primary.value,))
     
    @classmethod   
    def primary(cls):
        """
        Hunts for the PrimaryField in the attributes.
        TODO: Cache this value so it only hunts once.
        """
        for f in cls.fields():
            attr = object.__getattribute__(cls, f)
            if type(attr) is PrimaryField:
                return f
