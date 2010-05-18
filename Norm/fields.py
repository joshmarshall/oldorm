"""
NORM fields.py

Josh Marshall 2010
This file (will) contain the various Field classes.

TODO: Add FloatField, BoolField, etc.
"""

import types
import datetime
import time

class Field(object):
    """
    The base field class, not to be used directly.
    """
    format = u'%s'
    field = None
    type = None
    # Auto value is just a flag right now for IDs and TIMESTAMPS
    auto_value = False
    _value = None
    
    def __init__(self, *args, **kwargs):
        """
        Applies parameters from the arguments, and
        saves the arguments for instance copying.
        """
        for k,v in kwargs.iteritems():
            setattr(self, k, v)
        self.args = args
        self.kwargs = kwargs
        
    def set_value(self, value):
        """
        Pretty self-explanatory -- basic type checking.
        """
        if value is None:
            if not getattr(self, 'null', True):
                raise TypeError('Column does not allow null values.')
        elif type(value) is not self.type:
            raise TypeError('Value must be type %s' % self.type)
        self._value = value
        
    @property
    def value(self):
        """
        This is a property so that sub-classes can do
        other fun things with it.
        """
        return self._value
        
    def set_model(self, model):
        """
        This sets the parent model for reference.
        """
        self.model = model
        
    def create_syntax(self):
        """
        Generates the column definition for the CREATE TABLE step.
        """
        sql = '%s' % self.field
        if not getattr(self, 'null', True):
            sql += ' NOT NULL'
        return sql
        
    
class UnicodeField(Field):
    """
    Will eventually be the "catch-all" text field -- a VARCHAR for
    short strings, and a TEXT etc. for long strings. It will also
    do length validation.
    """
    format = u'%s'
    type = types.UnicodeType
    
    def __init__(self, *args, **kwargs):
        self.length = kwargs.get('length', None)
        Field.__init__(self, **kwargs)
        
    @property
    def field(self):
        if not self.length or self.length > 255:
            return 'TEXT'
        else:
            return 'TINYTEXT'
        
class IntField(Field):
    """
    The Integer Field type -- pretty simple.
    """
    def __init__(self, *args, **kwargs):
        self.unsigned = kwargs.get('unsigned')
        Field.__init__(self, **kwargs)
    
    format = u'%s'
    type = types.LongType
    field = 'INT'
    
    def create_syntax(self):
        sql = 'INT'
        if getattr(self, 'unsigned', False):
            sql += ' UNSIGNED'
        if not getattr(self, 'null', True):
            sql += ' NOT NULL'
        if getattr(self, 'primary', False):
            sql += ' PRIMARY KEY'
        #elif getattr(self, 'foreign_key', False):
        #    sql += ' FOREIGN KEY'
        if getattr(self, 'auto_increment', False):
            sql += ' AUTO_INCREMENT'
        return sql
        
""" For wordier fellows. """
IntegerField = IntField


class TimestampField(Field):
    """
    The TimestampField converts datetime -> timestamp -> datetime.
    Right now, this is mostly designed for the auto-updating
    fields -- would probably work for other datetime purposes though.
    """
    auto_value = True
    
    type = datetime.datetime
    format = u'%s'
    field = 'TIMESTAMP'
    @property
    def value(self):
        if self._value is None:
            return None
        return datetime.datetime.fromtimestamp(self._value)
    
    def set_value(self, value):
        if value is None:
            self._value = None
            return
        elif type(value) is not self.type:
            raise TypeError('Value must be type %s' % self.type)
        ts = time.mktime([i for i in value.timetuple()])
        self._value = ts

""" Auto updating field performs like a normal Timestamp. """
UpdatedField = TimestampField

class CreatedField(TimestampField):
    """
    A one-off TimestampField that shows the time of creation.
    """
    def create_syntax(self):
        sql = TimestampField.create_syntax(self)
        sql += ' DEFAULT NOW()'
        return sql 
    
class PrimaryField(IntField):
    """
    This is the Primary Key Field. Right now, one of these should be on
    every model. That may be taken away later if I'm clever enough.
    """
    auto_value = True

    def __init__(self, *args, **kwargs):
        kwargs['unsigned'] = True
        kwargs['auto_increment'] = True
        kwargs['primary'] = True
        kwargs['key'] = True
        kwargs['null'] = False
        IntField.__init__(self, **kwargs)
        
class ReferenceField(IntField):
    """
    This is the auto-loading (hopefully lazy) foreign model 
    field. When the value attribute is referenced, it 
    calls the fetch_one method on the foreign model.
    """
    def __init__(self, ref_model, *args, **kwargs):
        kwargs['ref_model'] = ref_model
        kwargs['foreign_key'] = True
        kwargs['unsigned'] = True
        args = list(args)
        args.insert(0, ref_model)
        IntField.__init__(self, *args, **kwargs)
    
    def set_value(self, value):
        if type(value) is self.ref_model:
            primary_k = value._primary()
            primary = object.__getattribute__(value, primary_k)
            value = primary.value
        IntField.set_value(self, value)
    
    @property
    def value(self):
        model = self.ref_model
        primary_k = model._primary()
        return model.fetch_one({primary_k:self._value})
