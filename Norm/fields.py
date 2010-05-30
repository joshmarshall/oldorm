"""
NORM fields.py

Josh Marshall 2010
This file (will) contain the various Field classes.

TODO: Add FloatField, BoolField, etc.
"""

import types
import datetime
import time
try:
    import json
except ImportError:
    import simpejson as json
    
class LateProperty(object):
    
    def __init__(self, getter=None, setter=None, deleter=None):
        self.getter = getter
        self.setter = setter
        self.deleter = deleter
    
    def __get__(self, inst, owner):
        if inst != None:
            return getattr(inst, self.getter.__name__)()
        else:
            return getattr(owner, self.getter.__name__)()
       
    def __set__(self, inst, value):
        inst._updated = True
        return getattr(inst, self.setter.__name__)(value)
        
    def __del__(self, inst):
        return getattr(inst, self.deleter.__name__)()

class Field(object):
    """
    The base field class, not to be used directly.
    """
    format = u'%s'
    field = None
    type = None
    # Auto value is just a flag right now for IDs and TIMESTAMPS
    auto_value = False
    default = None
    _value = None
    _updated = False
    
    def __init__(self, *args, **kwargs):
        """
        Applies parameters from the arguments, and
        saves the arguments for instance copying.
        """
        for k,v in kwargs.iteritems():
            setattr(self, k, v)
        if kwargs.has_key('default'):
            self.set_value(kwargs['default'])
        self.args = args
        self.kwargs = kwargs
        
    def get_value(self):
        """
        This is the getter property so that sub-classes can do
        other fun things with it.
        """
        return self._value
        
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
        
    value = LateProperty(get_value, set_value)
        
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
        if getattr(self, 'unique', False) and not getattr(self, 'index', False):
            sql += ' UNIQUE'
        if not getattr(self, 'null', True):
            sql += ' NOT NULL'
        return sql
        
    
class UnicodeField(Field):
    """
    Will eventually be the "catch-all" text field -- a VARCHAR for
    short strings, and a TEXT etc. for long strings. It will also
    do length validation.
    """
    type = types.UnicodeType
    
    def __init__(self, *args, **kwargs):
        self.length = kwargs.get('length', None)
        Field.__init__(self, **kwargs)
        
    def set_value(self, value):
        if type(value) is types.StringType:
            # Dangerous assumption?
            value = unicode(value)
        Field.set_value(self, value)
        
    @property
    def field(self):
        if not self.length:
            return u'TEXT' 
        elif self.length > 255:
            return u'TEXT(%d)' % self.length
        else:
            return 'VARCHAR(%d)' % self.length
        
class DictField(UnicodeField):
    """
    Stores a Dict in JSON format.
    """
    type = types.DictType
    
    def set_value(self, value):
        if value == None: 
            if not getattr(self, 'null', True):
                raise TypeError('Column does not allow null values.')
        elif type(value) != self.type:
            raise TypeError('Value must be of type %s' % self.type)
        json_string = json.dumps(value)
        self._value = json_string
        
    def get_value(self):
        if self._value == None:
            return None
        obj = self._value
        while type(obj) != self.type:
            """ Doing this because of MySQL escaping?? """
            obj = json.loads(obj)
        return obj
            
class ListField(DictField):
    """
    Stores a List in JSON format.
    """
    type = types.ListType
    
    def set_value(self, value):
        if type(value) is tuple:
            value = list(value)
        DictField.set_value(self, value)
        
class FloatField(Field):
    """
    The Float Field type
    """
    type = types.FloatType
    field = 'FLOAT'

class IntField(Field):
    """
    The Integer Field type -- pretty simple.
    """
    def __init__(self, *args, **kwargs):
        self.unsigned = kwargs.get('unsigned')
        Field.__init__(self, **kwargs)
    
    type = types.LongType
    field = 'INT'
    
    def set_value(self, value):
        if type(value) is types.IntType:
            value = long(value)
        Field.set_value(self, value)
    
    def create_syntax(self):
        sql = 'INT'
        if getattr(self, 'unsigned', False):
            sql += ' UNSIGNED'
        if not getattr(self, 'null', True):
            sql += ' NOT NULL'
        if getattr(self, 'primary', False):
            sql += ' PRIMARY KEY'
        if getattr(self, 'unique', False) and not getattr(self, 'index', False):
            sql += ' UNIQUE'
        #elif getattr(self, 'foreign_key', False):
        #    sql += ' FOREIGN KEY'
        if getattr(self, 'auto_increment', False):
            sql += ' AUTO_INCREMENT'
        return sql

class BoolField(IntField):
    
    type = types.BooleanType

    def set_value(self, value):
        if value is None:
            if not getattr(self, 'null', True):
                raise ValueError('This field does not accept null values.')
            self._value = None
        elif value:
            self._value = 1
        else:
            self._value = 0

    def get_value(self):
        value = self._value
        if type(value) != types.IntType:
            value = int(value)
        if value > 0:
            return True
        else:
            return False

    def create_syntax(self):
        sql = 'TINYINT(1) UNSIGNED'
        if not getattr(self, 'null', True):
            sql += ' NOT NULL'
        if getattr(self, 'unique', False) and not getattr(self, 'index', False):
            sql += ' UNIQUE'
        return sql

""" For wordier fellows. """
IntegerField = IntField
BooleanField = BoolField

class TimestampField(Field):
    """
    The TimestampField converts datetime -> timestamp -> datetime.
    Right now, this is mostly designed for the auto-updating
    fields -- would probably work for other datetime purposes though.
    """
    auto_value = True
    
    type = datetime.datetime
    field = 'TIMESTAMP'

""" Auto updating field performs like a normal Timestamp. """
UpdatedField = TimestampField

class CreatedField(TimestampField):
    """
    A one-off TimestampField that shows the time of creation.
    """
    def create_syntax(self):
        sql = TimestampField.create_syntax(self)
        if getattr(self, 'unique', False) and not getattr(self, 'index', False):
            sql += ' UNIQUE'
        sql += ' DEFAULT NOW()'
        return sql
    
class PrimaryField(IntField):
    """
    This is the Primary Key Field. Right now, one of these should be on
    every model. That may be taken away later if I'm clever enough.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('unsigned', True)
        kwargs.setdefault('auto_increment', True)
        kwargs.setdefault('primary', True)
        kwargs.setdefault('key', True)
        kwargs.setdefault('null', False)
        # Keys cannot be / already are indexed.
        if kwargs.has_key('index'):
            del kwargs['index']
            
        """
        If the key is not an auto_increment key, then
        we set the auto_value to False so that it is
        INSERTed and UPDATEd.
        """
        self.auto_value = kwargs['auto_increment']
            
        IntField.__init__(self, *args, **kwargs)
        
    def set_value(self, value):
        if type(value) is types.IntType:
            value = long(value)
        return IntField.set_value(self, value)
        
class ReferenceField(IntField):
    """
    This is the auto-loading (hopefully lazy) foreign model 
    field. When the value attribute is referenced, it 
    calls the get method on the foreign model.
    """
    def __init__(self, ref_model, *args, **kwargs):
        kwargs['ref_model'] = ref_model
        kwargs['foreign_key'] = True
        kwargs['unsigned'] = True
        args = list(args)
        args.insert(0, ref_model)
        IntField.__init__(self, *args, **kwargs)
    
    def set_value(self, value):
        if value.__class__ is self.ref_model:
            value = value.primary
        IntField.set_value(self, value)
    
    def get_value(self):
        model = self.ref_model
        if self._value == None:
            return None
        return model.get(self._value)

       
class ReferenceManyField(object):
    """
    This is the object that automatically selects a list
    of objects that Reference this table.
    """

    model = None
    ref_field = None
    many_field = None
    _value = None
  
  
    def __init__(self, ref_table, *args, **kwargs):
        kwargs['ref_table'] = ref_table
        self.ref_table = ref_table
        self.args = args
        self.kwargs = kwargs
        
    @property
    def value(self):
        if not self._value:
            self.get_foreign()
            self._value = self.ref_table.where(
                {self.ref_field:self.model.primary}
            )
        return self._value
  
    def get_foreign(self):
        table_instance = self.ref_table()
        for f in table_instance.fields():
            attr = object.__getattribute__(table_instance, f)
            if type(attr) is ReferenceField: 
                if type(self.model) == attr.ref_model:
                    self.ref_field = f
                    break
                
class ReferenceManyToManyField(ReferenceManyField):
    
    join_table = None
    join_field = None
    
    def __init__(self, ref_table, join_table=None, *args, **kwargs):
        self.join_table = join_table
        kwargs['join_table'] = join_table
        ReferenceManyField.__init__(self, ref_table, *args, **kwargs)
        
    @property
    def value(self):
        if not self._value:
            self.get_foreign()
            self._value = self.join_table.where({
                self.ref_field: self.model.primary,
                self.join_table.get_primary(): self.join_field
            })
        return self._value
            
    def get_foreign(self):
        join_tables = {}
        table_instance = self.ref_table()
        for f in table_instance.fields():
            attr = object.__getattribute__(table_instance, f)
            if type(attr) is ReferenceField:
                if type(self.model) == attr.ref_model:
                    self.ref_field = attr
                elif self.join_table:
                    if attr.ref_model == self.join_table:
                        self.join_field = attr
                else:
                    join_tables[attr.ref_model] = attr
        if self.join_table and self.join_field:
            return
        if len(join_tables) != 1:
            raise Exception('Could not find / limit to one ReferenceField.')
        self.join_table = join_tables.keys()[0]
        self.join_field = join_tables.values()[0]
