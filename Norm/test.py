"""
NORM connection.py

Josh Marshall 2010
This file is just some simple CRUD tests. Run like so:

    python test.py
    
Or, to print out the SQL statements:

    python test.py -v

"""

from Norm.model import Model
from Norm.fields import PrimaryField, UnicodeField, ReferenceField
from Norm.fields import BoolField, CreatedField, TimestampField
from Norm.fields import DictField, IntField, FloatField
from Norm.connection import connection
import time

class State(Model):
    """ Test State model"""
    id = PrimaryField()
    name = UnicodeField()
    
class City(Model):
    """ Test City model """
    id = PrimaryField()
    name = UnicodeField(length=300, index=10)
    state = ReferenceField(State)
    landlocked = BoolField()
    
class Person(Model):
    """ Test Person model """
    id = PrimaryField()
    name = UnicodeField(index=5, length=200)
    city = ReferenceField(City)
    created = CreatedField()
    updated = TimestampField()
    address = DictField()
    email = UnicodeField(length=100, unique=True)
    age = IntField(index=True, default=40)
    wage = FloatField(default=3.95)

STATE = State(name=u'Texas')
CITY = City(name=u'Austin')
CITY2 = City(name=u'Houston')

def run_test(func):
    """ Times the passed test function. """
    start_message = 'Starting Task %s' % func.__name__
    print start_message
    print '='*len(start_message)
    start = time.time()
    func()
    end = time.time()
    print 'Task finished in %s seconds' % (end-start)
    print
  
def create_tables():
    """ Setup the tables in the DB. """
    Person.create_table()
    State.create_table()
    City.create_table()
    
def add_city():
    """ Save the basic city and state objects. """
    STATE.save()
    CITY.state = STATE
    CITY.save()
    CITY2.state = STATE
    CITY2.save()
    
def add_user():
    """ Insert a single user into the database. """
    wilbur = Person(name=u'Wilbur', city=CITY)
    wilbur.age = 21
    wilbur.wage = 9.46
    wilbur.address = {
        'address':'200 W. Main', 
        'city':'Austin', 
        'state':'Texas'
    }
    wilbur.save()
    
def add_users():
    """ Insert a bunch of users into the database. """
    users = 0
    for i in range(1, 2001):
        user = Person(name=u'%d' % i , city=CITY)
        user.address = {
            'address':'200 W. Main', 
            'city':'Austin', 
            'state':'Texas'
        }
        user.save()
        users += 1
    print '%s user(s) added.' % users

def get_user():
    """ Get a single user from the database. """
    wilbur = Person.fetch_one({'name':u'Wilbur'})
    if not wilbur:
        print 'No result.'
        return
    for field in Person.fields():
        print '%s: %s' % (field, getattr(wilbur, field))
    
def get_users():
    """ Get a bunch of users from the database. """
    people = Person.all().reverse()[5:20]
    print '%s people found.' % len(people)
    
def update_user():
    """ Update a single user in the database. """
    wilbur = Person.fetch_one({'name':u'Wilbur'})
    wilbur.name = u'Wilburt'
    address = wilbur.address
    address['city'] = 'Houston'
    wilbur.address = address
    wilbur.city = CITY2
    wilbur.save()
    
def update_users():
    """ Update a bunch of users in the database. """
    print len(Person.where({'city':CITY}).update({'city':CITY2}).run())
    
def compare_users():
    """ Verify that the equivalent operations work. """
    wilbur = Person.fetch_one({'name':u'Wilburt'})
    other = Person.fetch_one({'name':u'%s' % 2})
    assert wilbur != other
    assert wilbur == wilbur
    
def delete_user():
    """ Delete a single user. """
    wilbur = Person.fetch_one({'name':u'Wilburt'})
    wilbur.delete()
    
def delete_users():
    """ Delete a bunch of users. """
    print len(Person.where({'city':CITY}).delete().run())
    
def delete_tables():
    """ Drop all tables. """
    Person.drop_table()
    State.drop_table()
    City.drop_table()
  
def test(verbose=False):
    """ Connect to a local database and run all the tests. """
    connection.connect('mysql://test:test@localhost/test', verbose=verbose)
    delete_tables()
    for test_func in [
        create_tables, add_city, add_user,
        add_users, get_user, get_users, 
        update_user, update_users, compare_users, 
        delete_user, delete_users, delete_tables
    ]:
        run_test(test_func)
    print 'Finished running tests.'

if __name__ == '__main__':
    import sys
    VERBOSE = False
    if len(sys.argv) > 1 and sys.argv[1] == '-v':
        VERBOSE = True
    test(VERBOSE)
