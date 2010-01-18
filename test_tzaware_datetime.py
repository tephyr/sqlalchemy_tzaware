#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
"""tests for sqlalchemy timezone-aware datetime support

Tests:
- test class without db access
- test class within db access
"""
# stdlib
import unittest
import datetime

# 3rd-party
import dateutil

# sqlalchemy
from sqlalchemy import MetaData, Table, Column, DateTime, Unicode, Integer
from sqlalchemy import create_engine
from sqlalchemy.orm import mapper, relation, composite, create_session, clear_mappers
from sqlalchemy.orm import CompositeProperty

# module to test
import tzaware_datetime

class TestBasicClass(unittest.TestCase):
    def test_basic_usage(self):
        """Basic usage"""
        # new empty value
        a = tzaware_datetime.TZAwareDateTime()
        self.assertEqual(a.realdate, None)
        b = tzaware_datetime.TZAwareDateTime()
        self.assertEqual(a.realdate, b.realdate)

    def test_roundtrip_UTC(self):
        """Compare tzutc & .utcdt values"""
        newdate = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.tzutc())
        newtzadt = tzaware_datetime.TZAwareDateTime(utcdt = newdate)
        self.assertEqual(newdate, newtzadt.realdate)
        self.assertEqual(newdate, newtzadt.utcdt)
    
    def test_roundtrip_NZST(self):
        """Roundtrip between New Zealand standard time"""
        newdate = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.tzstr('NZST'))
        newtzadt = tzaware_datetime.TZAwareDateTime(realdate=newdate)
        self.assertEqual(newdate, newtzadt.realdate)
        self.assertEqual(newdate.astimezone(dateutil.tz.tzutc()), newtzadt.utcdt)
    
    def test_roundtrip_PST(self):
        """Roundtrip between Pacific standard time"""
        newdate = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.gettz('PST'))
        newtzadt = tzaware_datetime.TZAwareDateTime(realdate=newdate)
        self.assertEqual(newdate, newtzadt.realdate)
        
    def test_retrieve_all_properties(self):
        """Retrieve all properties when only one is set"""
        newdate = datetime.datetime(2010, 1, 15, 9, 30, tzinfo=dateutil.tz.gettz('EST'))
        newtzadt = tzaware_datetime.TZAwareDateTime()
        self.assertTrue(newtzadt.utcdt == newtzadt.tzname == newtzadt.offsetseconds == newtzadt.realdate == None,
                        'Everything should be None')
        newtzadt.realdate = newdate
        self.assertFalse(newtzadt.utcdt == None)
        self.assertFalse(newtzadt.realdate == None)
        self.assertFalse(newtzadt.offsetseconds == None)
        
        newtzadt_naive = tzaware_datetime.TZAwareDateTime(utcdt=datetime.datetime(2010, 1, 15, 9, 45))
        self.assertFalse(newtzadt_naive.realdate == None, 
                         '.realdate from naive datetime should NOT be None')
        self.assertEqual(newtzadt_naive.realdate, 
                         datetime.datetime(2010, 1, 15, 9, 45, 
                                           tzinfo=dateutil.tz.tzutc()),
                         'Naive datetime should generate UTC timestamp')
    
    def test_compare_dates_from_different_timezones(self):
        """Compare dates from different timezones"""
        newdate_Sydney = datetime.datetime(2010, 1, 15, 8, 
                                           tzinfo=dateutil.tz.gettz('Australia/Sydney'))
        newdate_Rio = datetime.datetime(2010, 1, 15, 8, 
                                        tzinfo=dateutil.tz.gettz('America/Sao_Paulo'))
        newtzadt_Sydney = tzaware_datetime.TZAwareDateTime(realdate=newdate_Sydney)
        newtzadt_Rio = tzaware_datetime.TZAwareDateTime(realdate=newdate_Rio)
        self.assertNotEqual(newtzadt_Sydney.realdate, newtzadt_Rio.realdate)
        self.assertNotEqual(newtzadt_Sydney.utcdt, newtzadt_Rio.utcdt)
        
    def test_compare_offsets(self):
        """Compare offsets from datetime and TZAwareDateTime objects"""
        newdate_a = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.gettz('Europe/Rome'))
        newtzadt_a = tzaware_datetime.TZAwareDateTime(realdate=newdate_a)
        
        # Brazil official time (BRT, aka UTC-3)
        newdate_b = datetime.datetime(2010, 1, 15, 8, 
                                      tzinfo=dateutil.tz.gettz('America/Sao Paulo'))
        newtzadt_b = tzaware_datetime.TZAwareDateTime(realdate=newdate_b)

        # compare offsets
        newdate_a_offset = self.calc_offset(newdate_a.utcoffset())
        self.assertEqual(newdate_a_offset, newtzadt_a.offsetseconds)
        
        self.assertEqual(self.calc_offset(newdate_b.utcoffset()), 
                         newtzadt_b.offsetseconds)
        
    def calc_offset(self, tdelta):
        """properly calculates an offset in seconds (negative when day is 0, positive when day is 1)"""
        if tdelta.days == 0:
            return -tdelta.seconds
        else:
            return 86400 - tdelta.seconds

class TestDatabaseAccess(unittest.TestCase):
    """Test TZAwareDateTime within a sqlalchemy database"""
    def setUp(self):
        """prep db access"""
        # create engine
        self.db_myengine = create_engine('sqlite:///:memory:', echo=False)
        
        # setup table metadata
        self.db_metadata = MetaData()
        table_infomatic = Table('infomatic', self.db_metadata,
                          Column('id', Integer, primary_key=True),
                          Column('info', Unicode(255)),
                          Column('expectedoffset', Integer),
                          Column('utcdate', DateTime), # for tzawaredate
                          Column('tzname', Unicode), # for tzawaredate
                          Column('tzoffset', Integer)) # for tzawaredate
        
        # setup mappings
        mapper(InfoMatic, table_infomatic, properties={
            'info': table_infomatic.c.info,
            'expectedoffset': table_infomatic.c.expectedoffset,
            'tzawaredate': composite(tzaware_datetime.TZAwareDateTime, 
                                     table_infomatic.c.utcdate, 
                                     table_infomatic.c.tzname,
                                     table_infomatic.c.tzoffset)
        })
        
        # create all tables
        self.db_metadata.create_all(self.db_myengine)
        
        # create session
        self.session = create_session(bind=self.db_myengine, autocommit=True, autoflush=True)
    
    def tearDown(self):
        """properly clear sqlalchemy in-memory values"""
        self.session.close()
        #self.db_metadata.clear()
        clear_mappers()
        
    def test_roundtrip_date(self):
        """DB: Roundtrip a simple date"""
        newdate = datetime.datetime(2010, 1, 16, 9, tzinfo=dateutil.tz.tzutc())
        newtzadt = tzaware_datetime.TZAwareDateTime(realdate=newdate)
        infomatic = InfoMatic(u'first date', newtzadt)
        self.session.add(infomatic)
        
        newtzadt_fromdb = self.session.query(InfoMatic).first()
        self.assertEqual(newdate, newtzadt_fromdb.tzawaredate.realdate)
        
    def test_sorting(self):
        """add multiple TZAwareDateTimes, return by UTC order"""
        dates_to_add = {'Rome': [u'Rome (UTC+1)', datetime.datetime(2010, 1, 20, 6,
                                                          tzinfo=dateutil.tz.gettz('Europe/Rome'))], 
                        'London': [u'London (UTC)', datetime.datetime(2010, 1, 20, 6,
                                                           tzinfo=dateutil.tz.gettz('Europe/London'))], 
                        'Toronto': [u'Toronto (UTC-5)', datetime.datetime(2010, 1, 20, 6,
                                                              tzinfo=dateutil.tz.gettz('America/Toronto'))]
                        }
        for k, v in dates_to_add.iteritems():
            self.session.add(InfoMatic(v[0], tzaware_datetime.TZAwareDateTime(realdate=v[1])))
            
        dates_in_order = self.session.query(InfoMatic).order_by(InfoMatic.tzawaredate).all()
        
        # check for all dates
        self.assertTrue(len(dates_in_order) == 3)
        # check for order
        self.assertTrue(dates_in_order[0].tzawaredate.realdate == dates_to_add['Rome'][1])
        self.assertTrue(dates_in_order[2].tzawaredate.realdate == dates_to_add['Toronto'][1])
        
class TestDatabaseSetupHelper(unittest.TestCase):
    """test the TZAwareDateTime sqlalchemy setup helpers"""
    def setUp(self):
        pass
    
    def tearDown(self):
        clear_mappers()

    def test_add_columns(self):
        """Add Column objects"""
        # setup table metadata
        db_metadata = MetaData()
        table_infomatic = Table('infomatic', db_metadata,
                          Column('id', Integer, primary_key=True),
                          Column('info', Unicode(255)),
                          Column('expectedoffset', Integer))
        self.assertTrue(len(table_infomatic.columns) == 3)
        
        # append 3 TZADT columns
        tzaware_datetime.helper.append_columns(table_infomatic, 'newdate')
        
        self.assertTrue(len(table_infomatic.columns) == 6)
        
    def test_add_multiple_columns(self):
        """Add multiple tzadt columns"""
        # setup table metadata
        db_metadata = MetaData()
        table_x = Table('x', db_metadata,
                        Column('id', Integer, primary_key=True),
                        Column('info', Unicode(255)),
                        Column('expectedoffset', Integer))
        self.assertTrue(len(table_x.columns) == 3)
        
        # append 3 TZADT columns
        tzaware_datetime.helper.append_columns(table_x, 'onedate')
        self.assertEqual(len(table_x.columns), 6)
        
        # append 3 more TZADT columns
        tzaware_datetime.helper.append_columns(table_x, 'twodate')
        self.assertEqual(len(table_x.columns), 9)
        
    def test_set_mapping(self):
        """set mappings"""
        # setup table metadata
        db_metadata = MetaData()
        datecolumnname = 'newdate'
        table_infomatic = Table('infomatic', db_metadata,
                          Column('id', Integer, primary_key=True),
                          Column('info', Unicode(255)),
                          Column('expectedoffset', Integer))
        
        # append 3 TZADT columns
        tzaware_datetime.helper.append_columns(table_infomatic, datecolumnname)
        
        # get the mapper dictionary
        column_defintion = tzaware_datetime.helper.get_mapper_definition(table_infomatic, datecolumnname)
        
        self.assertTrue(isinstance(column_defintion, CompositeProperty))
 
    def test_set_mapping_multiple(self):
        """set mappings for multiple TZAwareDateTime columns"""
        # setup table metadata
        db_metadata = MetaData()
        datecolumnnames = ('reddate', 'bluedate')
        table_infomatic = Table('infomatic', db_metadata,
                          Column('id', Integer, primary_key=True),
                          Column('info', Unicode(255)),
                          Column('expectedoffset', Integer))
        
        # append 2 sets of TZADT columns
        tzaware_datetime.helper.append_columns(table_infomatic, datecolumnnames[0])
        tzaware_datetime.helper.append_columns(table_infomatic, datecolumnnames[1])
        
        # get the mapper dictionaries
        for datecolumnname in datecolumnnames:
            columndefinition = tzaware_datetime.helper.get_mapper_definition(table_infomatic, datecolumnname)
            self.assertTrue(isinstance(columndefinition, CompositeProperty))

    def test_whole_enchilada(self):
        """test entire database"""
        # create engine
        db_myengine = create_engine('sqlite:///:memory:', echo=False)
        
        # setup table metadata
        db_metadata = MetaData()
        table_infomatic = Table('infomatic', db_metadata,
                          Column('id', Integer, primary_key=True),
                          Column('info', Unicode(255)),
                          Column('expectedoffset', Integer))
        tzadtcolumnname = 'thedate'
        tzaware_datetime.helper.append_columns(table_infomatic, tzadtcolumnname)
        
        # setup mappings
        mapper(InfoMatic, table_infomatic, properties={
            'info': table_infomatic.c.info,
            'expectedoffset': table_infomatic.c.expectedoffset,
            tzadtcolumnname: tzaware_datetime.helper.get_mapper_definition(table_infomatic, tzadtcolumnname)
        })
        
        # create all tables
        db_metadata.create_all(db_myengine)
        
        # create session
        session = create_session(bind=db_myengine, autocommit=True, autoflush=True)
    
        # properly clear sqlalchemy in-memory values
        session.close()
        
class InfoMatic(object):
    """table to hold TZAwareDateTime values"""
    def __init__(self, info, tzawaredate):
        self.info = info
        self.tzawaredate = tzawaredate
    def __repr__(self):
        return "<InfoMatic('%s', %s)" % (self.info, self.tzawaredate)

def run_all_tests():
    # create a complete suite of tests
    allsuites = unittest.TestSuite()

    # add all TestCase subclasses
    allsuites.addTests(unittest.TestLoader().loadTestsFromTestCase(TestBasicClass))
    allsuites.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDatabaseAccess))
    allsuites.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDatabaseSetupHelper))
    unittest.TextTestRunner(verbosity=2).run(allsuites)

if __name__ == '__main__':
    run_all_tests()
