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

    def test_roundtripUTC(self):
        """Compare tzutc & .utcdt values"""
        newdate = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.tzutc())
        newtzadt = tzaware_datetime.TZAwareDateTime(utcdt = newdate)
        self.assertEqual(newdate, newtzadt.realdate)
        self.assertEqual(newdate, newtzadt.utcdt)
    
    def test_roundtripNZST(self):
        """Roundtrip between New Zealand standard time"""
        newdate = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.tzstr('NZST'))
        newtzadt = tzaware_datetime.TZAwareDateTime(realdate=newdate)
        self.assertEqual(newdate, newtzadt.realdate)
        self.assertEqual(newdate.astimezone(dateutil.tz.tzutc()), newtzadt.utcdt)
    
    def test_roundtripPST(self):
        """Roundtrip between Pacific standard time"""
        newdate = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.gettz('PST'))
        newtzadt = tzaware_datetime.TZAwareDateTime(realdate=newdate)
        self.assertEqual(newdate, newtzadt.realdate)
    
    def test_compareDatesFromDifferentTimeZones(self):
        """Compare dates from different timezones"""
        newdate_Sydney = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.gettz('Australia/Sydney'))
        newdate_Rio = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.gettz('America/Sao_Paulo'))
        newtzadt_Sydney = tzaware_datetime.TZAwareDateTime(realdate=newdate_Sydney)
        newtzadt_Rio = tzaware_datetime.TZAwareDateTime(realdate=newdate_Rio)
        self.assertNotEqual(newtzadt_Sydney.realdate, newtzadt_Rio.realdate)
        self.assertNotEqual(newtzadt_Sydney.utcdt, newtzadt_Rio.utcdt)
        
    def test_compareOffsets(self):
        """Compare offsets from datetime and TZAwareDateTime objects"""
        newdate_a = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.gettz('Europe/Rome'))
        newtzadt_a = tzaware_datetime.TZAwareDateTime(realdate=newdate_a)
        
        # Brazil official time (BRT, aka UTC-3)
        newdate_b = datetime.datetime(2010, 1, 15, 8, tzinfo=dateutil.tz.gettz('America/Sao Paulo'))
        newtzadt_b = tzaware_datetime.TZAwareDateTime(realdate=newdate_b)

        # compare offsets
        newdate_a_offset = self.calc_offset(newdate_a.utcoffset())
        self.assertEqual(newdate_a_offset, newtzadt_a.offsetseconds)
        
        self.assertEqual(self.calc_offset(newdate_b.utcoffset()), newtzadt_b.offsetseconds)
        
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
    unittest.TextTestRunner(verbosity=2).run(allsuites)

if __name__ == '__main__':
    run_all_tests()