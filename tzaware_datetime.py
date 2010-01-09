#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
"""sqlalchemy timezone-aware datetime support

Usage:
  Add the following columns to the table definition:
    Column('utcdate', DateTime),
    Column('tzname', Unicode),
    Column('tzoffset', Integer))

  In the mapper, add the following key:
  'tzawaredate': composite(TZAwareDateTime, 
                            thetable.c.utcdate, 
                            thetable.c.tzname,
                            thetable.c.tzoffset)
                            
  The columns can be named anything, but they must exist with those types and be reference in that order.
"""
__version_info__ = ('0', '4', '0')
__version__ = '.'.join(__version_info__)
__author__ = 'Andrew Ittner <projects@rhymingpanda.com>'
# stdlib
from datetime import datetime, timedelta

# 3rd-party: dateutil <http://labix.org/python-dateutil>
from dateutil import tz

class TZAwareDateTime(object):
    """A composite sqlalchemy column that round-trips timezone-aware datetime objects"""
    def __init__(self, utcdt=None, tzname=None, offsetseconds=None, realdate=None):
        """utcdt: UTC datetime
        tzname: human-readable timezone name
        offsetseconds: seconds between local date and UTC
        realdate: actual date in target timezone
        """
        if (realdate is None):
            self.utcdt = utcdt
            self.tzname = tzname
            self.offsetseconds = offsetseconds
        else:
            self._set_realdate(realdate)
            
    def __repr__(self):
        return "<TZDateTime (%s, offset=%s)>" % (self.realdate, self.offsetseconds)
        
    def __composite_values__(self):
        return [self.utcdt, self.tzname, self.offsetseconds]

    def __set_composite_values__(self, utcdt, tzname, offsetseconds):
        self.utcdt = utcdt
        self.tzname = tzname
        self.offsetseconds = offsetseconds
        
    def __eq__(self, other):
        return other.utcdt == self.utcdt
    
    def __ne__(self, other):
        return not self.__eq__(other)

    def _calc_offset_seconds(self, tdelta):
        """calculate timedelta seconds based on day value"""
        assert isinstance(tdelta, timedelta)
        return (-tdelta.days * 86400) - tdelta.seconds
    
    def _get_realdate(self):
        """reconstruct timezone-aware date from 3 columns"""
        tz_reconstitute = None
        # use offset from UTC (timezone name not guaranteed for roundtrip)
        if self.offsetseconds is None:
            # return date as UTC
            return self.utcdt
        else:
            tz_reconstitute = tz.tzoffset(name=None, offset=self.offsetseconds)
            
        return self.utcdt.astimezone(tz_reconstitute)
    
    def _set_realdate(self, newdate):
        """use a single datetime with a timezone to set class values"""
        # convert to utc
        self.utcdt = newdate.astimezone(tz.tzutc())

        # get timezone name
        newtzname = newdate.tzname()
        if newtzname is not None:
            self.tzname = unicode(newtzname)

        # set offset
        self.offsetseconds = self._calc_offset_seconds(newdate.utcoffset())
    
    realdate = property(_get_realdate, _set_realdate)
