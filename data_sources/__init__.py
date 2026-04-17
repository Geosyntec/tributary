"""
Data Sources Packages

This package provides a unified interface for downloading
environmental data from multiple sources.

Usage:
    from data)sources import USGSDataSource, Aquarius DataSource

    usgs = USGSDataSource()
    sites = usgs.get_sites(state='OR')
"""
# Import base classes
from .base import BaseDataSource, DataPoint, SiteInfo

# Import data sources
from .aquarius import AquariusDataSource
from .usgs import USGSDataSource

__all__ = [
    'BaseDataSource',
    'DataPoint',
    'SiteInfo',

    # Data sources
    'AquariusDataSource', 
    'USGSDataSource',
]
