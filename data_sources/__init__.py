"""
Data Sources Packages

This package provides a unified interface for downloading
environmental data from multiple sources.

Usage:
    from data)sources import USGSDataSource, Aquarius DataSource

    usgs = USGSDataSource()
    sites = usgs.get_sites(state='OR')
"""

from .base import BaseDataSource, DataPoint, SiteInfo
from .aquarius import AquariusDataSource

__all__ = [
    'BaseDataSource',
    'DataPoint',
    'SiteInfo',

    # Data sources
    'AquariusDataSource', 
    # 'USGSDataSource', # uncomment when written
]
