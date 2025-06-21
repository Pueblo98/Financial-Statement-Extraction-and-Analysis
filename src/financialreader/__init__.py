"""
Financial Statement Reader - EDGAR Data Extraction and Analysis Pipeline
"""

__version__ = "0.1.0"
__author__ = "Financial Analysis Team"

from .edgar_client import SECEdgarClient, CompanyLookup
from .filing_storage import FilingStorage
from .filing_manager import FilingManager
from .gaap_taxonomy import GAAP_MAPPER, GAAPTaxonomyMapper
from .xbrl_parser import XBRLFinancialParser
from .data_validator import FinancialDataValidator

__all__ = [
    'SECEdgarClient',
    'CompanyLookup', 
    'FilingStorage',
    'FilingManager',
    'GAAP_MAPPER',
    'GAAPTaxonomyMapper',
    'XBRLFinancialParser',
    'FinancialDataValidator'
]