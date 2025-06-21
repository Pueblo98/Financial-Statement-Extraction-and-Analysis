"""
Filing Manager - Orchestrates filing retrieval and storage
Combines EDGAR client and storage system for complete filing management
"""

import os
import requests
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import logging
from datetime import datetime

from .edgar_client import SECEdgarClient, CompanyLookup, Filing
from .filing_storage import FilingStorage, StoredFiling

@dataclass
class FilingRetrievalResult:
    """Results of filing retrieval operation"""
    company_info: Dict[str, Any]
    requested_filings: int
    new_downloads: int
    already_stored: int
    failed_downloads: int
    stored_filings: List[StoredFiling]
    errors: List[str]

class FilingManager:
    """
    Main class for managing SEC filing retrieval and storage
    Coordinates between EDGAR client and filing storage
    """
    
    def __init__(self, user_agent: str = "Financial Analysis Tool contact@example.com",
                 storage_path: str = "./data/filings"):
        """
        Initialize filing manager
        
        Args:
            user_agent: User agent string for SEC compliance
            storage_path: Path for filing storage
        """
        self.edgar_client = SECEdgarClient(user_agent)
        self.company_lookup = CompanyLookup(self.edgar_client)
        self.storage = FilingStorage(storage_path)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def retrieve_company_filings(self, identifier: str, years: int = 10, 
                               form_type: str = "10-K", 
                               force_redownload: bool = False) -> FilingRetrievalResult:
        """
        Retrieve and store company filings
        
        Args:
            identifier: Company ticker symbol or CIK
            years: Number of years of filings to retrieve
            form_type: Type of filing (default: 10-K)
            force_redownload: Whether to redownload existing filings
            
        Returns:
            FilingRetrievalResult with details of retrieval operation
        """
        errors = []
        
        # Resolve company identifier to CIK
        if len(identifier) <= 5 and identifier.isalpha():
            # Assume it's a ticker symbol
            cik = self.company_lookup.get_cik_by_ticker(identifier)
            if not cik:
                error_msg = f"Could not find CIK for ticker: {identifier}"
                self.logger.error(error_msg)
                return FilingRetrievalResult(
                    company_info={},
                    requested_filings=0,
                    new_downloads=0,
                    already_stored=0,
                    failed_downloads=0,
                    stored_filings=[],
                    errors=[error_msg]
                )
        else:
            # Assume it's a CIK
            cik = identifier.zfill(10)
        
        # Get company information
        try:
            company_info = self.company_lookup.get_company_info(cik)
            self.logger.info(f"Processing filings for {company_info['name']} (CIK: {cik})")
        except Exception as e:
            error_msg = f"Failed to get company info for CIK {cik}: {e}"
            errors.append(error_msg)
            self.logger.error(error_msg)
            return FilingRetrievalResult(
                company_info={'cik': cik},
                requested_filings=0,
                new_downloads=0,
                already_stored=0,
                failed_downloads=0,
                stored_filings=[],
                errors=errors
            )
        
        # Get filing list
        try:
            if form_type == "10-K":
                filings = self.edgar_client.get_10k_filings(cik, years)
            else:
                # For future expansion to other form types
                submissions = self.edgar_client.get_company_submissions(cik)
                filings = self._extract_filings_by_form(submissions, form_type, years)
        except Exception as e:
            error_msg = f"Failed to get filings list: {e}"
            errors.append(error_msg)
            self.logger.error(error_msg)
            return FilingRetrievalResult(
                company_info=company_info,
                requested_filings=0,
                new_downloads=0,
                already_stored=0,
                failed_downloads=0,
                stored_filings=[],
                errors=errors
            )
        
        # Process each filing
        new_downloads = 0
        already_stored = 0
        failed_downloads = 0
        stored_filings = []
        
        for filing in filings:
            try:
                # Check if already stored
                if not force_redownload and self.storage.is_filing_stored(filing.accession_number):
                    self.logger.info(f"Filing {filing.accession_number} already stored, skipping")
                    already_stored += 1
                    
                    # Get stored filing info
                    existing_filings = self.storage.get_stored_filings(
                        cik=cik, 
                        form=form_type
                    )
                    for stored_filing in existing_filings:
                        if stored_filing.accession_number == filing.accession_number:
                            stored_filings.append(stored_filing)
                            break
                    continue
                
                # Download filing
                self.logger.info(f"Downloading filing {filing.accession_number} from {filing.filing_date}")
                
                # Get filing content
                filing_content = self._download_filing_content(filing)
                
                # Store filing
                filing_data = {
                    'accession_number': filing.accession_number,
                    'filing_date': filing.filing_date,
                    'report_date': filing.report_date,
                    'form': filing.form,
                    'primary_document': filing.primary_document
                }
                
                stored_filing = self.storage.store_filing(
                    filing_data=filing_data,
                    file_content=filing_content,
                    company_info=company_info
                )
                
                stored_filings.append(stored_filing)
                new_downloads += 1
                
            except Exception as e:
                error_msg = f"Failed to download filing {filing.accession_number}: {e}"
                errors.append(error_msg)
                self.logger.error(error_msg)
                failed_downloads += 1
        
        # Create result
        result = FilingRetrievalResult(
            company_info=company_info,
            requested_filings=len(filings),
            new_downloads=new_downloads,
            already_stored=already_stored,
            failed_downloads=failed_downloads,
            stored_filings=stored_filings,
            errors=errors
        )
        
        self.logger.info(f"Filing retrieval complete: {new_downloads} new, {already_stored} existing, {failed_downloads} failed")
        return result
    
    def _download_filing_content(self, filing: Filing) -> bytes:
        """Download filing content from EDGAR"""
        # Construct filing URL using the correct format
        accession_clean = filing.accession_number.replace('-', '')
        
        # Extract CIK from accession number (first 10 digits) and remove leading zeros
        cik_from_accession = accession_clean[:10].lstrip('0') or '0'
        
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_from_accession}/{accession_clean}/{filing.primary_document}"
        
        # Rate limit the request
        self.edgar_client._rate_limit_request()
        
        response = self.edgar_client.session.get(filing_url, timeout=60)
        response.raise_for_status()
        
        return response.content
    
    def _extract_filings_by_form(self, submissions: Dict[str, Any], 
                                form_type: str, years: int) -> List[Filing]:
        """Extract filings of specific form type from submissions data"""
        recent_filings = submissions.get('filings', {}).get('recent', {})
        
        if not recent_filings:
            return []
        
        forms = recent_filings.get('form', [])
        filing_dates = recent_filings.get('filingDate', [])
        report_dates = recent_filings.get('reportDate', [])
        accession_numbers = recent_filings.get('accessionNumber', [])
        file_numbers = recent_filings.get('fileNumber', [])
        primary_documents = recent_filings.get('primaryDocument', [])
        primary_doc_descriptions = recent_filings.get('primaryDocDescription', [])
        sizes = recent_filings.get('size', [])
        
        filings = []
        cutoff_year = datetime.now().year - years
        
        for i, form in enumerate(forms):
            if form == form_type:
                filing_date = filing_dates[i]
                if datetime.strptime(filing_date, '%Y-%m-%d').year >= cutoff_year:
                    filing = Filing(
                        accession_number=accession_numbers[i],
                        filing_date=filing_date,
                        report_date=report_dates[i],
                        form=form,
                        file_number=file_numbers[i],
                        size=sizes[i],
                        primary_document=primary_documents[i],
                        primary_doc_description=primary_doc_descriptions[i]
                    )
                    filings.append(filing)
        
        return filings
    
    def get_company_filings_summary(self, identifier: str) -> Dict[str, Any]:
        """Get summary of stored filings for a company"""
        # Resolve identifier to CIK
        if len(identifier) <= 5 and identifier.isalpha():
            cik = self.company_lookup.get_cik_by_ticker(identifier)
        else:
            cik = identifier.zfill(10)
        
        if not cik:
            return {'error': f'Could not resolve identifier: {identifier}'}
        
        # Get company info
        try:
            company_info = self.company_lookup.get_company_info(cik)
        except Exception as e:
            return {'error': f'Failed to get company info: {e}'}
        
        # Get stored filings
        stored_filings = self.storage.get_stored_filings(cik=cik)
        
        # Organize by form type
        by_form = {}
        for filing in stored_filings:
            form = filing.form
            if form not in by_form:
                by_form[form] = []
            by_form[form].append({
                'accession_number': filing.accession_number,
                'filing_date': filing.filing_date,
                'report_date': filing.report_date,
                'file_size_mb': round(filing.file_size / (1024 * 1024), 2)
            })
        
        # Sort by filing date
        for form in by_form:
            by_form[form].sort(key=lambda x: x['filing_date'], reverse=True)
        
        return {
            'company_info': company_info,
            'total_filings': len(stored_filings),
            'by_form': by_form,
            'date_range': {
                'earliest': min(f.filing_date for f in stored_filings) if stored_filings else None,
                'latest': max(f.filing_date for f in stored_filings) if stored_filings else None
            }
        }
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get overall storage statistics"""
        return self.storage.get_storage_stats()


if __name__ == "__main__":
    # Example usage
    manager = FilingManager()
    
    # Retrieve Apple's 10-K filings for the last 10 years
    print("Retrieving Apple's 10-K filings...")
    result = manager.retrieve_company_filings("AAPL", years=10, form_type="10-K")
    
    print(f"\nRetrieval Results:")
    print(f"Company: {result.company_info.get('name', 'Unknown')}")
    print(f"Requested filings: {result.requested_filings}")
    print(f"New downloads: {result.new_downloads}")
    print(f"Already stored: {result.already_stored}")
    print(f"Failed downloads: {result.failed_downloads}")
    
    if result.errors:
        print(f"Errors: {result.errors}")
    
    # Get storage statistics
    print(f"\nStorage Statistics:")
    stats = manager.get_storage_stats()
    print(f"Total filings: {stats.get('total_filings', 0)}")
    print(f"Total size: {stats.get('total_size_mb', 0)} MB")