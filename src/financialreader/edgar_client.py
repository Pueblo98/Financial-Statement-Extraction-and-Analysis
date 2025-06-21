"""
SEC EDGAR API Client
Handles data acquisition from SEC EDGAR database with proper rate limiting
"""

import requests
import time
import json
import os
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin
import logging
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Filing:
    """Represents a single SEC filing"""
    accession_number: str
    filing_date: str
    report_date: str
    form: str
    file_number: str
    size: int
    primary_document: str
    primary_doc_description: str

class SECEdgarClient:
    """
    Client for accessing SEC EDGAR data via official APIs
    Implements proper rate limiting (max 10 requests per second)
    """
    
    BASE_URL = "https://data.sec.gov"
    SUBMISSIONS_ENDPOINT = "/submissions/CIK{cik}.json"
    COMPANY_FACTS_ENDPOINT = "/api/xbrl/companyfacts/CIK{cik}.json"
    COMPANY_CONCEPT_ENDPOINT = "/api/xbrl/companyconcept/CIK{cik}/{taxonomy}/{tag}.json"
    
    def __init__(self, user_agent: str = "Financial Analysis Tool contact@example.com", 
                 requests_per_second: float = 9.0):
        """
        Initialize EDGAR client
        
        Args:
            user_agent: Required user agent string for SEC compliance
            requests_per_second: Rate limit (max 10, using 9 for safety)
        """
        self.user_agent = user_agent
        self.rate_limit = requests_per_second
        self.last_request_time = 0
        
        # Setup session with proper headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def _rate_limit_request(self):
        """Implement rate limiting to comply with SEC 10 requests/second limit"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        min_interval = 1.0 / self.rate_limit
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, url: str) -> Dict[str, Any]:
        """
        Make rate-limited request to SEC API
        
        Args:
            url: Full URL to request
            
        Returns:
            JSON response as dictionary
        """
        self._rate_limit_request()
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON response from {url}: {e}")
            raise
    
    def get_company_submissions(self, cik: str) -> Dict[str, Any]:
        """
        Get all submissions for a company by CIK
        
        Args:
            cik: 10-digit Central Index Key (with leading zeros)
            
        Returns:
            Complete submissions data including filing history
        """
        # Ensure CIK is properly formatted (10 digits with leading zeros)
        cik_formatted = str(cik).zfill(10)
        url = urljoin(self.BASE_URL, self.SUBMISSIONS_ENDPOINT.format(cik=cik_formatted))
        
        self.logger.info(f"Fetching submissions for CIK {cik_formatted}")
        return self._make_request(url)
    
    def get_company_facts(self, cik: str) -> Dict[str, Any]:
        """
        Get all company facts (XBRL financial data) for a company
        
        Args:
            cik: 10-digit Central Index Key (with leading zeros)
            
        Returns:
            Complete XBRL facts data including all financial metrics
        """
        cik_formatted = str(cik).zfill(10)
        url = urljoin(self.BASE_URL, self.COMPANY_FACTS_ENDPOINT.format(cik=cik_formatted))
        
        self.logger.info(f"Fetching company facts for CIK {cik_formatted}")
        return self._make_request(url)
    
    def get_10k_filings(self, cik: str, years: int = 10) -> List[Filing]:
        """
        Get 10-K filings for specified number of years
        
        Args:
            cik: Central Index Key
            years: Number of years of filings to retrieve
            
        Returns:
            List of Filing objects for 10-K forms
        """
        submissions = self.get_company_submissions(cik)
        
        # Extract filings data
        recent_filings = submissions.get('filings', {}).get('recent', {})
        
        if not recent_filings:
            self.logger.warning(f"No recent filings found for CIK {cik}")
            return []
        
        # Find 10-K filings
        forms = recent_filings.get('form', [])
        filing_dates = recent_filings.get('filingDate', [])
        report_dates = recent_filings.get('reportDate', [])
        accession_numbers = recent_filings.get('accessionNumber', [])
        file_numbers = recent_filings.get('fileNumber', [])
        primary_documents = recent_filings.get('primaryDocument', [])
        primary_doc_descriptions = recent_filings.get('primaryDocDescription', [])
        sizes = recent_filings.get('size', [])
        
        ten_k_filings = []
        cutoff_year = datetime.now().year - years
        
        for i, form in enumerate(forms):
            if form == '10-K':
                filing_date = filing_dates[i]
                # Check if filing is within our year range
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
                    ten_k_filings.append(filing)
        
        self.logger.info(f"Found {len(ten_k_filings)} 10-K filings for CIK {cik}")
        return ten_k_filings
    
    def download_filing(self, cik: str, accession_number: str, primary_document: str, 
                       save_path: str) -> str:
        """
        Download a specific filing document
        
        Args:
            cik: Company CIK (required for URL construction)
            accession_number: SEC accession number
            primary_document: Primary document filename
            save_path: Local path to save the document
            
        Returns:
            Path to downloaded file
        """
        # Format accession number for URL (remove dashes)
        accession_clean = accession_number.replace('-', '')
        cik_formatted = str(cik).zfill(10)
        
        # Construct filing URL
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_formatted}/{accession_clean}/{primary_document}"
        
        self._rate_limit_request()
        
        try:
            response = self.session.get(filing_url, timeout=60)
            response.raise_for_status()
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # Save file
            with open(save_path, 'wb') as f:
                f.write(response.content)
            
            self.logger.info(f"Downloaded filing to {save_path}")
            return save_path
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to download filing {accession_number}: {e}")
            raise


class CompanyLookup:
    """Helper class for looking up company CIKs and metadata"""
    
    def __init__(self, edgar_client: SECEdgarClient):
        self.edgar_client = edgar_client
        self._company_cache = {}
    
    def get_cik_by_ticker(self, ticker: str) -> Optional[str]:
        """
        Look up CIK by ticker symbol
        Note: This is a simplified implementation. In production, you'd want
        to use a more comprehensive ticker-to-CIK mapping.
        """
        # Known major company CIKs for testing
        known_ciks = {
            'AAPL': '0000320193',  # Apple Inc.
            'MSFT': '0000789019',  # Microsoft Corp
            'GOOGL': '0001652044', # Alphabet Inc.
            'AMZN': '0001018724',  # Amazon.com Inc
            'TSLA': '0001318605',  # Tesla Inc
            'META': '0001326801',  # Meta Platforms Inc
            'NVDA': '0001045810',  # NVIDIA Corp
        }
        
        return known_ciks.get(ticker.upper())
    
    def get_company_info(self, cik: str) -> Dict[str, Any]:
        """Get basic company information"""
        if cik in self._company_cache:
            return self._company_cache[cik]
        
        try:
            submissions = self.edgar_client.get_company_submissions(cik)
            company_info = {
                'cik': cik,
                'name': submissions.get('name', 'Unknown'),
                'ticker': submissions.get('tickers', []),
                'exchanges': submissions.get('exchanges', []),
                'sic': submissions.get('sic', ''),
                'sicDescription': submissions.get('sicDescription', ''),
                'ein': submissions.get('ein', ''),
                'fiscalYearEnd': submissions.get('fiscalYearEnd', ''),
            }
            
            self._company_cache[cik] = company_info
            return company_info
            
        except Exception as e:
            self.edgar_client.logger.error(f"Failed to get company info for CIK {cik}: {e}")
            return {'cik': cik, 'name': 'Unknown', 'error': str(e)}


if __name__ == "__main__":
    # Example usage
    client = SECEdgarClient("Financial Analysis Tool test@example.com")
    lookup = CompanyLookup(client)
    
    # Test with Apple
    apple_cik = lookup.get_cik_by_ticker('AAPL')
    if apple_cik:
        print(f"Apple CIK: {apple_cik}")
        
        # Get company info
        company_info = lookup.get_company_info(apple_cik)
        print(f"Company: {company_info['name']}")
        
        # Get 10-K filings
        filings = client.get_10k_filings(apple_cik, years=5)
        print(f"Found {len(filings)} 10-K filings")
        
        for filing in filings[:3]:  # Show first 3
            print(f"  {filing.filing_date}: {filing.form} - {filing.accession_number}")