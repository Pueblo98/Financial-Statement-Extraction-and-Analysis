"""
Test script to verify EDGAR URL structure and filing access
"""

import requests
from edgar_client import SECEdgarClient, CompanyLookup
import json

def test_edgar_access():
    """Test EDGAR API access and URL construction"""
    
    client = SECEdgarClient("Financial Analysis Test contact@example.com")
    lookup = CompanyLookup(client)
    
    # Get Apple's CIK
    apple_cik = lookup.get_cik_by_ticker('AAPL')
    print(f"Apple CIK: {apple_cik}")
    
    # Get Apple's submissions
    submissions = client.get_company_submissions(apple_cik)
    
    # Get the most recent 10-K filing details
    recent_filings = submissions.get('filings', {}).get('recent', {})
    forms = recent_filings.get('form', [])
    accession_numbers = recent_filings.get('accessionNumber', [])
    primary_documents = recent_filings.get('primaryDocument', [])
    filing_dates = recent_filings.get('filingDate', [])
    
    # Find the first 10-K
    latest_10k = None
    for i, form in enumerate(forms):
        if form == '10-K':
            latest_10k = {
                'accession': accession_numbers[i],
                'primary_doc': primary_documents[i],
                'filing_date': filing_dates[i]
            }
            break
    
    if latest_10k:
        print(f"\nLatest 10-K filing:")
        print(f"Accession: {latest_10k['accession']}")
        print(f"Primary Document: {latest_10k['primary_doc']}")
        print(f"Filing Date: {latest_10k['filing_date']}")
        
        # Test different URL constructions
        accession_clean = latest_10k['accession'].replace('-', '')
        cik_clean = apple_cik.lstrip('0') or '0'
        
        # Option 1: CIK with dashes in accession
        url1 = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{latest_10k['accession']}/{latest_10k['primary_doc']}"
        
        # Option 2: CIK with clean accession
        url2 = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{accession_clean}/{latest_10k['primary_doc']}"
        
        # Option 3: Full CIK with dashes
        url3 = f"https://www.sec.gov/Archives/edgar/data/{apple_cik}/{latest_10k['accession']}/{latest_10k['primary_doc']}"
        
        print(f"\nTesting URL constructions:")
        
        # Test each URL
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Financial Analysis Test contact@example.com',
            'Accept': 'text/html,application/xhtml+xml'
        })
        
        for i, url in enumerate([url1, url2, url3], 1):
            print(f"\nURL {i}: {url}")
            try:
                response = session.head(url, timeout=10)
                print(f"Status: {response.status_code}")
                if response.status_code < 400:
                    print("✅ SUCCESS - This URL works!")
                    return url
            except Exception as e:
                print(f"Error: {e}")
        
        # Try to find the actual URL by checking the filing index
        print(f"\nTrying to find actual filing URL...")
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{latest_10k['accession']}/index.json"
        print(f"Index URL: {index_url}")
        
        try:
            response = session.get(index_url, timeout=10)
            if response.status_code == 200:
                index_data = response.json()
                print("Index data found:")
                directory = index_data.get('directory', {})
                items = directory.get('item', [])
                for item in items:
                    if item.get('name') == latest_10k['primary_doc']:
                        print(f"Found primary document: {item}")
                        break
        except Exception as e:
            print(f"Error getting index: {e}")
    
    return None

if __name__ == "__main__":
    test_edgar_access()