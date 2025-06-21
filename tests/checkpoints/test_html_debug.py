#!/usr/bin/env python3
"""
Debug HTML parsing for 10-K filings
"""

import sys
sys.path.append('../../src')

from bs4 import BeautifulSoup
import re

def debug_filing_structure():
    """Debug the structure of Apple's 10-K filing"""
    
    filing_path = 'data/filings/0000320193_Apple_Inc/2024/0000320193-24-000123_aapl-20240928.htm'
    
    with open(filing_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Parse with BeautifulSoup and clean
    soup = BeautifulSoup(content, 'html.parser')
    
    # Remove XBRL metadata
    for tag in soup.find_all(['script', 'style', 'ix:header', 'ix:hidden']):
        tag.decompose()
    
    text = soup.get_text()
    
    print(f"Total text length: {len(text)} characters")
    print(f"Total words: {len(text.split())}")
    
    # Look for specific patterns
    patterns = [
        r'part\s*i\s*item\s*1\.\s*business',
        r'item\s*1\.\s*business',
        r'item\s*1a\.\s*risk\s*factors',
        r'item\s*7\.\s*management'
    ]
    
    print("\n=== PATTERN MATCHES ===")
    for pattern in patterns:
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        print(f"\nPattern: {pattern}")
        print(f"Matches found: {len(matches)}")
        
        for i, match in enumerate(matches[:3]):  # Show first 3 matches
            start = max(0, match.start() - 100)
            end = min(len(text), match.end() + 200)
            context = text[start:end].replace('\n', ' ').strip()
            print(f"  Match {i+1} at position {match.start()}:")
            print(f"    Context: {context}")
            
            # Check what comes after this match
            next_500 = text[match.end():match.end() + 500]
            next_500_clean = ' '.join(next_500.split())
            print(f"    Next 500 chars: {next_500_clean}")
    
    # Look for Business section specifically
    print("\n=== BUSINESS SECTION SEARCH ===")
    business_patterns = [
        r'PART\s*I.*?Item\s*1.*?Business',
        r'Item\s*1.*?Business.*?Company\s*Background',
        r'Company\s*designs,\s*manufactures'
    ]
    
    for pattern in business_patterns:
        matches = list(re.finditer(pattern, text, re.IGNORECASE | re.DOTALL))
        print(f"\nBusiness pattern: {pattern}")
        print(f"Matches: {len(matches)}")
        
        for match in matches:
            start = max(0, match.start() - 50)
            end = min(len(text), match.end() + 300)
            context = text[start:end]
            print(f"  Business context: {context[:500]}...")

if __name__ == "__main__":
    debug_filing_structure()