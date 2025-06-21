"""
AI-Driven HTML Section Parser for 10-K Filings
Uses Gemini AI to extract specific sections from SEC 10-K filings
"""

from typing import Dict, List, Optional, Tuple
import re
from dataclasses import dataclass
from bs4 import BeautifulSoup, Tag, NavigableString
import logging
import os
import json

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    genai = None

@dataclass
class FilingSection:
    """Represents a section from a 10-K filing"""
    section_id: str
    title: str
    content: str
    word_count: int
    start_position: Optional[int] = None
    end_position: Optional[int] = None

class AIHTMLSectionParser:
    """
    AI-powered parser that uses Gemini to extract key sections from 10-K filings
    Bypasses complex HTML parsing by having AI read and extract content directly
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        
        # Initialize Gemini
        self.model = None
        if api_key and GEMINI_AVAILABLE:
            try:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel('gemini-2.0-flash')
                self.logger.info("AI HTML Parser initialized with Gemini")
            except Exception as e:
                self.logger.error(f"Failed to initialize Gemini: {e}")
                self.model = None
        else:
            self.logger.warning("No Gemini API key or library available - using fallback parsing")
        
        # Target sections we want to extract
        self.target_sections = {
            'item1': {
                'title': 'Business Overview',
                'description': 'Company business description, products, services, and operations'
            },
            'item1a': {
                'title': 'Risk Factors', 
                'description': 'Material risks and uncertainties facing the company'
            },
            'item7': {
                'title': 'Management Discussion & Analysis',
                'description': 'Management analysis of financial condition and results of operations'
            }
        }
    
    def parse_filing(self, html_content: str) -> Dict[str, FilingSection]:
        """
        Parse a 10-K HTML filing and extract key sections using AI
        
        Args:
            html_content: Raw HTML content of 10-K filing
            
        Returns:
            Dictionary mapping section IDs to FilingSection objects
        """
        self.logger.info("Parsing 10-K HTML filing with AI")
        
        # Clean and extract text from HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove XBRL metadata and script tags
        for tag in soup.find_all(['script', 'style', 'ix:header', 'ix:hidden']):
            tag.decompose()
        
        # Get clean text content
        clean_text = soup.get_text()
        
        if self.model:
            # Use AI to extract sections
            return self._extract_sections_with_ai(clean_text)
        else:
            # Fallback to basic text parsing
            return self._extract_sections_fallback(clean_text)
    
    def _extract_sections_with_ai(self, text: str) -> Dict[str, FilingSection]:
        """Extract sections using Gemini AI"""
        
        sections = {}
        
        for section_id, config in self.target_sections.items():
            try:
                section_content = self._extract_single_section_with_ai(text, section_id, config)
                if section_content and len(section_content.split()) >= 50:
                    sections[section_id] = FilingSection(
                        section_id=section_id,
                        title=config['title'],
                        content=section_content,
                        word_count=len(section_content.split())
                    )
                    self.logger.info(f"AI extracted {section_id}: {len(section_content.split())} words")
                else:
                    self.logger.warning(f"AI could not extract meaningful content for {section_id}")
            except Exception as e:
                self.logger.error(f"AI extraction failed for {section_id}: {e}")
        
        self.logger.info(f"AI extracted {len(sections)} sections from filing")
        return sections
    
    def _extract_single_section_with_ai(self, text: str, section_id: str, config: Dict) -> Optional[str]:
        """Extract a single section using AI with improved prompts"""
        
        # Increase text sample size and use different strategies for different sections
        if section_id == 'item1a':  # Risk Factors - usually longer, need more text
            text_sample = text[:25000] if len(text) > 25000 else text
        elif section_id == 'item7':  # MD&A - also longer, need more text
            text_sample = text[:25000] if len(text) > 25000 else text
        else:  # item1 - Business Overview
            text_sample = text[:20000] if len(text) > 20000 else text
        
        # Create section-specific prompts
        if section_id == 'item1a':
            prompt = f"""You are an expert at analyzing SEC 10-K filings. Extract the "Risk Factors" section (Item 1A) from this filing text.

IMPORTANT: Look for the section that discusses risks, uncertainties, and potential threats to the business. This section typically:
- Lists material risks facing the company
- Discusses regulatory, competitive, operational, and financial risks
- May be titled "Risk Factors", "Item 1A", or similar
- Usually appears after the business overview section

Instructions:
1. Find the Risk Factors section in the text
2. Extract the COMPLETE content of this section
3. Include all risk factors mentioned
4. Return ONLY the extracted text, no explanations
5. If you cannot find this section, return "NOT_FOUND"

Filing text (first 25,000 characters):
{text_sample}

Extracted Risk Factors content:"""

        elif section_id == 'item7':
            prompt = f"""You are an expert at analyzing SEC 10-K filings. Extract the "Management's Discussion and Analysis of Financial Condition and Results of Operations" section (Item 7) from this filing text.

IMPORTANT: Look for the section where management discusses financial performance, trends, and outlook. This section typically:
- Analyzes revenue, expenses, and profitability trends
- Explains changes in financial performance
- Discusses business drivers and challenges
- May be titled "MD&A", "Management Discussion", "Item 7", or similar
- Usually appears after the risk factors section

Instructions:
1. Find the Management Discussion and Analysis section in the text
2. Extract the COMPLETE content of this section
3. Include all financial analysis and management commentary
4. Return ONLY the extracted text, no explanations
5. If you cannot find this section, return "NOT_FOUND"

Filing text (first 25,000 characters):
{text_sample}

Extracted MD&A content:"""

        else:  # item1
            prompt = f"""You are an expert at analyzing SEC 10-K filings. Extract the "Business Overview" section (Item 1) from this filing text.

IMPORTANT: Look for the section that describes the company's business, products, services, and operations. This section typically:
- Describes what the company does
- Lists main products and services
- Discusses business segments and markets
- May be titled "Business", "Business Overview", "Item 1", or similar
- Usually appears early in the filing

Instructions:
1. Find the Business Overview section in the text
2. Extract the COMPLETE content of this section
3. Include all business description and operations details
4. Return ONLY the extracted text, no explanations
5. If you cannot find this section, return "NOT_FOUND"

Filing text (first 20,000 characters):
{text_sample}

Extracted Business Overview content:"""

        try:
            response = self.model.generate_content(prompt)
            content = response.text.strip()
            
            # Check if AI found the section
            if content.lower() in ['not found', 'not_found', '']:
                self.logger.debug(f"AI returned NOT_FOUND for {section_id}")
                return None
            
            # Additional validation - check if content seems meaningful
            if len(content.split()) < 30:  # Too short to be meaningful
                self.logger.debug(f"AI returned too short content for {section_id}: {len(content.split())} words")
                return None
            
            # Check if content contains relevant keywords for the section
            if section_id == 'item1a' and not any(word in content.lower() for word in ['risk', 'uncertainty', 'threat', 'challenge']):
                self.logger.debug(f"AI content for {section_id} doesn't contain risk-related keywords")
                return None
            elif section_id == 'item7' and not any(word in content.lower() for word in ['revenue', 'income', 'financial', 'management', 'discussion']):
                self.logger.debug(f"AI content for {section_id} doesn't contain financial discussion keywords")
                return None
            elif section_id == 'item1' and not any(word in content.lower() for word in ['business', 'company', 'product', 'service', 'operation']):
                self.logger.debug(f"AI content for {section_id} doesn't contain business description keywords")
                return None
            
            return content
            
        except Exception as e:
            self.logger.error(f"AI call failed for {section_id}: {e}")
            return None
    
    def _extract_sections_fallback(self, text: str) -> Dict[str, FilingSection]:
        """Fallback method using improved text parsing when AI is unavailable"""
        
        sections = {}
        
        # More comprehensive keyword-based extraction patterns
        section_patterns = {
            'item1': [
                r'item\s*1[\.\s]*business.*?(?=item\s*1a|item\s*2|$)',
                r'business\s*overview.*?(?=risk\s*factors|item\s*2|$)',
                r'business\s*description.*?(?=risk\s*factors|item\s*2|$)',
                r'company\s*overview.*?(?=risk\s*factors|item\s*2|$)',
                r'overview.*?(?=risk\s*factors|item\s*2|$)',
            ],
            'item1a': [
                r'item\s*1a[\.\s]*risk\s*factors.*?(?=item\s*2|$)',
                r'risk\s*factors.*?(?=item\s*2|$)',
                r'item\s*1a.*?(?=item\s*2|$)',
                r'risks.*?(?=item\s*2|$)',
                r'uncertainties.*?(?=item\s*2|$)',
            ],
            'item7': [
                r'item\s*7[\.\s]*management.*?(?=item\s*8|$)',
                r"management's\s*discussion\s*and\s*analysis.*?(?=item\s*8|$)",
                r'md&a.*?(?=item\s*8|$)',
                r'management\s*discussion.*?(?=item\s*8|$)',
                r'financial\s*discussion.*?(?=item\s*8|$)',
            ]
        }
        
        for section_id, patterns in section_patterns.items():
            content_found = False
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                if match:
                    content = match.group(0).strip()
                    if len(content.split()) >= 50:  # Minimum meaningful content
                        sections[section_id] = FilingSection(
                            section_id=section_id,
                            title=self.target_sections[section_id]['title'],
                            content=content,
                            word_count=len(content.split())
                        )
                        self.logger.info(f"Fallback extracted {section_id}: {len(content.split())} words")
                        content_found = True
                        break
            
            if not content_found:
                self.logger.warning(f"Fallback could not extract {section_id} - no patterns matched")
        
        self.logger.info(f"Fallback extracted {len(sections)} sections from filing")
        return sections
    
    def get_section_summary(self, sections: Dict[str, FilingSection]) -> Dict[str, any]:
        """Generate summary of extracted sections"""
        
        summary = {
            'total_sections': len(sections),
            'sections_found': list(sections.keys()),
            'total_words': sum(section.word_count for section in sections.values()),
            'section_details': {}
        }
        
        for section_id, section in sections.items():
            summary['section_details'][section_id] = {
                'title': section.title,
                'word_count': section.word_count,
                'content_preview': section.content[:200] + "..." if len(section.content) > 200 else section.content
            }
        
        return summary


# Backward compatibility - use the new AI parser
HTMLSectionParser = AIHTMLSectionParser

if __name__ == "__main__":
    # Example usage
    parser = HTMLSectionParser()
    
    # Test with Apple 2024 filing
    filing_path = "data/filings/0000320193_Apple_Inc/2024/0000320193-24-000123_aapl-20240928.htm"
    
    if os.path.exists(filing_path):
        print("Testing HTML parser with Apple 2024 10-K...")
        
        with open(filing_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        sections = parser.parse_filing(html_content)
        summary = parser.get_section_summary(sections)
        
        print(f"Extracted {summary['total_sections']} sections:")
        for section_id, details in summary['section_details'].items():
            print(f"  {section_id}: {details['title']} ({details['word_count']} words)")
            print(f"    Preview: {details['content_preview']}")
    else:
        print(f"Test file not found: {filing_path}")