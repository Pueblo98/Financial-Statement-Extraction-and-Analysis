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
        """Extract a single section using AI"""
        
        # Truncate text to fit in prompt (keep first 15000 chars)
        text_sample = text[:15000] if len(text) > 15000 else text
        
        prompt = f"""You are an expert at analyzing SEC 10-K filings. Extract the content for {config['title']} from this filing text.

Section: {config['title']} ({config['description']})

Instructions:
1. Find the relevant section in the text
2. Extract the complete content for this section
3. Return ONLY the extracted text, no explanations or formatting
4. If you cannot find this section, return "NOT_FOUND"

Filing text:
{text_sample}

Extracted content:"""

        try:
            response = self.model.generate_content(prompt)
            content = response.text.strip()
            
            # Check if AI found the section
            if content.lower() in ['not found', 'not_found', '']:
                return None
            
            return content
            
        except Exception as e:
            self.logger.error(f"AI call failed for {section_id}: {e}")
            return None
    
    def _extract_sections_fallback(self, text: str) -> Dict[str, FilingSection]:
        """Fallback method using basic text parsing when AI is unavailable"""
        
        sections = {}
        
        # Simple keyword-based extraction
        section_patterns = {
            'item1': [
                r'item\s*1[\.\s]*business.*?(?=item\s*1a|item\s*2|$)',
                r'business\s*overview.*?(?=risk\s*factors|item\s*2|$)',
            ],
            'item1a': [
                r'item\s*1a[\.\s]*risk\s*factors.*?(?=item\s*2|$)',
                r'risk\s*factors.*?(?=item\s*2|$)',
            ],
            'item7': [
                r'item\s*7[\.\s]*management.*?(?=item\s*8|$)',
                r"management's\s*discussion\s*and\s*analysis.*?(?=item\s*8|$)",
            ]
        }
        
        for section_id, patterns in section_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                if match:
                    content = match.group(0).strip()
                    if len(content.split()) >= 50:
                        sections[section_id] = FilingSection(
                            section_id=section_id,
                            title=self.target_sections[section_id]['title'],
                            content=content,
                            word_count=len(content.split())
                        )
                        break
        
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