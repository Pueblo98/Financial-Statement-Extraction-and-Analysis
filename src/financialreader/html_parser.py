"""
HTML Section Parser for 10-K Filings
Extracts specific sections from SEC 10-K HTML filings
"""

from typing import Dict, List, Optional, Tuple
import re
from dataclasses import dataclass
from bs4 import BeautifulSoup, Tag, NavigableString
import logging

@dataclass
class FilingSection:
    """Represents a section from a 10-K filing"""
    section_id: str
    title: str
    content: str
    word_count: int
    start_position: Optional[int] = None
    end_position: Optional[int] = None

class HTMLSectionParser:
    """
    Parses HTML 10-K filings to extract key sections
    Handles variations in formatting across companies
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Target sections we want to extract
        self.target_sections = {
            'item1': {
                'patterns': [
                    r'part\s*i\s*item\s*1\.\s*business',
                    r'item\s*1\.\s*business',
                    r'item\s*1\s*business'
                ],
                'title': 'Business Overview',
                'keywords': ['business', 'overview', 'description', 'company', 'products']
            },
            'item1a': {
                'patterns': [
                    r'item\s*1a\.\s*risk\s*factors',
                    r'item\s*1a\s*risk\s*factors',
                    r'part\s*i\s*item\s*1a'
                ],
                'title': 'Risk Factors',
                'keywords': ['risk', 'factors', 'uncertainties', 'competition']
            },
            'item7': {
                'patterns': [
                    r'item\s*7\.\s*management',
                    r"management's\s*discussion\s*and\s*analysis",
                    r'part\s*ii\s*item\s*7'
                ],
                'title': 'Management Discussion & Analysis',
                'keywords': ['management', 'discussion', 'analysis', 'results', 'operations']
            }
        }
    
    def parse_filing(self, html_content: str) -> Dict[str, FilingSection]:
        """
        Parse a 10-K HTML filing and extract key sections
        
        Args:
            html_content: Raw HTML content of 10-K filing
            
        Returns:
            Dictionary mapping section IDs to FilingSection objects
        """
        self.logger.info("Parsing 10-K HTML filing")
        
        # Clean and parse HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove XBRL metadata and script tags
        for tag in soup.find_all(['script', 'style', 'ix:header', 'ix:hidden']):
            tag.decompose()
        
        # Get clean text content
        clean_text = soup.get_text()
        
        # Extract sections
        sections = {}
        for section_id, config in self.target_sections.items():
            section = self._extract_section(clean_text, section_id, config)
            if section:
                sections[section_id] = section
        
        self.logger.info(f"Extracted {len(sections)} sections from filing")
        return sections
    
    def _extract_section(self, text: str, section_id: str, config: Dict) -> Optional[FilingSection]:
        """Extract a specific section from the filing text"""
        
        # Find section start
        start_pos = self._find_section_start(text, config['patterns'])
        if start_pos == -1:
            self.logger.warning(f"Could not find start of section {section_id}")
            return None
        
        # Find section end
        end_pos = self._find_section_end(text, start_pos, section_id)
        
        # Extract content
        section_text = text[start_pos:end_pos].strip()
        
        # Clean up the content
        cleaned_content = self._clean_section_content(section_text)
        
        # Validate minimum content length
        if len(cleaned_content.split()) < 50:  # At least 50 words
            self.logger.warning(f"Section {section_id} too short ({len(cleaned_content.split())} words)")
            return None
        
        return FilingSection(
            section_id=section_id,
            title=config['title'],
            content=cleaned_content,
            word_count=len(cleaned_content.split()),
            start_position=start_pos,
            end_position=end_pos
        )
    
    def _find_section_start(self, text: str, patterns: List[str]) -> int:
        """Find the start position of a section using multiple patterns"""
        
        text_lower = text.lower()
        best_match = -1
        
        for pattern in patterns:
            # Search for pattern (case insensitive)
            matches = list(re.finditer(pattern, text_lower, re.IGNORECASE))
            
            for match in matches:
                pos = match.start()
                
                # Look for section header context (within 200 characters)
                context_start = max(0, pos - 100)
                context_end = min(len(text), pos + 200)
                context = text[context_start:context_end].lower()
                
                # Check if this looks like a section header
                if self._is_section_header(context, pos - context_start):
                    # Find the end of the header line to start content extraction
                    header_end = text.find('\n', pos)
                    if header_end == -1:
                        header_end = pos + 100  # fallback
                    
                    # Look for actual content start (skip table of contents style entries)
                    content_start = self._find_content_start(text, header_end)
                    
                    if best_match == -1 or content_start < best_match:
                        best_match = content_start
        
        return best_match
    
    def _is_section_header(self, context: str, match_pos: int) -> bool:
        """Check if a match appears to be a section header"""
        
        # Look for indicators this is a section header
        header_indicators = [
            'part i',
            'part ii', 
            'table of contents',
            'item',
            '\n\n',  # paragraph break before
            'page',
            'form 10-k'
        ]
        
        # Check context around the match
        context_before = context[:match_pos].lower()
        context_after = context[match_pos:].lower()
        
        # Should have line breaks or clear separation
        has_separation = (
            '\n' in context_before[-20:] or 
            '\n' in context_after[:20:] or
            any(indicator in context for indicator in header_indicators)
        )
        
        return has_separation
    
    def _find_content_start(self, text: str, header_end: int) -> int:
        """Find where the actual section content starts after the header"""
        
        # Look for first substantial paragraph after header
        search_text = text[header_end:header_end + 2000]  # Search next 2000 chars
        
        # Split into potential paragraphs
        paragraphs = re.split(r'\n\s*\n', search_text)
        
        for i, paragraph in enumerate(paragraphs):
            # Skip short lines, numbers, table entries
            cleaned = paragraph.strip()
            if (len(cleaned.split()) >= 10 and  # At least 10 words
                not re.match(r'^\d+$', cleaned) and  # Not just numbers
                not re.match(r'^page\s*\d+', cleaned.lower()) and
                'item' not in cleaned.lower()[:50]):  # Not another item header
                
                # Found content start
                content_pos = header_end + search_text.find(paragraph)
                return content_pos
        
        # Fallback to right after header
        return header_end
    
    def _find_section_end(self, text: str, start_pos: int, current_section: str) -> int:
        """Find where the current section ends"""
        
        text_lower = text.lower()
        
        # Look for next major section
        next_section_patterns = [
            r'item\s*\d+[a-z]?\b',
            r'part\s*[i]+',
            r'signatures',
            r'exhibits'
        ]
        
        # Start searching after current section header
        search_start = start_pos + 500  # Skip at least 500 chars to avoid header
        
        best_end = len(text)  # Default to end of document
        
        for pattern in next_section_patterns:
            matches = list(re.finditer(pattern, text_lower[search_start:], re.IGNORECASE))
            
            for match in matches:
                absolute_pos = search_start + match.start()
                
                # Check if this looks like a real section break
                context_start = max(0, absolute_pos - 100)
                context_end = min(len(text), absolute_pos + 100)
                context = text[context_start:context_end].lower()
                
                if self._is_section_header(context, absolute_pos - context_start):
                    if absolute_pos < best_end:
                        best_end = absolute_pos
        
        return best_end
    
    def _clean_section_content(self, content: str) -> str:
        """Clean and normalize section content"""
        
        # Remove excessive whitespace
        content = re.sub(r'\s+', ' ', content)
        
        # Remove page numbers and headers/footers
        lines = content.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            
            # Skip likely headers/footers/page numbers
            if (len(line) < 5 or 
                re.match(r'^\d+$', line) or  # Just a number
                re.match(r'^page\s*\d+', line.lower()) or
                'table of contents' in line.lower()):
                continue
                
            cleaned_lines.append(line)
        
        # Rejoin and clean up
        cleaned = ' '.join(cleaned_lines)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def get_section_summary(self, sections: Dict[str, FilingSection]) -> Dict[str, any]:
        """Generate a summary of extracted sections"""
        
        summary = {
            'sections_found': list(sections.keys()),
            'total_sections': len(sections),
            'total_word_count': sum(section.word_count for section in sections.values()),
            'section_details': {}
        }
        
        for section_id, section in sections.items():
            summary['section_details'][section_id] = {
                'title': section.title,
                'word_count': section.word_count,
                'content_preview': section.content[:200] + '...' if len(section.content) > 200 else section.content
            }
        
        return summary


if __name__ == "__main__":
    # Example usage
    import os
    
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