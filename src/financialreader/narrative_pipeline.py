"""
Narrative Data Pipeline for 10-K Analysis
Orchestrates HTML parsing and AI agents to extract narrative insights
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import pandas as pd
import logging
from pathlib import Path

from .html_parser import HTMLSectionParser, FilingSection
from .narrative_agents import ResearchAgent, ExtractionAgent, ResearchInsight, ExtractedInsights
from .edgar_client import SECEdgarClient, CompanyLookup

@dataclass 
class NarrativeAnalysis:
    """Complete narrative analysis of a 10-K filing"""
    company_cik: str
    company_name: str
    fiscal_year: int
    filing_date: str
    research_insight: ResearchInsight
    section_extractions: Dict[str, ExtractedInsights]
    sections_parsed: Dict[str, FilingSection]
    analysis_summary: Dict[str, Any]

class NarrativeDataPipeline:
    """
    Orchestrates narrative extraction from 10-K filings
    Coordinates HTML parsing, Research Agent, and Extraction Agent
    """
    
    def __init__(self, edgar_client: SECEdgarClient = None, gemini_api_key: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.edgar_client = edgar_client or SECEdgarClient()
        self.html_parser = HTMLSectionParser()
        self.research_agent = ResearchAgent(api_key=gemini_api_key)
        self.extraction_agent = ExtractionAgent(api_key=gemini_api_key)
        
        # Cache for parsed filings
        self._filing_cache = {}
    
    def analyze_company_narrative(self, cik: str, years: int = 5) -> List[NarrativeAnalysis]:
        """
        Analyze narrative content for a company across multiple years
        
        Args:
            cik: Company CIK
            years: Number of years to analyze
            
        Returns:
            List of NarrativeAnalysis objects, one per year
        """
        self.logger.info(f"Starting narrative analysis for CIK {cik} over {years} years")
        
        # Get company information
        lookup = CompanyLookup(self.edgar_client)
        try:
            # Try to get company info - use a known method or fallback
            company_info = lookup.get_company_info(cik)
            company_name = company_info.get('name', f'Company_{cik}')
        except:
            # Fallback to CIK if lookup fails
            company_name = f'Company_{cik}'
            self.logger.warning(f"Could not lookup company name for CIK {cik}, using fallback")
        
        # Find filing paths
        filing_paths = self._find_filing_paths(cik, years)
        
        analyses = []
        for filing_path in filing_paths:
            try:
                analysis = self._analyze_single_filing(cik, company_name, filing_path)
                if analysis:
                    analyses.append(analysis)
            except Exception as e:
                self.logger.error(f"Failed to analyze filing {filing_path}: {e}")
                continue
        
        self.logger.info(f"Completed narrative analysis for {len(analyses)} filings")
        return analyses
    
    def _find_filing_paths(self, cik: str, years: int) -> List[Path]:
        """Find local filing paths for a company"""
        
        # Look for filings in data directory
        data_dir = Path("data/filings")
        company_dirs = [d for d in data_dir.glob("*") if cik in d.name]
        
        if not company_dirs:
            self.logger.warning(f"No filing directories found for CIK {cik}")
            return []
        
        company_dir = company_dirs[0]
        filing_paths = []
        
        # Get filing paths from year directories
        year_dirs = sorted([d for d in company_dir.iterdir() if d.is_dir()], reverse=True)
        
        for year_dir in year_dirs[:years]:
            htm_files = list(year_dir.glob("*.htm"))
            if htm_files:
                filing_paths.append(htm_files[0])  # Take first .htm file
        
        return filing_paths
    
    def _analyze_single_filing(self, cik: str, company_name: str, filing_path: Path) -> Optional[NarrativeAnalysis]:
        """Analyze a single 10-K filing"""
        
        self.logger.info(f"Analyzing filing: {filing_path}")
        
        # Extract year from path
        fiscal_year = int(filing_path.parent.name)
        
        # Read filing content
        try:
            with open(filing_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
        except Exception as e:
            self.logger.error(f"Failed to read filing {filing_path}: {e}")
            return None
        
        # Step 1: Parse HTML sections
        self.logger.info("Step 1: Parsing HTML sections")
        sections = self.html_parser.parse_filing(html_content)
        
        if not sections:
            self.logger.warning(f"No sections extracted from {filing_path}")
            return None
        
        # Step 2: Research analysis
        self.logger.info("Step 2: Running research analysis")
        
        # Use full text for research analysis (first 10000 chars)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove XBRL metadata
        for tag in soup.find_all(['script', 'style', 'ix:header', 'ix:hidden']):
            tag.decompose()
        
        clean_text = soup.get_text()
        research_text = clean_text[:10000]  # First 10K chars for research
        
        research_insight = self.research_agent.analyze_filing_structure(
            company_name, fiscal_year, research_text
        )
        
        # Step 3: Extract insights from each section
        self.logger.info("Step 3: Extracting section insights")
        section_extractions = {}
        
        for section_id, section in sections.items():
            self.logger.info(f"Extracting insights from {section_id}")
            
            extraction = self.extraction_agent.extract_section_insights(
                section.content, section_id
            )
            section_extractions[section_id] = extraction
        
        # Step 4: Generate analysis summary
        analysis_summary = self._generate_analysis_summary(
            research_insight, section_extractions, sections
        )
        
        return NarrativeAnalysis(
            company_cik=cik,
            company_name=company_name,
            fiscal_year=fiscal_year,
            filing_date=filing_path.stem.split('_')[-1] if '_' in filing_path.stem else str(fiscal_year),
            research_insight=research_insight,
            section_extractions=section_extractions,
            sections_parsed=sections,
            analysis_summary=analysis_summary
        )
    
    def _generate_analysis_summary(self, research_insight: ResearchInsight, 
                                 section_extractions: Dict[str, ExtractedInsights],
                                 sections: Dict[str, FilingSection]) -> Dict[str, Any]:
        """Generate a comprehensive analysis summary"""
        
        summary = {
            'sections_analyzed': list(sections.keys()),
            'total_word_count': sum(section.word_count for section in sections.values()),
            'business_segments': research_insight.business_segments,
            'key_risks': research_insight.top_risks,
            'strategic_focus': research_insight.strategic_initiatives,
            'section_insights': {}
        }
        
        # Add section-specific summaries
        for section_id, extraction in section_extractions.items():
            section_summary = {'section_type': extraction.section_type}
            
            if extraction.business_overview:
                section_summary['business_model'] = extraction.business_overview.get('business_model', '')
                section_summary['revenue_streams'] = extraction.business_overview.get('revenue_streams', [])
            
            if extraction.risk_assessment:
                section_summary['material_risks'] = extraction.risk_assessment.get('material_risks', [])
            
            if extraction.performance_analysis:
                section_summary['revenue_drivers'] = extraction.performance_analysis.get('revenue_drivers', [])
                section_summary['strategic_priorities'] = extraction.performance_analysis.get('strategic_priorities', [])
            
            summary['section_insights'][section_id] = section_summary
        
        return summary
    
    def to_dataframe(self, analyses: List[NarrativeAnalysis]) -> pd.DataFrame:
        """
        Convert narrative analyses to a pandas DataFrame
        
        Args:
            analyses: List of NarrativeAnalysis objects
            
        Returns:
            DataFrame with narrative insights as columns, years as rows
        """
        if not analyses:
            return pd.DataFrame()
        
        rows = []
        for analysis in analyses:
            row = {
                'company_cik': analysis.company_cik,
                'company_name': analysis.company_name,
                'fiscal_year': analysis.fiscal_year,
                'filing_date': analysis.filing_date,
                'sections_analyzed': ', '.join(analysis.analysis_summary.get('sections_analyzed', [])),
                'total_word_count': analysis.analysis_summary.get('total_word_count', 0),
                'business_segments': ', '.join(analysis.analysis_summary.get('business_segments', [])),
                'key_risks': ', '.join(analysis.analysis_summary.get('key_risks', [])[:3]),  # Top 3 risks
                'strategic_focus': ', '.join(analysis.analysis_summary.get('strategic_focus', []))
            }
            
            # Add section-specific insights
            section_insights = analysis.analysis_summary.get('section_insights', {})
            
            if 'item1' in section_insights:
                item1 = section_insights['item1']
                row['business_model'] = item1.get('business_model', '')
                row['revenue_streams'] = ', '.join(item1.get('revenue_streams', []))
            
            if 'item1a' in section_insights:
                item1a = section_insights['item1a']
                row['material_risks'] = ', '.join(item1a.get('material_risks', [])[:3])  # Top 3
            
            if 'item7' in section_insights:
                item7 = section_insights['item7']
                row['revenue_drivers'] = ', '.join(item7.get('revenue_drivers', []))
                row['md_a_priorities'] = ', '.join(item7.get('strategic_priorities', []))
            
            rows.append(row)
        
        df = pd.DataFrame(rows)
        
        # Sort by fiscal year (most recent first)
        if not df.empty:
            df = df.sort_values('fiscal_year', ascending=False)
            df = df.reset_index(drop=True)
        
        return df
    
    def get_narrative_summary_report(self, analyses: List[NarrativeAnalysis]) -> Dict[str, Any]:
        """Generate a comprehensive summary report of narrative analyses"""
        
        if not analyses:
            return {"error": "No analyses provided"}
        
        company_name = analyses[0].company_name
        years_analyzed = [a.fiscal_year for a in analyses]
        years_analyzed.sort(reverse=True)
        
        # Aggregate insights across years
        all_business_segments = set()
        all_risks = set()
        all_strategic_initiatives = set()
        
        for analysis in analyses:
            all_business_segments.update(analysis.research_insight.business_segments)
            all_risks.update(analysis.research_insight.top_risks)
            all_strategic_initiatives.update(analysis.research_insight.strategic_initiatives)
        
        # Track evolution over time
        business_evolution = []
        risk_evolution = []
        
        for analysis in sorted(analyses, key=lambda x: x.fiscal_year):
            year_summary = {
                'year': analysis.fiscal_year,
                'sections': list(analysis.sections_parsed.keys()),
                'word_count': sum(s.word_count for s in analysis.sections_parsed.values()),
                'key_themes': analysis.research_insight.business_segments[:3]
            }
            business_evolution.append(year_summary)
            
            risk_summary = {
                'year': analysis.fiscal_year,
                'top_risks': analysis.research_insight.top_risks[:3]
            }
            risk_evolution.append(risk_summary)
        
        return {
            'company_overview': {
                'name': company_name,
                'years_analyzed': years_analyzed,
                'total_filings': len(analyses)
            },
            'business_insights': {
                'all_segments': list(all_business_segments),
                'recurring_segments': self._find_recurring_items(analyses, 'business_segments'),
                'business_evolution': business_evolution
            },
            'risk_analysis': {
                'all_risks': list(all_risks),
                'recurring_risks': self._find_recurring_items(analyses, 'top_risks'),
                'risk_evolution': risk_evolution
            },
            'strategic_focus': {
                'all_initiatives': list(all_strategic_initiatives),
                'recurring_initiatives': self._find_recurring_items(analyses, 'strategic_initiatives')
            },
            'content_metrics': {
                'avg_word_count': sum(sum(s.word_count for s in a.sections_parsed.values()) for a in analyses) / len(analyses),
                'sections_consistency': self._analyze_section_consistency(analyses)
            }
        }
    
    def _find_recurring_items(self, analyses: List[NarrativeAnalysis], attribute: str) -> List[str]:
        """Find items that appear across multiple years"""
        
        item_counts = {}
        
        for analysis in analyses:
            items = getattr(analysis.research_insight, attribute, [])
            for item in items:
                item_counts[item] = item_counts.get(item, 0) + 1
        
        # Return items appearing in at least 2 years
        threshold = min(2, len(analyses))
        recurring = [item for item, count in item_counts.items() if count >= threshold]
        
        return recurring
    
    def _analyze_section_consistency(self, analyses: List[NarrativeAnalysis]) -> Dict[str, float]:
        """Analyze consistency of section extraction across years"""
        
        section_counts = {}
        total_filings = len(analyses)
        
        for analysis in analyses:
            for section_id in analysis.sections_parsed.keys():
                section_counts[section_id] = section_counts.get(section_id, 0) + 1
        
        # Calculate consistency percentages
        consistency = {}
        for section_id, count in section_counts.items():
            consistency[section_id] = (count / total_filings) * 100
        
        return consistency


if __name__ == "__main__":
    # Example usage
    import sys
    import os
    
    # Test the narrative pipeline
    pipeline = NarrativeDataPipeline()
    
    # Test with Apple (if available)
    apple_cik = "0000320193"
    
    print("Testing Narrative Pipeline with Apple...")
    analyses = pipeline.analyze_company_narrative(apple_cik, years=2)
    
    if analyses:
        print(f"Analyzed {len(analyses)} filings")
        
        # Show summary for most recent analysis
        recent = analyses[0]
        print(f"\nFY{recent.fiscal_year} Analysis Summary:")
        print(f"  Sections: {list(recent.sections_parsed.keys())}")
        print(f"  Business Segments: {recent.research_insight.business_segments}")
        print(f"  Top Risks: {recent.research_insight.top_risks[:3]}")
        
        # Generate DataFrame
        df = pipeline.to_dataframe(analyses)
        print(f"\nDataFrame shape: {df.shape}")
        if not df.empty:
            print(f"Columns: {list(df.columns)}")
        
        # Generate summary report
        report = pipeline.get_narrative_summary_report(analyses)
        print(f"\nSummary Report:")
        print(f"  Company: {report['company_overview']['name']}")
        print(f"  Years: {report['company_overview']['years_analyzed']}")
        print(f"  Recurring segments: {report['business_insights']['recurring_segments']}")
    else:
        print("No analyses generated - check filing availability")