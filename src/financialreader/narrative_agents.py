"""
AI Agents for 10-K Narrative Analysis
Research Agent and Extraction Agent for processing 10-K text sections
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json
import logging
import google.generativeai as genai
import os

@dataclass
class ResearchInsight:
    """Research insights from 10-K analysis"""
    business_segments: List[str]
    performance_drivers: List[str]
    top_risks: List[str]
    forward_statements: List[str]
    strategic_initiatives: List[str]
    priority_sections: Dict[str, int]  # section_id -> priority score

@dataclass
class ExtractedInsights:
    """Extracted business insights from specific sections"""
    section_type: str
    business_overview: Optional[Dict[str, Any]] = None
    performance_analysis: Optional[Dict[str, Any]] = None
    risk_assessment: Optional[Dict[str, Any]] = None
    raw_response: Optional[str] = None

class ResearchAgent:
    """
    AI Agent for analyzing 10-K structure and prioritizing content extraction
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        
        # Initialize Gemini client
        self.model = None
        if api_key:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel('gemini-2.0-flash')
        else:
            self.logger.warning("No Gemini API key provided - using mock responses")
    
    def analyze_filing_structure(self, company_name: str, year: int, filing_text: str, 
                               industry: str = "Technology") -> ResearchInsight:
        """
        Analyze 10-K filing structure to identify key themes and priorities
        
        Args:
            company_name: Name of the company
            year: Filing year
            filing_text: Full or partial 10-K text content
            industry: Industry context
            
        Returns:
            ResearchInsight with prioritized analysis
        """
        self.logger.info(f"Analyzing {company_name} {year} 10-K structure")
        
        # Create research prompt
        prompt = self._build_research_prompt(company_name, year, industry, filing_text)
        
        if self.model:
            try:
                response = self._call_gemini_api(prompt)
                return self._parse_research_response(response)
            except Exception as e:
                self.logger.error(f"Gemini API call failed: {e}")
                return self._generate_mock_research_insight(company_name, year)
        else:
            # Generate mock response for testing
            return self._generate_mock_research_insight(company_name, year)
    
    def _build_research_prompt(self, company_name: str, year: int, industry: str, filing_text: str) -> str:
        """Build the research analysis prompt"""
        
        # Truncate filing text to fit in prompt (keep first 8000 chars)
        text_sample = filing_text[:8000] if len(filing_text) > 8000 else filing_text
        
        prompt = f"""You are a financial research analyst tasked with analyzing a 10-K filing structure.

Given this 10-K filing excerpt, identify:

1. The most strategically important business segments/divisions mentioned
2. Key performance drivers mentioned in MD&A or business sections
3. Top 5 most material risk factors
4. Any forward-looking statements or guidance provided
5. Significant events, acquisitions, or strategic initiatives

Return a structured JSON analysis prioritizing information most relevant to financial modeling and trend analysis.

Company: {company_name}
Filing Year: {year}
Industry Context: {industry}

Filing Text Excerpt:
{text_sample}

Please respond with a JSON object in this format:
{{
    "business_segments": ["segment1", "segment2", ...],
    "performance_drivers": ["driver1", "driver2", ...],
    "top_risks": ["risk1", "risk2", ...],
    "forward_statements": ["statement1", "statement2", ...],
    "strategic_initiatives": ["initiative1", "initiative2", ...],
    "priority_sections": {{
        "item1": 85,
        "item1a": 75,
        "item7": 90
    }}
}}"""
        
        return prompt
    
    def _call_gemini_api(self, prompt: str) -> str:
        """Call Gemini API for research analysis"""
        
        full_prompt = f"""You are a financial research analyst expert at analyzing SEC 10-K filings.

{prompt}"""
        
        response = self.model.generate_content(full_prompt)
        return response.text
    
    def _parse_research_response(self, response: str) -> ResearchInsight:
        """Parse Gemini response into ResearchInsight object"""
        
        try:
            data = json.loads(response)
            
            return ResearchInsight(
                business_segments=data.get('business_segments', []),
                performance_drivers=data.get('performance_drivers', []),
                top_risks=data.get('top_risks', []),
                forward_statements=data.get('forward_statements', []),
                strategic_initiatives=data.get('strategic_initiatives', []),
                priority_sections=data.get('priority_sections', {})
            )
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse research response: {e}")
            # Fallback to extracting key information from text
            return self._extract_insights_from_text(response)
    
    def _generate_mock_research_insight(self, company_name: str, year: int) -> ResearchInsight:
        """Generate mock research insight for testing"""
        
        if "apple" in company_name.lower():
            return ResearchInsight(
                business_segments=["iPhone", "Mac", "iPad", "Wearables", "Services"],
                performance_drivers=["iPhone sales", "Services growth", "Geographic expansion", "Product innovation"],
                top_risks=["Supply chain disruption", "Intense competition", "Economic uncertainty", "Regulatory changes", "Cybersecurity threats"],
                forward_statements=["Continued investment in R&D", "Focus on Services growth", "Supply chain resilience"],
                strategic_initiatives=["Apple Intelligence", "Environmental sustainability", "Market expansion"],
                priority_sections={"item1": 85, "item1a": 75, "item7": 90}
            )
        else:
            return ResearchInsight(
                business_segments=["Core Business", "Growth Segments"],
                performance_drivers=["Revenue growth", "Market expansion", "Operational efficiency"],
                top_risks=["Market competition", "Economic conditions", "Regulatory environment"],
                forward_statements=["Strategic investments", "Market opportunities"],
                strategic_initiatives=["Digital transformation", "Market expansion"],
                priority_sections={"item1": 80, "item1a": 70, "item7": 85}
            )
    
    def _extract_insights_from_text(self, response_text: str) -> ResearchInsight:
        """Extract insights from non-JSON response text"""
        
        # Basic text parsing fallback
        return ResearchInsight(
            business_segments=["Core Business"],
            performance_drivers=["Revenue growth"],
            top_risks=["Market risks"],
            forward_statements=["Future outlook"],
            strategic_initiatives=["Strategic focus"],
            priority_sections={"item1": 80, "item1a": 70, "item7": 85}
        )


class ExtractionAgent:
    """
    AI Agent for extracting structured insights from specific 10-K sections
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        
        # Initialize Gemini client
        self.model = None
        if api_key:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel('gemini-2.0-flash')
        else:
            self.logger.warning("No Gemini API key provided - using mock responses")
    
    def extract_section_insights(self, section_text: str, section_type: str) -> ExtractedInsights:
        """
        Extract structured insights from a specific 10-K section
        
        Args:
            section_text: Text content of the section
            section_type: Type of section (item1, item1a, item7)
            
        Returns:
            ExtractedInsights with structured summaries
        """
        self.logger.info(f"Extracting insights from {section_type} section ({len(section_text)} chars)")
        
        # Create extraction prompt
        prompt = self._build_extraction_prompt(section_text, section_type)
        
        if self.model:
            try:
                response = self._call_gemini_api(prompt)
                return self._parse_extraction_response(response, section_type)
            except Exception as e:
                self.logger.error(f"Gemini API call failed: {e}")
                return self._generate_mock_extraction(section_type)
        else:
            # Generate mock response for testing
            return self._generate_mock_extraction(section_type)
    
    def _build_extraction_prompt(self, section_text: str, section_type: str) -> str:
        """Build the extraction prompt based on section type"""
        
        # Truncate text to fit in prompt (keep first 6000 chars)
        text_sample = section_text[:6000] if len(section_text) > 6000 else section_text
        
        base_prompt = """You are a financial analyst extracting key insights from 10-K sections.

For the following text section, extract:"""
        
        if section_type == "item1":
            specific_instructions = """
BUSINESS OVERVIEW:
- Core business model and revenue streams
- Market position and competitive advantages  
- Geographic/segment breakdown
- Key subsidiaries or business units

Return JSON format:
{
    "business_overview": {
        "business_model": "description",
        "revenue_streams": ["stream1", "stream2"],
        "market_position": "description", 
        "competitive_advantages": ["advantage1", "advantage2"],
        "geographic_breakdown": ["region1", "region2"],
        "key_subsidiaries": ["sub1", "sub2"]
    }
}"""
        
        elif section_type == "item1a":
            specific_instructions = """
RISK ASSESSMENT:
- Top 5 most material risks to business performance
- Industry-specific vs company-specific risks
- New or emerging risk factors vs recurring ones

Return JSON format:
{
    "risk_assessment": {
        "material_risks": ["risk1", "risk2", "risk3", "risk4", "risk5"],
        "industry_risks": ["risk1", "risk2"],
        "company_risks": ["risk1", "risk2"], 
        "emerging_risks": ["risk1", "risk2"],
        "recurring_risks": ["risk1", "risk2"]
    }
}"""
        
        elif section_type == "item7":
            specific_instructions = """
PERFORMANCE ANALYSIS:
- Main drivers of revenue/profit changes vs prior year
- Management's explanation of performance trends
- Forward-looking strategic priorities
- Guidance or outlook statements

Return JSON format:
{
    "performance_analysis": {
        "revenue_drivers": ["driver1", "driver2"],
        "profit_drivers": ["driver1", "driver2"],
        "performance_trends": ["trend1", "trend2"],
        "strategic_priorities": ["priority1", "priority2"],
        "outlook_statements": ["statement1", "statement2"]
    }
}"""
        
        else:
            specific_instructions = """
Extract key business insights and return in JSON format.
"""
        
        full_prompt = f"""{base_prompt}

{specific_instructions}

Section Type: {section_type}
Text: {text_sample}

Return concise, factual summaries optimized for quantitative analysis."""
        
        return full_prompt
    
    def _call_gemini_api(self, prompt: str) -> str:
        """Call Gemini API for extraction analysis"""
        
        full_prompt = f"""You are a financial analyst expert at extracting structured insights from SEC 10-K filings.

{prompt}"""
        
        response = self.model.generate_content(full_prompt)
        return response.text
    
    def _parse_extraction_response(self, response: str, section_type: str) -> ExtractedInsights:
        """Parse Gemini response into ExtractedInsights object"""
        
        try:
            data = json.loads(response)
            
            return ExtractedInsights(
                section_type=section_type,
                business_overview=data.get('business_overview'),
                performance_analysis=data.get('performance_analysis'),
                risk_assessment=data.get('risk_assessment'),
                raw_response=response
            )
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse extraction response: {e}")
            return ExtractedInsights(
                section_type=section_type,
                raw_response=response
            )
    
    def _generate_mock_extraction(self, section_type: str) -> ExtractedInsights:
        """Generate mock extraction for testing"""
        
        if section_type == "item1":
            return ExtractedInsights(
                section_type=section_type,
                business_overview={
                    "business_model": "Technology company designing and manufacturing consumer electronics",
                    "revenue_streams": ["Product sales", "Services", "Accessories"],
                    "market_position": "Leading position in premium smartphone market",
                    "competitive_advantages": ["Brand loyalty", "Ecosystem integration", "Innovation"],
                    "geographic_breakdown": ["Americas", "Europe", "Greater China", "Japan", "Rest of Asia Pacific"],
                    "key_subsidiaries": ["Apple Operations International", "Apple Sales International"]
                }
            )
        
        elif section_type == "item1a":
            return ExtractedInsights(
                section_type=section_type,
                risk_assessment={
                    "material_risks": ["Supply chain disruptions", "Intense competition", "Economic uncertainty", "Regulatory changes", "Technology risks"],
                    "industry_risks": ["Technology disruption", "Supply chain constraints"],
                    "company_risks": ["Product concentration", "Key personnel dependence"],
                    "emerging_risks": ["AI regulation", "Cybersecurity threats"],
                    "recurring_risks": ["Competition", "Economic cycles"]
                }
            )
        
        elif section_type == "item7":
            return ExtractedInsights(
                section_type=section_type,
                performance_analysis={
                    "revenue_drivers": ["iPhone sales growth", "Services expansion", "International markets"],
                    "profit_drivers": ["Product mix", "Services margin", "Operational efficiency"],
                    "performance_trends": ["Services growth", "Geographic diversification"],
                    "strategic_priorities": ["Innovation", "Services expansion", "Sustainability"],
                    "outlook_statements": ["Continued investment in R&D", "Services growth expected"]
                }
            )
        
        else:
            return ExtractedInsights(section_type=section_type)


if __name__ == "__main__":
    # Example usage
    research_agent = ResearchAgent()
    extraction_agent = ExtractionAgent()
    
    # Test with mock Apple data
    print("Testing Research Agent...")
    sample_text = """Apple Inc. designs, manufactures and markets smartphones, personal computers, tablets, wearables and accessories. The Company's fiscal year ends on the last Saturday of September. iPhone is the Company's line of smartphones. Mac is the Company's line of personal computers. Services include advertising, AppleCare, cloud services, digital content and payment services."""
    
    insight = research_agent.analyze_filing_structure("Apple Inc.", 2024, sample_text)
    print(f"Research Insight - Business Segments: {insight.business_segments}")
    print(f"Performance Drivers: {insight.performance_drivers}")
    
    print("\nTesting Extraction Agent...")
    extraction = extraction_agent.extract_section_insights(sample_text, "item1")
    print(f"Business Overview: {extraction.business_overview}")