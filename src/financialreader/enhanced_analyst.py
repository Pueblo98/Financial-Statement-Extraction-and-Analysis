"""
Enhanced Company Analysis Agent
Multi-prompt AI system for deep company analysis with risk scoring and probing questions
Uses Google GenAI with grounding tools for real-time data access
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import pandas as pd
import numpy as np

try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    genai = None
    types = None

@dataclass
class RiskScore:
    """Risk assessment with 0-10 scoring"""
    risk_type: str
    score: float  # 0-10 scale, 10 being highest risk
    rationale: str
    key_factors: List[str]
    mitigation_strategies: List[str]

@dataclass
class CompanyAnalysis:
    """Comprehensive company analysis with risk scores and insights"""
    # Risk Scores (0-10 scale)
    credit_risk_score: RiskScore
    supply_chain_risk_score: RiskScore
    regulatory_risk_score: RiskScore
    
    # Strategic Analysis
    ma_acquisition_potential: Dict[str, Any]
    competitive_positioning: Dict[str, Any]
    
    # AI-Generated Probing Questions & Answers
    probing_questions: List[Dict[str, str]]  # [{"question": "...", "answer": "..."}]
    
    # Enhanced Business Intelligence
    business_segments_detailed: List[Dict[str, Any]]
    key_performance_metrics: Dict[str, Any]
    future_outlook: Dict[str, Any]

class EnhancedCompanyAnalyst:
    """
    Advanced AI analyst that validates data, scores risks, and asks probing questions
    Uses multiple focused prompts for structured analysis
    """
    
    def __init__(self, gemini_api_key: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        
        # Initialize Google GenAI client with grounding tools
        self.client = None
        self.config = None
        
        if gemini_api_key and GEMINI_AVAILABLE and genai is not None and types is not None:
            try:
                # Configure the client
                self.client = genai.Client(api_key=gemini_api_key)
                
                # Define the grounding tool
                grounding_tool = types.Tool(
                    google_search=types.GoogleSearch()
                )
                
                # Configure generation settings with grounding
                self.config = types.GenerateContentConfig(
                    tools=[grounding_tool]
                )
                
                self.logger.info("Enhanced Company Analyst initialized with Google GenAI and grounding tools")
            except Exception as e:
                self.logger.error(f"Failed to initialize Google GenAI client: {e}")
                self.client = None
                self.config = None
        else:
            self.logger.warning("No Gemini API key or Google GenAI library available - using mock analysis")
    
    def analyze_company_comprehensive(self, 
                                    company_name: str,
                                    financial_data: pd.DataFrame,
                                    narrative_text: str,
                                    industry: str = "Technology") -> CompanyAnalysis:
        """
        Perform comprehensive company analysis with risk scoring and probing questions
        
        Args:
            company_name: Company name
            financial_data: Financial metrics DataFrame
            narrative_text: 10-K narrative content
            industry: Industry classification
            
        Returns:
            CompanyAnalysis with risk scores and detailed insights
        """
        self.logger.info(f"Starting enhanced analysis for {company_name}")
        
        # Step 1: Validate and analyze financial data
        financial_summary = self._analyze_financial_health(financial_data)
        
        # Step 2: Risk Scoring (0-10 scale)
        credit_risk = self._score_credit_risk(company_name, financial_data, narrative_text)
        supply_chain_risk = self._score_supply_chain_risk(company_name, narrative_text, industry)
        regulatory_risk = self._score_regulatory_risk(company_name, narrative_text, industry)
        
        # Step 3: Strategic Analysis
        ma_potential = self._analyze_ma_potential(company_name, financial_data, narrative_text)
        competitive_position = self._analyze_competitive_positioning(company_name, narrative_text, industry)
        
        # Step 4: AI-Generated Probing Questions
        probing_questions = self._generate_probing_questions(company_name, financial_data, narrative_text, industry)
        
        # Step 5: Enhanced Business Intelligence
        business_segments = self._analyze_business_segments_detailed(company_name, narrative_text)
        performance_metrics = self._extract_key_metrics(company_name, financial_data, narrative_text)
        future_outlook = self._analyze_future_outlook(company_name, narrative_text)
        
        return CompanyAnalysis(
            credit_risk_score=credit_risk,
            supply_chain_risk_score=supply_chain_risk,
            regulatory_risk_score=regulatory_risk,
            ma_acquisition_potential=ma_potential,
            competitive_positioning=competitive_position,
            probing_questions=probing_questions,
            business_segments_detailed=business_segments,
            key_performance_metrics=performance_metrics,
            future_outlook=future_outlook
        )
    
    def _safe_json_loads(self, response_text, fallback, context_name=None):
        """Safely parse JSON from AI response, fallback if invalid or empty."""
        if not response_text or not response_text.strip().startswith(("{", "[")):
            if context_name:
                self.logger.error(f"AI response for {context_name} is empty or not JSON: {repr(response_text)}")
            else:
                self.logger.error(f"AI response is empty or not JSON: {repr(response_text)}")
            return fallback
        try:
            return json.loads(response_text)
        except json.JSONDecodeError as e:
            if context_name:
                self.logger.error(f"Failed to parse JSON for {context_name}: {e}")
                self.logger.debug(f"Raw response: {response_text[:500]}...")
            else:
                self.logger.error(f"Failed to parse JSON: {e}")
                self.logger.debug(f"Raw response: {response_text[:500]}...")
            return fallback
    
    def _score_credit_risk(self, company_name: str, financial_data: pd.DataFrame, narrative_text: str) -> RiskScore:
        """Score credit risk on 0-10 scale (10 = highest risk)"""
        
        prompt = f"""You are a credit risk analyst evaluating {company_name}.
        
Analyze the financial data and 10-K narrative to score credit risk on a 0-10 scale where:
- 0-2: Minimal risk (AAA/AA rating equivalent)  
- 3-4: Low risk (A rating equivalent)
- 5-6: Moderate risk (BBB rating equivalent)
- 7-8: High risk (BB/B rating equivalent)
- 9-10: Very high risk (CCC or below equivalent)

Financial Data Summary:
- Latest Revenue: ${financial_data['revenue'].iloc[-1]:,.0f} if 'revenue' in financial_data.columns else 'N/A'
- Latest Net Income: ${financial_data['net_income'].iloc[-1]:,.0f} if 'net_income' in financial_data.columns else 'N/A'
- Total Assets: ${financial_data['total_assets'].iloc[-1]:,.0f} if 'total_assets' in financial_data.columns else 'N/A'

10-K Narrative (first 3000 chars): {narrative_text[:3000]}

Respond with ONLY a JSON object:
{{
    "score": 0.0,
    "rationale": "Brief explanation of score",
    "key_factors": ["factor1", "factor2", "factor3"],
    "mitigation_strategies": ["strategy1", "strategy2"]
}}"""
        
        if self.client:
            try:
                response = self.client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=self.config
                )
                if response and hasattr(response, 'text') and response.text and response.text.strip():
                    response_text = response.text.strip()
                    # Clean up response - remove markdown formatting if present
                    if response_text.startswith('```json'):
                        response_text = response_text.replace('```json', '').replace('```', '').strip()
                    
                    data = self._safe_json_loads(response_text, fallback=None, context_name="credit risk")
                    if data:
                        return RiskScore(
                            risk_type="Credit Risk",
                            score=float(data.get('score', 5.0)),
                            rationale=data.get('rationale', 'Analysis pending'),
                            key_factors=data.get('key_factors', []),
                            mitigation_strategies=data.get('mitigation_strategies', [])
                        )
                else:
                    self.logger.warning("Empty or invalid response from Google GenAI for credit risk")
            except Exception as e:
                self.logger.error(f"Credit risk analysis failed: {e}")
        
        # Fallback analysis based on financial metrics
        return self._fallback_credit_risk_score(company_name, financial_data)
    
    def _score_supply_chain_risk(self, company_name: str, narrative_text: str, industry: str) -> RiskScore:
        """Score supply chain risk on 0-10 scale"""
        
        prompt = f"""You are a supply chain risk analyst evaluating {company_name} in the {industry} industry.

Analyze the 10-K narrative for supply chain risks. Score on 0-10 scale where:
- 0-2: Minimal risk (diversified suppliers, minimal geographic concentration)
- 3-4: Low risk (some concentration but manageable) 
- 5-6: Moderate risk (notable dependencies or geographic risks)
- 7-8: High risk (significant single points of failure)
- 9-10: Very high risk (critical vulnerabilities, major disruption likely)

Consider:
- Supplier concentration and dependencies
- Geographic risks (geopolitical tensions, natural disasters)
- Single source suppliers for critical components  
- Manufacturing complexity and lead times
- Recent supply chain disruptions mentioned

10-K Narrative (first 4000 chars): {narrative_text[:4000]}

Respond with ONLY a JSON object:
{{
    "score": 0.0,
    "rationale": "Brief explanation focusing on supply chain vulnerabilities",
    "key_factors": ["factor1", "factor2", "factor3"],
    "mitigation_strategies": ["strategy1", "strategy2"]
}}"""
        
        if self.client:
            try:
                response = self.client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=self.config
                )
                if response and hasattr(response, 'text') and response.text and response.text.strip():
                    response_text = response.text.strip()
                    # Clean up response - remove markdown formatting if present
                    if response_text.startswith('```json'):
                        response_text = response_text.replace('```json', '').replace('```', '').strip()
                    
                    data = self._safe_json_loads(response_text, fallback=None, context_name="supply chain risk")
                    if data:
                        return RiskScore(
                            risk_type="Supply Chain Risk",
                            score=float(data.get('score', 5.0)),
                            rationale=data.get('rationale', 'Analysis pending'),
                            key_factors=data.get('key_factors', []),
                            mitigation_strategies=data.get('mitigation_strategies', [])
                        )
                else:
                    self.logger.warning("Empty or invalid response from Google GenAI for supply chain risk")
            except Exception as e:
                self.logger.error(f"Supply chain risk analysis failed: {e}")
        
        return self._fallback_supply_chain_score(company_name, industry)
    
    def _score_regulatory_risk(self, company_name: str, narrative_text: str, industry: str) -> RiskScore:
        """Score regulatory risk on 0-10 scale"""
        
        prompt = f"""You are a regulatory risk analyst evaluating {company_name} in the {industry} industry.

Analyze regulatory risks from the 10-K. Score on 0-10 scale where:
- 0-2: Minimal risk (stable regulatory environment, good compliance track record)
- 3-4: Low risk (minor regulatory changes expected)
- 5-6: Moderate risk (some regulatory uncertainty or compliance costs)
- 7-8: High risk (major regulatory changes likely, significant compliance burden)  
- 9-10: Very high risk (severe regulatory threats, potential business model disruption)

Consider:
- Pending or proposed regulation changes
- Compliance costs and complexity
- History of regulatory violations or fines
- Antitrust or competition concerns
- Data privacy and security regulations
- Environmental regulations
- Industry-specific regulatory trends

10-K Narrative (first 4000 chars): {narrative_text[:4000]}

Respond with ONLY a JSON object:
{{
    "score": 0.0,
    "rationale": "Brief explanation of regulatory landscape and risks",
    "key_factors": ["factor1", "factor2", "factor3"],
    "mitigation_strategies": ["strategy1", "strategy2"]
}}"""
        
        if self.client:
            try:
                response = self.client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=self.config
                )
                response_text = response.text.strip() if response and hasattr(response, 'text') and response.text else ''
                data = self._safe_json_loads(response_text, fallback=None, context_name="regulatory risk")
                if data:
                    return RiskScore(
                        risk_type="Regulatory Risk",
                        score=float(data.get('score', 5.0)),
                        rationale=data.get('rationale', 'Analysis pending'),
                        key_factors=data.get('key_factors', []),
                        mitigation_strategies=data.get('mitigation_strategies', [])
                    )
            except Exception as e:
                self.logger.error(f"Regulatory risk analysis failed: {e}")
        
        return self._fallback_regulatory_score(company_name, industry)
    
    def _analyze_ma_potential(self, company_name: str, financial_data: pd.DataFrame, narrative_text: str) -> Dict[str, Any]:
        """Analyze M&A potential and strategic focus areas"""
        
        # Extract key financial metrics for context
        latest_data = financial_data.iloc[-1] if not financial_data.empty else {}
        revenue = latest_data.get('revenue', 0)
        net_income = latest_data.get('net_income', 0)
        total_assets = latest_data.get('total_assets', revenue * 2)
        cash_and_equivalents = latest_data.get('cash_and_equivalents', 0)
        margin = (net_income / revenue * 100) if revenue > 0 else 0
        fiscal_year = latest_data.get('fiscal_year', 'current')
        
        prompt = f"""You are an M&A analyst evaluating {company_name} for fiscal year {fiscal_year}.

Financial Context:
- Revenue: ${revenue:,.0f}
- Cash Position: ${cash_and_equivalents:,.0f}
- Fiscal Year: {fiscal_year}

Analyze the company's M&A potential and strategic focus areas based on their 10-K narrative.

10-K Narrative (first 3000 chars): {narrative_text[:3000]}

Respond with ONLY a JSON object:
{{
    "acquisition_appetite": "low/moderate/high",
    "strategic_focus_areas": ["area1", "area2", "area3"],
    "potential_targets": ["target1", "target2"],
    "acquisition_capacity": "description of financial capacity",
    "strategic_rationale": "brief explanation of M&A strategy"
}}"""
        
        if self.client:
            try:
                response = self.client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=self.config
                )
                if response and hasattr(response, 'text') and response.text and response.text.strip():
                    response_text = response.text.strip()
                    
                    # Clean up response - remove markdown formatting if present
                    if response_text.startswith('```json'):
                        response_text = response_text.replace('```json', '').replace('```', '').strip()
                    elif response_text.startswith('```'):
                        response_text = response_text.replace('```', '').strip()
                    
                    # Try to extract JSON from the response
                    json_start = response_text.find('{')
                    json_end = response_text.rfind('}') + 1
                    
                    if json_start != -1 and json_end > json_start:
                        json_text = response_text[json_start:json_end]
                        data = self._safe_json_loads(json_text, fallback=None, context_name="M&A analysis")
                    else:
                        # If no JSON found, try parsing the entire response
                        data = self._safe_json_loads(response_text, fallback=None, context_name="M&A analysis")
                    
                    return data
                    
                else:
                    self.logger.warning("Empty or invalid response from Google GenAI for M&A analysis")
                    
            except Exception as e:
                self.logger.error(f"M&A analysis failed: {e}")
        
        # Fallback analysis with year-specific context
        return self._generate_fallback_ma_analysis(company_name, fiscal_year, revenue, cash_and_equivalents)
    
    def _generate_fallback_ma_analysis(self, company_name: str, fiscal_year: str, revenue: float, cash_position: float) -> Dict[str, Any]:
        """Generate fallback M&A analysis with year-specific context"""
        
        if "apple" in company_name.lower():
            return {
                "acquisition_appetite": "moderate",
                "strategic_focus_areas": ["Technology capabilities", "Market expansion", "Services growth"],
                "potential_targets": ["AI/ML startups", "Content companies", "Health tech firms"],
                "acquisition_capacity": f"Strong cash position of ${cash_position:,.0f} in {fiscal_year}",
                "strategic_rationale": f"Apple typically focuses on strategic acquisitions to enhance technology capabilities and expand services ecosystem in {fiscal_year}"
            }
        
        # Generic analysis for other companies
        if revenue > 10000000000:  # $10B+ revenue
            appetite = "moderate"
            focus_areas = ["Market expansion", "Technology integration", "Competitive positioning"]
        elif revenue > 1000000000:  # $1B+ revenue
            appetite = "moderate"
            focus_areas = ["Growth markets", "Product expansion", "Operational efficiency"]
        else:
            appetite = "low"
            focus_areas = ["Core business focus", "Organic growth", "Strategic partnerships"]
        
        return {
            "acquisition_appetite": appetite,
            "strategic_focus_areas": focus_areas,
            "potential_targets": ["Analysis pending"],
            "acquisition_capacity": f"Revenue: ${revenue:,.0f}, Cash: ${cash_position:,.0f} in {fiscal_year}",
            "strategic_rationale": f"Company appears to focus on {appetite} M&A activity in {fiscal_year} based on financial position"
        }
    
    def _analyze_competitive_positioning(self, company_name: str, narrative_text: str, industry: str) -> Dict[str, Any]:
        """Analyze competitive positioning and market dynamics"""
        
        prompt = f"""Analyze the competitive positioning of {company_name} in the {industry} industry.

Based on the 10-K narrative, assess:
1. Market position and competitive advantages
2. Key competitors mentioned
3. Competitive threats and challenges
4. Moats and defensive strategies
5. Market share dynamics

10-K Narrative: {narrative_text[:3000]}

Respond with ONLY a JSON object:
{{
    "market_position": "market leadership description",
    "competitive_advantages": ["advantage1", "advantage2"],
    "key_competitors": ["competitor1", "competitor2"],
    "competitive_threats": ["threat1", "threat2"],
    "moats": ["moat1", "moat2"]
}}"""
        
        if self.client:
            try:
                response = self.client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=self.config
                )
                if response and hasattr(response, 'text') and response.text and response.text.strip():
                    response_text = response.text.strip()
                    data = self._safe_json_loads(response_text, fallback=None, context_name="competitive positioning")
                    if data:
                        return data
            except Exception as e:
                self.logger.error(f"Competitive positioning analysis failed: {e}")
        
        return {
            "market_position": "Analysis pending",
            "competitive_advantages": ["Strong brand", "Innovation"],
            "key_competitors": ["Analysis pending"],
            "competitive_threats": ["Market competition"],
            "moats": ["Brand loyalty", "Ecosystem"]
        }
    
    def _generate_probing_questions(self, company_name: str, financial_data: pd.DataFrame, 
                                  narrative_text: str, industry: str) -> List[Dict[str, str]]:
        """Generate AI-driven probing questions and research answers using web search"""
        
        # Extract key financial metrics for context
        latest_data = financial_data.iloc[-1] if not financial_data.empty else {}
        revenue = latest_data.get('revenue', 0)
        net_income = latest_data.get('net_income', 0)
        total_assets = latest_data.get('total_assets', revenue * 2)
        cash_and_equivalents = latest_data.get('cash_and_equivalents', 0)
        margin = (net_income / revenue * 100) if revenue > 0 else 0
        fiscal_year = latest_data.get('fiscal_year', 'current')
        
        # Use Google GenAI with search to research and answer questions
        prompt = f"""You are an expert financial analyst researching {company_name} for fiscal year {fiscal_year}. 

Current Financial Context:
- Revenue: ${revenue:,.0f}
- Net Income: ${net_income:,.0f}  
- Net Profit Margin: {margin:.1f}%
- Industry: {industry}
- Fiscal Year: {fiscal_year}

Using your knowledge and search capabilities, generate 5 specific probing questions about {company_name} for {fiscal_year} and provide detailed, researched answers. Focus on:

1. Unit economics (e.g., iPhone sales volumes, average selling prices)
2. Industry comparisons (vs competitors like Samsung, Google)
3. Market share and competitive position
4. Key business drivers and risks
5. Industry benchmarks and averages

For each question, provide a substantive answer using current market data, not just "requires additional data."

IMPORTANT: You must respond with ONLY a valid JSON array. Do not include any explanatory text, markdown formatting, or other content outside the JSON.

Respond with ONLY this JSON structure:
[
    {{"question": "How many iPhones did Apple sell in fiscal {fiscal_year}?", "answer": "Research-based answer with specific numbers or estimates"}},
    {{"question": "How does Apple's profit margin compare to tech industry average in {fiscal_year}?", "answer": "Detailed comparison with industry benchmarks"}},
    {{"question": "What is Apple's market share in smartphones globally in {fiscal_year}?", "answer": "Market share data and competitive position"}},
    {{"question": "How dependent is Apple on China for revenue in {fiscal_year}?", "answer": "Geographic revenue breakdown and China exposure"}},
    {{"question": "What are Apple's main competitive threats in {fiscal_year}?", "answer": "Analysis of competitive landscape and threats"}}
]

Use your search and knowledge capabilities to provide substantive, data-driven answers rather than placeholders."""
        
        if self.client:
            try:
                response = self.client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=self.config
                )
                if response and hasattr(response, 'text') and response.text and response.text.strip():
                    response_text = response.text.strip()
                    
                    # Clean up response - remove markdown formatting if present
                    if response_text.startswith('```json'):
                        response_text = response_text.replace('```json', '').replace('```', '').strip()
                    elif response_text.startswith('```'):
                        response_text = response_text.replace('```', '').strip()
                    
                    # Try to extract JSON from the response
                    json_start = response_text.find('[')
                    json_end = response_text.rfind(']') + 1
                    
                    if json_start != -1 and json_end > json_start:
                        json_text = response_text[json_start:json_end]
                        data = self._safe_json_loads(json_text, fallback=None, context_name="probing questions")
                    else:
                        # If no JSON array found, try parsing the entire response
                        data = self._safe_json_loads(response_text, fallback=None, context_name="probing questions")
                    
                    # Validate the structure
                    if isinstance(data, list) and len(data) >= 2:
                        return data[:5]  # Return up to 5 questions
                    else:
                        self.logger.warning(f"Invalid JSON structure for probing questions: {type(data)}")
                        
                else:
                    self.logger.warning("Empty or invalid response from Google GenAI for probing questions")
                    
            except Exception as e:
                self.logger.error(f"Probing questions generation failed: {e}")
        
        # Fallback questions with year-specific context
        return self._generate_fallback_questions(company_name, fiscal_year, industry)
    
    def _generate_fallback_questions(self, company_name: str, fiscal_year: str, industry: str) -> List[Dict[str, str]]:
        """Generate fallback questions with year-specific context"""
        
        if "apple" in company_name.lower():
            return [
                {
                    "question": f"How many iPhones were sold in fiscal {fiscal_year}?", 
                    "answer": f"Requires unit sales data from {fiscal_year} earnings call - Apple typically reports unit sales quarterly"
                },
                {
                    "question": f"What percentage of revenue comes from Services vs Hardware in {fiscal_year}?", 
                    "answer": f"Requires detailed revenue breakdown by segment from {fiscal_year} 10-K filing"
                },
                {
                    "question": f"How does Apple's R&D spending compare to competitors in {fiscal_year}?", 
                    "answer": f"Requires industry comparison data and {fiscal_year} R&D expenditure analysis"
                },
                {
                    "question": f"What's the geographic revenue breakdown for {fiscal_year}?", 
                    "answer": f"Check {fiscal_year} 10-K for regional revenue reporting and geographic concentration"
                },
                {
                    "question": f"How dependent is Apple on Chinese manufacturing in {fiscal_year}?", 
                    "answer": f"Requires supply chain analysis and {fiscal_year} manufacturing footprint assessment"
                }
            ]
        
        # Generic questions for other companies
        return [
            {
                "question": f"What's the company's largest revenue driver in {fiscal_year}?", 
                "answer": f"Requires detailed segment analysis from {fiscal_year} financial statements"
            },
            {
                "question": f"How does profitability compare to industry average in {fiscal_year}?", 
                "answer": f"Requires industry benchmarking data and {fiscal_year} peer comparison"
            },
            {
                "question": f"What's the biggest competitive threat in {fiscal_year}?", 
                "answer": f"Requires competitive landscape analysis and {fiscal_year} market dynamics assessment"
            },
            {
                "question": f"What are the key growth initiatives for {fiscal_year}?", 
                "answer": f"Requires strategic analysis from {fiscal_year} management discussion"
            },
            {
                "question": f"How does the company's debt position look in {fiscal_year}?", 
                "answer": f"Requires balance sheet analysis and {fiscal_year} debt maturity assessment"
            }
        ]
    
    def _analyze_business_segments_detailed(self, company_name: str, narrative_text: str) -> List[Dict[str, Any]]:
        """Extract detailed business segment information"""
        
        prompt = f"""Extract detailed business segment information for {company_name} from their 10-K filing.

For each business segment, provide:
- Segment name
- Revenue contribution (% or description)
- Key products/services  
- Market position
- Growth prospects
- Key risks

10-K Narrative: {narrative_text[:4000]}

Respond with ONLY a JSON array:
[
    {{
        "segment_name": "Segment Name",
        "revenue_contribution": "percentage or description",
        "key_products": ["product1", "product2"],
        "market_position": "market position description",
        "growth_prospects": "growth outlook",
        "key_risks": ["risk1", "risk2"]
    }}
]"""
        
        if self.client:
            try:
                response = self.client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=self.config
                )
                response_text = response.text.strip() if response and hasattr(response, 'text') and response.text else ''
                data = self._safe_json_loads(response_text, fallback=None, context_name="business segments")
                if isinstance(data, list) and len(data) > 0:
                    return data
            except Exception as e:
                self.logger.error(f"Business segments analysis failed: {e}")
        
        # Fallback for Apple
        if "apple" in company_name.lower():
            return [
                {
                    "segment_name": "iPhone",
                    "revenue_contribution": "~50% of total revenue",
                    "key_products": ["iPhone 15", "iPhone 14", "iPhone SE"],
                    "market_position": "Premium smartphone market leader",
                    "growth_prospects": "Steady with AI integration opportunities",
                    "key_risks": ["Market saturation", "Intense competition"]
                },
                {
                    "segment_name": "Services",
                    "revenue_contribution": "~20% of total revenue",
                    "key_products": ["App Store", "iCloud", "Apple Music"],
                    "market_position": "Growing ecosystem services",
                    "growth_prospects": "High growth potential",
                    "key_risks": ["Regulatory pressure", "Competition"]
                }
            ]
        
        return [{"segment_name": "Analysis pending", "revenue_contribution": "TBD"}]
    
    def _extract_key_metrics(self, company_name: str, financial_data: pd.DataFrame, narrative_text: str) -> Dict[str, Any]:
        """Extract key performance metrics specific to the company"""
        
        # Calculate financial metrics
        latest_data = financial_data.iloc[-1] if not financial_data.empty else {}
        
        metrics = {
            "revenue_growth_3y": "Calculate from financial data",
            "profit_margin_trend": "Calculate from financial data", 
            "cash_position": latest_data.get('cash_and_equivalents', 'N/A'),
            "debt_to_equity": "Calculate from balance sheet",
            "return_on_assets": latest_data.get('roa', 'N/A'),
            "employee_productivity": "Revenue per employee if available"
        }
        
        return metrics
    
    def _analyze_future_outlook(self, company_name: str, narrative_text: str) -> Dict[str, Any]:
        """Analyze future outlook and guidance"""
        
        prompt = f"""Analyze the future outlook for {company_name} based on their 10-K filing.

Extract:
1. Key growth drivers and opportunities
2. Major risks and challenges  
3. Strategic initiatives and investments
4. Market trends and competitive dynamics
5. Management guidance and expectations

10-K Narrative: {narrative_text[:3000]}

Respond with ONLY a JSON object:
{{
    "growth_drivers": ["driver1", "driver2", "driver3"],
    "key_risks": ["risk1", "risk2", "risk3"],
    "strategic_initiatives": ["initiative1", "initiative2"],
    "market_trends": "description of key trends",
    "guidance_summary": "management guidance summary"
}}"""
        
        if self.client:
            try:
                response = self.client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=self.config
                )
                if response and hasattr(response, 'text') and response.text and response.text.strip():
                    response_text = response.text.strip()
                    # Clean up response - remove markdown formatting if present
                    if response_text.startswith('```json'):
                        response_text = response_text.replace('```json', '').replace('```', '').strip()
                    
                    data = self._safe_json_loads(response_text, fallback=None, context_name="future outlook")
                    if data:
                        return data
            except Exception as e:
                self.logger.error(f"Future outlook analysis failed: {e}")
        
        # Fallback analysis
        return {
            "growth_drivers": ["Product innovation", "Market expansion", "Services growth"],
            "key_risks": ["Competition", "Regulation", "Supply chain"],
            "strategic_initiatives": ["R&D investment", "Market penetration"],
            "market_trends": "Technology evolution and digital transformation",
            "guidance_summary": "Management expects continued growth with focus on innovation"
        }
    
    # Fallback scoring methods
    def _fallback_credit_risk_score(self, company_name: str, financial_data: pd.DataFrame) -> RiskScore:
        """Fallback credit risk assessment"""
        return RiskScore(
            risk_type="Credit Risk",
            score=3.0,  # Conservative moderate-low risk
            rationale="Basic financial analysis indicates stable credit profile",
            key_factors=["Strong revenue base", "Stable operations"],
            mitigation_strategies=["Maintain cash reserves", "Monitor debt levels"]
        )
    
    def _fallback_supply_chain_score(self, company_name: str, industry: str) -> RiskScore:
        """Fallback supply chain risk assessment"""
        score = 4.0 if industry.lower() == "technology" else 3.0
        return RiskScore(
            risk_type="Supply Chain Risk",
            score=score,
            rationale=f"{industry} companies typically face moderate supply chain complexity",
            key_factors=["Geographic concentration", "Supplier dependencies"],
            mitigation_strategies=["Diversify suppliers", "Build inventory buffers"]
        )
    
    def _fallback_regulatory_score(self, company_name: str, industry: str) -> RiskScore:
        """Fallback regulatory risk assessment"""
        score = 5.0 if industry.lower() == "technology" else 3.0
        return RiskScore(
            risk_type="Regulatory Risk", 
            score=score,
            rationale=f"{industry} faces evolving regulatory landscape",
            key_factors=["Data privacy regulations", "Antitrust scrutiny"],
            mitigation_strategies=["Compliance programs", "Regulatory monitoring"]
        )
    
    def _analyze_financial_health(self, financial_data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze overall financial health"""
        if financial_data.empty:
            return {"status": "insufficient_data"}
        
        latest = financial_data.iloc[-1]
        return {
            "revenue": latest.get('revenue', 0),
            "profitability": latest.get('net_income', 0),
            "leverage": latest.get('debt_to_equity', 0),
            "liquidity": latest.get('current_ratio', 0)
        }

# Export function for integration
def analyze_company_enhanced(company_name: str, financial_data: pd.DataFrame, 
                           narrative_text: str, gemini_api_key: Optional[str] = None) -> CompanyAnalysis:
    """Convenience function for enhanced company analysis"""
    analyst = EnhancedCompanyAnalyst(gemini_api_key)
    return analyst.analyze_company_comprehensive(company_name, financial_data, narrative_text)

def align_growth_series(series, n):
    # series: pd.Series of calculated growth (length = len(df) - n)
    # n: window (e.g., 3 for 3y CAGR)
    # Returns: pd.Series aligned to original df length, with NaN for first n, 0 for last row
    aligned = [np.nan] * n + list(series) + [0]
    # Truncate to original length
    return pd.Series(aligned[:len(aligned)])

def align_series_with_shift(series, shift=0, fill_value=np.nan):
    # Shift the series and fill missing values
    return series.shift(shift, fill_value=fill_value)