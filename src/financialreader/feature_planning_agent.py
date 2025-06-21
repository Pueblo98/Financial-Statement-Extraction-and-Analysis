"""
Feature Planning Agent for AI-Powered Feature Engineering Recommendations
Analyzes financial datasets and recommends advanced feature engineering opportunities
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import pandas as pd

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    genai = None

@dataclass
class FeatureRecommendation:
    """Single feature engineering recommendation"""
    feature_name: str
    feature_type: str  # 'transformation', 'interaction', 'narrative', 'temporal'
    description: str
    implementation: str
    priority: int  # 1-5, 5 being highest
    complexity: str  # 'low', 'medium', 'high'
    expected_value: str
    data_requirements: List[str]

@dataclass
class FeaturePlan:
    """Complete feature engineering plan from AI agent"""
    dataset_summary: Dict[str, Any]
    transformation_features: List[FeatureRecommendation]
    interaction_features: List[FeatureRecommendation]
    narrative_features: List[FeatureRecommendation]
    temporal_features: List[FeatureRecommendation]
    quality_recommendations: List[str]
    implementation_priority: List[str]

class FeaturePlanningAgent:
    """
    AI agent that analyzes financial datasets and recommends feature engineering strategies
    Uses Google Gemini to understand data patterns and suggest advanced transformations
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        
        if not GEMINI_AVAILABLE:
            self.logger.warning("Google Generative AI not available. Using mock responses.")
            self.model = None
            return
        
        # Configure Gemini
        api_key = api_key or os.getenv('GEMINI_API_KEY')
        if not api_key:
            self.logger.warning("No Gemini API key found. Using mock responses.")
            self.model = None
            return
        
        try:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel('gemini-2.0-flash')
            self.logger.info("Feature Planning Agent initialized with Gemini 2.0 Flash")
        except Exception as e:
            self.logger.error(f"Failed to initialize Gemini: {e}")
            self.model = None
    
    def analyze_dataset_and_recommend_features(self, 
                                            financial_data: pd.DataFrame,
                                            performance_data: pd.DataFrame,
                                            narrative_data: Optional[pd.DataFrame] = None,
                                            analysis_goals: str = "predictive modeling") -> FeaturePlan:
        """
        Analyze complete dataset and generate comprehensive feature engineering plan
        
        Args:
            financial_data: Raw financial statements DataFrame
            performance_data: Performance analytics DataFrame  
            narrative_data: Narrative insights DataFrame (optional)
            analysis_goals: Target use case for feature engineering
            
        Returns:
            FeaturePlan with AI-recommended feature engineering strategies
        """
        self.logger.info("Analyzing dataset for feature engineering opportunities")
        
        # Generate dataset summary
        dataset_summary = self._generate_dataset_summary(financial_data, performance_data, narrative_data)
        
        # Get AI recommendations
        if self.model:
            try:
                ai_recommendations = self._get_ai_feature_recommendations(dataset_summary, analysis_goals)
            except Exception as e:
                self.logger.error(f"AI feature recommendation failed: {e}")
                ai_recommendations = self._get_mock_feature_recommendations(dataset_summary)
        else:
            ai_recommendations = self._get_mock_feature_recommendations(dataset_summary)
        
        # Parse and structure recommendations
        feature_plan = self._parse_ai_recommendations(ai_recommendations, dataset_summary)
        
        self.logger.info(f"Generated feature plan with {len(feature_plan.transformation_features + feature_plan.interaction_features + feature_plan.narrative_features + feature_plan.temporal_features)} recommendations")
        
        return feature_plan
    
    def _generate_dataset_summary(self, financial_data: pd.DataFrame, 
                                performance_data: pd.DataFrame,
                                narrative_data: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """Generate comprehensive summary of available dataset"""
        
        summary = {
            'financial_metrics': {
                'shape': financial_data.shape,
                'columns': list(financial_data.columns),
                'years_available': sorted(financial_data['fiscal_year'].unique()) if 'fiscal_year' in financial_data.columns else [],
                'companies': financial_data['company_name'].nunique() if 'company_name' in financial_data.columns else 1,
                'completeness': (financial_data.notna().sum() / len(financial_data)).to_dict(),
                'key_metrics': ['revenue', 'net_income', 'total_assets', 'shareholders_equity', 'operating_cash_flow']
            },
            'performance_metrics': {
                'shape': performance_data.shape,
                'columns': list(performance_data.columns),
                'derived_metrics_count': len([col for col in performance_data.columns if col not in ['company_cik', 'company_name', 'fiscal_year']]),
                'completeness': (performance_data.notna().sum() / len(performance_data)).to_dict()
            },
            'narrative_data': {
                'available': narrative_data is not None,
                'shape': narrative_data.shape if narrative_data is not None else None,
                'columns': list(narrative_data.columns) if narrative_data is not None else []
            },
            'data_quality': {
                'financial_completeness': financial_data.notna().mean().mean(),
                'performance_completeness': performance_data.notna().mean().mean(),
                'temporal_coverage': len(financial_data['fiscal_year'].unique()) if 'fiscal_year' in financial_data.columns else 0
            }
        }
        
        return summary
    
    def _get_ai_feature_recommendations(self, dataset_summary: Dict[str, Any], analysis_goals: str) -> str:
        """Get feature engineering recommendations from Gemini AI"""
        
        prompt = f"""You are a quantitative analyst and feature engineering expert reviewing a financial dataset for advanced modeling.

DATASET OVERVIEW:
- Financial Data: {dataset_summary['financial_metrics']['shape'][0]} records, {dataset_summary['financial_metrics']['shape'][1]} columns
- Years Available: {dataset_summary['financial_metrics']['years_available']}
- Key Metrics: {dataset_summary['financial_metrics']['key_metrics']}
- Performance Analytics: {dataset_summary['performance_metrics']['derived_metrics_count']} derived metrics
- Data Completeness: {dataset_summary['data_quality']['financial_completeness']:.1%} financial, {dataset_summary['data_quality']['performance_completeness']:.1%} performance
- Narrative Data: {'Available' if dataset_summary['narrative_data']['available'] else 'Not Available'}

TARGET ANALYSIS: {analysis_goals}

Please recommend advanced feature engineering strategies in the following categories:

TRANSFORMATION FEATURES (mathematical transformations of existing metrics):
- Ratios and normalized metrics not already calculated
- Log transformations for skewed distributions  
- Volatility measures (rolling standard deviations)
- Momentum indicators and rate-of-change calculations
- Industry-relative metrics (percentile rankings)

INTERACTION FEATURES (cross-metric relationships):
- Multiplicative interactions between key metrics
- Revenue growth × Innovation intensity relationships
- Margin expansion × Scale effects
- Risk-adjusted performance metrics
- Capital efficiency × Growth sustainability

TEMPORAL FEATURES (time-series patterns):
- Rolling averages (3-year, 5-year) for trend smoothing
- Lagged variables for trend analysis  
- Seasonal/cyclical adjustments
- Momentum and acceleration metrics
- Change-in-change calculations

NARRATIVE FEATURES (if narrative data available):
- Sentiment scoring of MD&A sections
- Risk factor categorization and intensity scoring
- Strategic theme extraction (digital transformation, ESG, etc.)
- Forward-looking statement analysis
- Management confidence indicators

For each recommendation, provide:
1. Feature name and description
2. Implementation approach
3. Priority level (1-5)
4. Expected predictive value
5. Data requirements

Respond in valid JSON format with the structure:
{{
  "transformation_features": [{{
    "feature_name": "string",
    "description": "string", 
    "implementation": "string",
    "priority": number,
    "complexity": "low|medium|high",
    "expected_value": "string",
    "data_requirements": ["string"]
  }}],
  "interaction_features": [...],
  "temporal_features": [...],
  "narrative_features": [...],
  "quality_recommendations": ["string"],
  "implementation_priority": ["string"]
}}

Focus on features that would be valuable for {analysis_goals} with this financial dataset."""

        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            self.logger.error(f"Gemini API call failed: {e}")
            raise
    
    def _get_mock_feature_recommendations(self, dataset_summary: Dict[str, Any]) -> str:
        """Fallback mock recommendations when AI is unavailable"""
        
        mock_response = {
            "transformation_features": [
                {
                    "feature_name": "revenue_volatility_3y",
                    "feature_type": "transformation",
                    "description": "3-year rolling standard deviation of revenue to measure business stability",
                    "implementation": "Calculate rolling std of revenue over 3-year windows",
                    "priority": 4,
                    "complexity": "low",
                    "expected_value": "High predictive value for risk assessment and valuation",
                    "data_requirements": ["revenue", "fiscal_year"]
                },
                {
                    "feature_name": "margin_momentum",
                    "feature_type": "transformation",
                    "description": "Rate of change in operating margin to capture profitability trends",
                    "implementation": "Calculate (current_margin - previous_margin) / previous_margin",
                    "priority": 5,
                    "complexity": "low", 
                    "expected_value": "Strong predictor of future profitability performance",
                    "data_requirements": ["operating_margin", "fiscal_year"]
                },
                {
                    "feature_name": "asset_efficiency_percentile",
                    "feature_type": "transformation",
                    "description": "Industry percentile ranking of asset turnover ratio",
                    "implementation": "Rank asset turnover within industry peer group",
                    "priority": 3,
                    "complexity": "medium",
                    "expected_value": "Useful for relative performance assessment",
                    "data_requirements": ["asset_turnover", "industry_classification"]
                }
            ],
            "interaction_features": [
                {
                    "feature_name": "growth_quality_score",
                    "feature_type": "interaction",
                    "description": "Revenue growth rate multiplied by cash conversion ratio",
                    "implementation": "revenue_growth_yoy * cash_conversion_ratio",
                    "priority": 5,
                    "complexity": "low",
                    "expected_value": "Distinguishes sustainable vs unsustainable growth",
                    "data_requirements": ["revenue_growth_yoy", "cash_conversion_ratio"]
                },
                {
                    "feature_name": "leverage_profitability_ratio",
                    "feature_type": "interaction",
                    "description": "Interaction between debt levels and return on equity",
                    "implementation": "debt_to_equity * roe",
                    "priority": 4,
                    "complexity": "low",
                    "expected_value": "Captures leverage amplification effects on returns",
                    "data_requirements": ["debt_to_equity", "roe"]
                }
            ],
            "temporal_features": [
                {
                    "feature_name": "revenue_3y_avg",
                    "feature_type": "temporal",
                    "description": "3-year rolling average revenue for trend smoothing",
                    "implementation": "Calculate rolling mean of revenue over 3-year windows",
                    "priority": 3,
                    "complexity": "low",
                    "expected_value": "Reduces noise in revenue trend analysis",
                    "data_requirements": ["revenue", "fiscal_year"]
                },
                {
                    "feature_name": "margin_lagged_1y",
                    "feature_type": "temporal",
                    "description": "Previous year operating margin for trend analysis",
                    "implementation": "Shift operating_margin by 1 year",
                    "priority": 4,
                    "complexity": "low", 
                    "expected_value": "Helps capture margin persistence patterns",
                    "data_requirements": ["operating_margin", "fiscal_year"]
                }
            ],
            "narrative_features": [
                {
                    "feature_name": "risk_sentiment_score",
                    "feature_type": "narrative",
                    "description": "Sentiment analysis of risk factor section language",
                    "implementation": "NLP sentiment scoring of risk factors text",
                    "priority": 3,
                    "complexity": "high",
                    "expected_value": "May predict management outlook and risk perception",
                    "data_requirements": ["risk_factors_text"]
                }
            ],
            "quality_recommendations": [
                "Implement outlier detection for extreme ratio values",
                "Add data validation for logical constraints (e.g., positive revenue)",
                "Create completeness scoring by metric and time period"
            ],
            "implementation_priority": [
                "Start with transformation features (lowest complexity)",
                "Implement interaction features for growth/quality metrics",
                "Add temporal features for trend analysis",
                "Consider narrative features if text data is clean"
            ]
        }
        
        return json.dumps(mock_response, indent=2)
    
    def _parse_ai_recommendations(self, ai_response: str, dataset_summary: Dict[str, Any]) -> FeaturePlan:
        """Parse AI response into structured FeaturePlan"""
        
        try:
            # Clean response and parse JSON
            clean_response = ai_response.strip()
            if clean_response.startswith('```json'):
                clean_response = clean_response[7:]
            if clean_response.endswith('```'):
                clean_response = clean_response[:-3]
            
            recommendations = json.loads(clean_response)
            
            # Convert to FeatureRecommendation objects
            transformation_features = [
                FeatureRecommendation(**rec) for rec in recommendations.get('transformation_features', [])
            ]
            
            interaction_features = [
                FeatureRecommendation(**rec) for rec in recommendations.get('interaction_features', [])
            ]
            
            temporal_features = [
                FeatureRecommendation(**rec) for rec in recommendations.get('temporal_features', [])
            ]
            
            narrative_features = [
                FeatureRecommendation(**rec) for rec in recommendations.get('narrative_features', [])
            ]
            
            return FeaturePlan(
                dataset_summary=dataset_summary,
                transformation_features=transformation_features,
                interaction_features=interaction_features,
                narrative_features=narrative_features,
                temporal_features=temporal_features,
                quality_recommendations=recommendations.get('quality_recommendations', []),
                implementation_priority=recommendations.get('implementation_priority', [])
            )
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse AI recommendations: {e}")
            self.logger.error(f"AI Response: {ai_response[:500]}...")
            
            # Return basic fallback plan
            return FeaturePlan(
                dataset_summary=dataset_summary,
                transformation_features=[],
                interaction_features=[],
                narrative_features=[],
                temporal_features=[],
                quality_recommendations=["Error parsing AI recommendations"],
                implementation_priority=["Manual feature engineering required"]
            )
    
    def get_feature_plan_summary(self, feature_plan: FeaturePlan) -> Dict[str, Any]:
        """Generate summary report of feature plan"""
        
        all_features = (feature_plan.transformation_features + 
                       feature_plan.interaction_features + 
                       feature_plan.temporal_features + 
                       feature_plan.narrative_features)
        
        return {
            'total_features_recommended': len(all_features),
            'features_by_type': {
                'transformation': len(feature_plan.transformation_features),
                'interaction': len(feature_plan.interaction_features),
                'temporal': len(feature_plan.temporal_features),
                'narrative': len(feature_plan.narrative_features)
            },
            'priority_distribution': {
                f'priority_{i}': len([f for f in all_features if f.priority == i])
                for i in range(1, 6)
            },
            'complexity_distribution': {
                'low': len([f for f in all_features if f.complexity == 'low']),
                'medium': len([f for f in all_features if f.complexity == 'medium']),
                'high': len([f for f in all_features if f.complexity == 'high'])
            },
            'high_priority_features': [
                f.feature_name for f in all_features if f.priority >= 4
            ],
            'implementation_recommendations': feature_plan.implementation_priority,
            'data_quality_recommendations': feature_plan.quality_recommendations
        }


if __name__ == "__main__":
    # Example usage and testing
    import sys
    import os
    sys.path.append('../../src')
    
    from financialreader.xbrl_parser import XBRLFinancialParser
    from financialreader.performance_analytics import PerformanceAnalyticsEngine
    from financialreader.edgar_client import SECEdgarClient, CompanyLookup
    
    print("Testing Feature Planning Agent...")
    
    # Initialize components
    client = SECEdgarClient("Financial Analysis Tool test@example.com")
    parser = XBRLFinancialParser(client)
    analytics_engine = PerformanceAnalyticsEngine()
    agent = FeaturePlanningAgent()
    
    # Get sample data for Apple
    lookup = CompanyLookup(client)
    apple_cik = lookup.get_cik_by_ticker('AAPL')
    
    if apple_cik:
        print(f"Testing with Apple CIK: {apple_cik}")
        
        # Extract sample data
        financial_statements = parser.extract_company_financials(apple_cik, years=3)
        financial_df = parser.to_dataframe(financial_statements)
        
        performance_metrics = analytics_engine.calculate_performance_metrics(financial_df)
        performance_df = analytics_engine.to_dataframe(performance_metrics)
        
        print(f"Financial data: {financial_df.shape}")
        print(f"Performance data: {performance_df.shape}")
        
        # Generate feature plan
        feature_plan = agent.analyze_dataset_and_recommend_features(
            financial_data=financial_df,
            performance_data=performance_df,
            analysis_goals="investment risk assessment and return prediction"
        )
        
        # Show results
        summary = agent.get_feature_plan_summary(feature_plan)
        print(f"\nFeature Plan Summary:")
        print(f"  Total features recommended: {summary['total_features_recommended']}")
        print(f"  By type: {summary['features_by_type']}")
        print(f"  High priority features: {summary['high_priority_features']}")
        print(f"  Implementation recommendations: {summary['implementation_recommendations']}")
    else:
        print("Could not find Apple CIK for testing")