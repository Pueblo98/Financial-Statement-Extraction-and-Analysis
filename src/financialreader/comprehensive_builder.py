"""
Comprehensive Financial Data Builder
Creates unified DataFrames with quantitative financial data and qualitative AI insights
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
import logging
from pathlib import Path

from .xbrl_parser import XBRLFinancialParser
from .performance_analytics import PerformanceAnalyticsEngine
from .narrative_pipeline import NarrativeDataPipeline
from .edgar_client import SECEdgarClient, CompanyLookup
from .enhanced_analyst import EnhancedCompanyAnalyst

class ComprehensiveFinancialDataBuilder:
    """
    Builds comprehensive financial datasets combining:
    1. Raw financial statements (quantitative)
    2. Performance analytics (derived quantitative)
    3. AI-extracted narrative insights (qualitative)
    """
    
    def __init__(self, user_agent: str = "Financial Analysis Tool contact@example.com", gemini_api_key: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        
        # Initialize core components
        self.client = SECEdgarClient(user_agent)
        self.parser = XBRLFinancialParser(self.client)
        self.analytics_engine = PerformanceAnalyticsEngine()
        self.narrative_pipeline = NarrativeDataPipeline(self.client, gemini_api_key)
        self.enhanced_analyst = EnhancedCompanyAnalyst(gemini_api_key)
        self.lookup = CompanyLookup(self.client)
    
    def build_comprehensive_dataset(self, ticker: str, years: int = 5) -> pd.DataFrame:
        """
        Build comprehensive dataset for a single company
        
        Args:
            ticker: Company ticker symbol (e.g., 'AAPL')
            years: Number of years of data to extract
            
        Returns:
            Comprehensive DataFrame with quantitative and qualitative data
        """
        self.logger.info(f"Building comprehensive dataset for {ticker}")
        
        # Get company CIK
        cik = self.lookup.get_cik_by_ticker(ticker)
        if not cik:
            raise ValueError(f"Could not find CIK for ticker {ticker}")
        
        company_info = self.lookup.get_company_info(cik)
        company_name = company_info.get('name', ticker)
        
        self.logger.info(f"Processing {company_name} (CIK: {cik})")
        
        # 1. Extract financial statements
        self.logger.info("Extracting financial statements...")
        financial_statements = self.parser.extract_company_financials(cik, years=years)
        financial_df = self.parser.to_dataframe(financial_statements)
        
        if financial_df.empty:
            raise ValueError(f"No financial data found for {ticker}")
        
        # 2. Calculate performance analytics
        self.logger.info("Calculating performance analytics...")
        performance_metrics = self.analytics_engine.calculate_performance_metrics(financial_df)
        performance_df = self.analytics_engine.to_dataframe(performance_metrics)
        
        # 3. Extract narrative insights with AI
        self.logger.info("Extracting narrative insights...")
        narrative_analyses = self.narrative_pipeline.analyze_company_narrative(cik, years=years)
        narrative_df = self.narrative_pipeline.to_dataframe(narrative_analyses)
        
        # 4. Enhanced AI analysis with risk scoring and probing questions
        self.logger.info("Running enhanced AI analysis...")
        enhanced_df = self._run_enhanced_analysis(financial_df, narrative_analyses, company_name)
        
        # 5. Merge all data into comprehensive dataset
        self.logger.info("Merging datasets...")
        comprehensive_df = self._merge_datasets(financial_df, performance_df, narrative_df, enhanced_df)
        
        # 6. Add company metadata
        comprehensive_df['ticker'] = ticker
        comprehensive_df['company_name'] = company_name
        comprehensive_df['data_extraction_date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        
        # Reorder columns for better readability
        comprehensive_df = self._reorder_columns(comprehensive_df)
        
        self.logger.info(f"Comprehensive dataset complete: {comprehensive_df.shape}")
        return comprehensive_df
    
    def _merge_datasets(self, financial_df: pd.DataFrame, performance_df: pd.DataFrame, 
                       narrative_df: pd.DataFrame, enhanced_df: pd.DataFrame = None) -> pd.DataFrame:
        """Merge financial, performance, and narrative datasets"""
        
        # Start with financial data as base
        comprehensive_df = financial_df.copy()
        
        # Merge performance analytics
        merge_keys = ['company_cik', 'fiscal_year']
        if not performance_df.empty and all(key in performance_df.columns for key in merge_keys):
            comprehensive_df = comprehensive_df.merge(
                performance_df, on=merge_keys, how='left', suffixes=('', '_perf')
            )
            # Remove duplicate columns
            comprehensive_df = comprehensive_df.loc[:, ~comprehensive_df.columns.duplicated()]
        
        # Merge narrative insights
        if not narrative_df.empty and all(key in narrative_df.columns for key in merge_keys):
            comprehensive_df = comprehensive_df.merge(
                narrative_df, on=merge_keys, how='left', suffixes=('', '_narr')
            )
            # Remove duplicate columns
            comprehensive_df = comprehensive_df.loc[:, ~comprehensive_df.columns.duplicated()]
        
        # Merge enhanced analysis (risk scores, probing questions)
        if enhanced_df is not None and not enhanced_df.empty and all(key in enhanced_df.columns for key in merge_keys):
            comprehensive_df = comprehensive_df.merge(
                enhanced_df, on=merge_keys, how='left', suffixes=('', '_enhanced')
            )
            # Remove duplicate columns
            comprehensive_df = comprehensive_df.loc[:, ~comprehensive_df.columns.duplicated()]
        
        return comprehensive_df
    
    def _run_enhanced_analysis(self, financial_df: pd.DataFrame, narrative_analyses: List[Any], company_name: str) -> pd.DataFrame:
        """Run enhanced AI analysis with risk scoring and probing questions"""
        
        if not narrative_analyses:
            return pd.DataFrame()
        
        enhanced_rows = []
        
        for analysis in narrative_analyses:
            # Extract narrative text from the analysis
            narrative_text = ""
            for section_id, section in analysis.sections_parsed.items():
                narrative_text += f"{section.content}\n\n"
            
            # Get financial data for this year
            year_financial = financial_df[financial_df['fiscal_year'] == analysis.fiscal_year]
            
            if not year_financial.empty:
                try:
                    # Run enhanced analysis
                    enhanced_analysis = self.enhanced_analyst.analyze_company_comprehensive(
                        company_name, year_financial, narrative_text[:8000]  # Limit text length
                    )
                    
                    # Convert to row format
                    row = {
                        'company_cik': analysis.company_cik,
                        'fiscal_year': analysis.fiscal_year,
                        
                        # Risk Scores (0-10 scale)
                        'credit_risk_score': enhanced_analysis.credit_risk_score.score,
                        'credit_risk_rationale': enhanced_analysis.credit_risk_score.rationale,
                        'supply_chain_risk_score': enhanced_analysis.supply_chain_risk_score.score,
                        'supply_chain_risk_rationale': enhanced_analysis.supply_chain_risk_score.rationale,
                        'regulatory_risk_score': enhanced_analysis.regulatory_risk_score.score,
                        'regulatory_risk_rationale': enhanced_analysis.regulatory_risk_score.rationale,
                        
                        # M&A Analysis
                        'ma_acquisition_appetite': enhanced_analysis.ma_acquisition_potential.get('acquisition_appetite', 'N/A'),
                        'ma_strategic_focus': ', '.join(enhanced_analysis.ma_acquisition_potential.get('strategic_focus_areas', [])),
                        'ma_potential_targets': ', '.join(enhanced_analysis.ma_acquisition_potential.get('potential_targets', [])),
                        
                        # Probing Questions (first 3)
                        'probing_question_1': enhanced_analysis.probing_questions[0]['question'] if len(enhanced_analysis.probing_questions) > 0 else '',
                        'probing_answer_1': enhanced_analysis.probing_questions[0]['answer'] if len(enhanced_analysis.probing_questions) > 0 else '',
                        'probing_question_2': enhanced_analysis.probing_questions[1]['question'] if len(enhanced_analysis.probing_questions) > 1 else '',
                        'probing_answer_2': enhanced_analysis.probing_questions[1]['answer'] if len(enhanced_analysis.probing_questions) > 1 else '',
                        'probing_question_3': enhanced_analysis.probing_questions[2]['question'] if len(enhanced_analysis.probing_questions) > 2 else '',
                        'probing_answer_3': enhanced_analysis.probing_questions[2]['answer'] if len(enhanced_analysis.probing_questions) > 2 else '',
                        
                        # Business Intelligence
                        'detailed_business_segments': str(enhanced_analysis.business_segments_detailed)[:500],  # Truncate for Excel
                        'future_outlook_summary': enhanced_analysis.future_outlook.get('strategic_priorities', 'N/A'),
                        'competitive_positioning': enhanced_analysis.competitive_positioning.get('market_position', 'N/A')
                    }
                    
                    enhanced_rows.append(row)
                    
                except Exception as e:
                    self.logger.error(f"Enhanced analysis failed for {analysis.fiscal_year}: {e}")
                    # Add basic row with error indicators
                    enhanced_rows.append({
                        'company_cik': analysis.company_cik,
                        'fiscal_year': analysis.fiscal_year,
                        'credit_risk_score': 5.0,  # Default moderate risk
                        'credit_risk_rationale': 'Analysis failed',
                        'supply_chain_risk_score': 5.0,
                        'supply_chain_risk_rationale': 'Analysis failed',
                        'regulatory_risk_score': 5.0,
                        'regulatory_risk_rationale': 'Analysis failed'
                    })
        
        return pd.DataFrame(enhanced_rows)
    
    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder columns for better readability"""
        
        # Priority column order
        priority_columns = [
            'ticker', 'company_name', 'company_cik', 'fiscal_year', 'data_extraction_date',
            # Core financials
            'revenue', 'net_income', 'total_assets', 'shareholders_equity', 'operating_cash_flow',
            # Key performance metrics
            'revenue_growth_yoy', 'net_profit_margin', 'roe', 'roa', 'free_cash_flow',
            # Risk Scores (0-10 scale)
            'credit_risk_score', 'supply_chain_risk_score', 'regulatory_risk_score',
            # Probing Questions & Answers
            'probing_question_1', 'probing_answer_1', 'probing_question_2', 'probing_answer_2',
            # M&A Analysis
            'ma_acquisition_appetite', 'ma_strategic_focus', 'ma_potential_targets',
            # Narrative insights
            'business_segments', 'key_risks', 'strategic_focus'
        ]
        
        # Get columns that exist in the DataFrame
        existing_priority = [col for col in priority_columns if col in df.columns]
        other_columns = [col for col in df.columns if col not in priority_columns]
        
        # Reorder with priority columns first
        column_order = existing_priority + sorted(other_columns)
        return df[column_order]
    
    def get_dataset_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary of the comprehensive dataset"""
        
        return {
            'shape': df.shape,
            'companies': df['company_name'].nunique() if 'company_name' in df.columns else 1,
            'years_covered': sorted(df['fiscal_year'].unique()) if 'fiscal_year' in df.columns else [],
            'data_completeness': {
                'financial_metrics': (df[['revenue', 'net_income', 'total_assets']].notna().all(axis=1)).mean(),
                'performance_metrics': (df[['revenue_growth_yoy', 'net_profit_margin', 'roe']].notna().all(axis=1)).mean() if 'roe' in df.columns else 0,
                'narrative_insights': (df[['business_segments', 'key_risks']].notna().all(axis=1)).mean() if 'business_segments' in df.columns else 0
            },
            'column_types': {
                'quantitative': len(df.select_dtypes(include=[np.number]).columns),
                'qualitative': len(df.select_dtypes(include=['object']).columns),
                'total': len(df.columns)
            }
        }
    
    def export_to_excel(self, df: pd.DataFrame, filename: str, export_directory: str = "exports") -> str:
        """Export comprehensive dataset to Excel with organized sheets"""
        
        export_dir = Path(export_directory)
        export_dir.mkdir(exist_ok=True)
        file_path = export_dir / f"{filename}.xlsx"
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # Main comprehensive sheet
            df.to_excel(writer, sheet_name='Comprehensive Data', index=False)
            
            # Financial metrics only
            financial_cols = ['ticker', 'company_name', 'fiscal_year'] + [
                col for col in df.columns if col in [
                    'revenue', 'net_income', 'total_assets', 'shareholders_equity',
                    'operating_cash_flow', 'capital_expenditures', 'cash_and_equivalents',
                    'current_assets', 'current_liabilities', 'long_term_debt'
                ]
            ]
            if len(financial_cols) > 3:
                df[financial_cols].to_excel(writer, sheet_name='Financial Statements', index=False)
            
            # Performance analytics only
            performance_cols = ['ticker', 'company_name', 'fiscal_year'] + [
                col for col in df.columns if any(keyword in col.lower() for keyword in [
                    'growth', 'margin', 'roe', 'roa', 'roic', 'ratio', 'turnover'
                ])
            ]
            if len(performance_cols) > 3:
                df[performance_cols].to_excel(writer, sheet_name='Performance Analytics', index=False)
            
            # Narrative insights only
            narrative_cols = ['ticker', 'company_name', 'fiscal_year'] + [
                col for col in df.columns if any(keyword in col.lower() for keyword in [
                    'segments', 'risks', 'strategic', 'business_model', 'revenue_streams'
                ])
            ]
            if len(narrative_cols) > 3:
                df[narrative_cols].to_excel(writer, sheet_name='Narrative Insights', index=False)
            
            # Summary sheet
            summary = self.get_dataset_summary(df)
            summary_df = pd.DataFrame([
                ['Dataset Shape', f"{summary['shape'][0]} rows × {summary['shape'][1]} columns"],
                ['Companies', summary['companies']],
                ['Years Covered', ', '.join(map(str, summary['years_covered']))],
                ['Financial Completeness', f"{summary['data_completeness']['financial_metrics']:.1%}"],
                ['Performance Completeness', f"{summary['data_completeness']['performance_metrics']:.1%}"],
                ['Narrative Completeness', f"{summary['data_completeness']['narrative_insights']:.1%}"],
                ['Quantitative Columns', summary['column_types']['quantitative']],
                ['Qualitative Columns', summary['column_types']['qualitative']],
                ['Total Columns', summary['column_types']['total']]
            ], columns=['Metric', 'Value'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        self.logger.info(f"Comprehensive dataset exported to {file_path}")
        return str(file_path)


def build_company_dataset(ticker: str, years: int = 5, gemini_api_key: Optional[str] = None) -> pd.DataFrame:
    """
    Convenience function to build comprehensive dataset for a single company
    
    Args:
        ticker: Company ticker symbol
        years: Number of years of data
        gemini_api_key: Optional Gemini API key for narrative extraction
        
    Returns:
        Comprehensive DataFrame
    """
    builder = ComprehensiveFinancialDataBuilder(gemini_api_key=gemini_api_key)
    return builder.build_comprehensive_dataset(ticker, years)


if __name__ == "__main__":
    # Example usage
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    print("Testing Comprehensive Financial Data Builder...")
    
    # Build Apple dataset
    try:
        builder = ComprehensiveFinancialDataBuilder()
        apple_df = builder.build_comprehensive_dataset('AAPL', years=5)
        
        print(f"Apple dataset shape: {apple_df.shape}")
        print(f"Columns: {list(apple_df.columns)}")
        
        # Export to Excel
        export_path = builder.export_to_excel(apple_df, 'apple_comprehensive_test')
        print(f"Exported to: {export_path}")
        
        # Show summary
        summary = builder.get_dataset_summary(apple_df)
        print(f"Summary: {summary}")
        
    except Exception as e:
        print(f"Error: {e}")