"""
Performance Analytics Engine for Financial Statement Analysis
Calculates advanced business performance indicators from raw financial data
"""

from typing import Dict, List, Optional, Any, Union
import pandas as pd
import numpy as np
from dataclasses import dataclass
import logging
from datetime import datetime

@dataclass
class PerformanceMetrics:
    """Container for calculated performance metrics"""
    company_cik: str
    company_name: str
    fiscal_year: int
    
    # Growth Metrics
    revenue_growth_yoy: Optional[float] = None
    net_income_growth_yoy: Optional[float] = None
    eps_growth_yoy: Optional[float] = None
    revenue_cagr_3y: Optional[float] = None
    revenue_cagr_5y: Optional[float] = None
    
    # Profitability Metrics
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    net_profit_margin: Optional[float] = None
    roe: Optional[float] = None  # Return on Equity
    roa: Optional[float] = None  # Return on Assets
    roic: Optional[float] = None  # Return on Invested Capital
    
    # Financial Health Indicators
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    cash_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    debt_to_assets: Optional[float] = None
    interest_coverage: Optional[float] = None
    
    # Cash Flow Analytics
    free_cash_flow: Optional[float] = None
    fcf_margin: Optional[float] = None
    cash_conversion_ratio: Optional[float] = None
    capex_intensity: Optional[float] = None
    
    # Efficiency Metrics
    asset_turnover: Optional[float] = None
    working_capital: Optional[float] = None
    working_capital_turnover: Optional[float] = None

class PerformanceAnalyticsEngine:
    """
    Calculates advanced business performance indicators from financial data
    Handles growth rates, profitability, financial health, and cash flow metrics
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def calculate_performance_metrics(self, financial_data: pd.DataFrame) -> List[PerformanceMetrics]:
        """
        Calculate comprehensive performance metrics for financial dataset
        
        Args:
            financial_data: DataFrame with financial statements data
            
        Returns:
            List of PerformanceMetrics objects, one per year
        """
        self.logger.info(f"Calculating performance metrics for {len(financial_data)} periods")
        
        if financial_data.empty:
            self.logger.warning("Empty financial data provided")
            return []
        
        # Ensure data is sorted by fiscal year
        data = financial_data.sort_values('fiscal_year', ascending=True).copy()
        
        metrics_list = []
        
        for idx, row in data.iterrows():
            metrics = PerformanceMetrics(
                company_cik=row.get('company_cik', ''),
                company_name=row.get('company_name', ''),
                fiscal_year=row.get('fiscal_year', 0)
            )
            
            # Calculate all metric categories
            self._calculate_growth_metrics(metrics, data, idx)
            self._calculate_profitability_metrics(metrics, row)
            self._calculate_financial_health_metrics(metrics, row)
            self._calculate_cash_flow_metrics(metrics, row)
            self._calculate_efficiency_metrics(metrics, row)
            
            metrics_list.append(metrics)
        
        self.logger.info(f"Calculated metrics for {len(metrics_list)} periods")
        return metrics_list
    
    def _calculate_growth_metrics(self, metrics: PerformanceMetrics, data: pd.DataFrame, current_idx: int):
        """Calculate growth rate metrics"""
        
        current_row = data.iloc[current_idx]
        
        # Year-over-year growth (need previous year)
        if current_idx > 0:
            prev_row = data.iloc[current_idx - 1]
            
            # Revenue growth YoY
            current_revenue = current_row.get('revenue', 0)
            prev_revenue = prev_row.get('revenue', 0)
            if prev_revenue and prev_revenue != 0:
                metrics.revenue_growth_yoy = ((current_revenue - prev_revenue) / prev_revenue) * 100
            
            # Net income growth YoY
            current_ni = current_row.get('net_income', 0)
            prev_ni = prev_row.get('net_income', 0)
            if prev_ni and prev_ni != 0:
                metrics.net_income_growth_yoy = ((current_ni - prev_ni) / prev_ni) * 100
            
            # EPS growth YoY
            current_eps = current_row.get('earnings_per_share_diluted', 0)
            prev_eps = prev_row.get('earnings_per_share_diluted', 0)
            if prev_eps and prev_eps != 0:
                metrics.eps_growth_yoy = ((current_eps - prev_eps) / prev_eps) * 100
        
        # CAGR calculations (need multiple years)
        current_year = current_row.get('fiscal_year', 0)
        
        # 3-year CAGR
        three_years_ago = data[data['fiscal_year'] == current_year - 2]
        if not three_years_ago.empty:
            start_revenue = three_years_ago.iloc[0].get('revenue', 0)
            end_revenue = current_row.get('revenue', 0)
            if start_revenue and start_revenue > 0 and end_revenue and end_revenue > 0:
                metrics.revenue_cagr_3y = (((end_revenue / start_revenue) ** (1/3)) - 1) * 100
        
        # 5-year CAGR
        five_years_ago = data[data['fiscal_year'] == current_year - 4]
        if not five_years_ago.empty:
            start_revenue = five_years_ago.iloc[0].get('revenue', 0)
            end_revenue = current_row.get('revenue', 0)
            if start_revenue and start_revenue > 0 and end_revenue and end_revenue > 0:
                metrics.revenue_cagr_5y = (((end_revenue / start_revenue) ** (1/5)) - 1) * 100
    
    def _calculate_profitability_metrics(self, metrics: PerformanceMetrics, row: pd.Series):
        """Calculate profitability metrics"""
        
        revenue = row.get('revenue', 0)
        cost_of_goods_sold = row.get('cost_of_goods_sold', 0)
        operating_income = row.get('operating_income', 0)
        net_income = row.get('net_income', 0)
        total_assets = row.get('total_assets', 0)
        shareholders_equity = row.get('shareholders_equity', 0)
        
        # Margin calculations
        if revenue and revenue > 0:
            # Gross Margin
            if cost_of_goods_sold is not None:
                gross_profit = revenue - cost_of_goods_sold
                metrics.gross_margin = (gross_profit / revenue) * 100
            
            # Operating Margin
            if operating_income is not None:
                metrics.operating_margin = (operating_income / revenue) * 100
            
            # Net Profit Margin
            if net_income is not None:
                metrics.net_profit_margin = (net_income / revenue) * 100
        
        # Return metrics
        if net_income and net_income > 0:
            # Return on Assets (ROA)
            if total_assets and total_assets > 0:
                metrics.roa = (net_income / total_assets) * 100
            
            # Return on Equity (ROE)
            if shareholders_equity and shareholders_equity > 0:
                metrics.roe = (net_income / shareholders_equity) * 100
        
        # ROIC calculation (simplified)
        if operating_income and total_assets and shareholders_equity:
            total_debt = total_assets - shareholders_equity
            invested_capital = shareholders_equity + total_debt
            if invested_capital > 0:
                # Use operating income after tax (approximated)
                tax_rate = 0.25  # Approximate corporate tax rate
                nopat = operating_income * (1 - tax_rate)
                metrics.roic = (nopat / invested_capital) * 100
    
    def _calculate_financial_health_metrics(self, metrics: PerformanceMetrics, row: pd.Series):
        """Calculate financial health and liquidity metrics"""
        
        current_assets = row.get('current_assets', 0)
        current_liabilities = row.get('current_liabilities', 0)
        cash_and_equivalents = row.get('cash_and_equivalents', 0)
        inventory = row.get('inventory', 0)
        accounts_receivable = row.get('accounts_receivable', 0)
        total_assets = row.get('total_assets', 0)
        total_liabilities = row.get('total_liabilities', 0)
        shareholders_equity = row.get('shareholders_equity', 0)
        long_term_debt = row.get('long_term_debt', 0)
        operating_income = row.get('operating_income', 0)
        interest_expense = row.get('interest_expense', 0)
        
        # Liquidity ratios
        if current_liabilities and current_liabilities > 0:
            # Current Ratio
            if current_assets:
                metrics.current_ratio = current_assets / current_liabilities
            
            # Quick Ratio (Current Assets - Inventory) / Current Liabilities
            if current_assets is not None and inventory is not None:
                quick_assets = current_assets - (inventory or 0)
                metrics.quick_ratio = quick_assets / current_liabilities
            
            # Cash Ratio
            if cash_and_equivalents:
                metrics.cash_ratio = cash_and_equivalents / current_liabilities
        
        # Leverage ratios
        if shareholders_equity and shareholders_equity > 0:
            # Debt-to-Equity
            if total_liabilities:
                metrics.debt_to_equity = total_liabilities / shareholders_equity
        
        if total_assets and total_assets > 0:
            # Debt-to-Assets
            if total_liabilities:
                metrics.debt_to_assets = total_liabilities / total_assets
        
        # Interest Coverage Ratio
        if interest_expense and interest_expense > 0 and operating_income:
            metrics.interest_coverage = operating_income / interest_expense
    
    def _calculate_cash_flow_metrics(self, metrics: PerformanceMetrics, row: pd.Series):
        """Calculate cash flow analytics"""
        
        operating_cash_flow = row.get('operating_cash_flow', 0)
        capital_expenditures = row.get('capital_expenditures', 0)
        net_income = row.get('net_income', 0)
        revenue = row.get('revenue', 0)
        
        # Free Cash Flow = Operating Cash Flow - Capital Expenditures
        if operating_cash_flow is not None and capital_expenditures is not None:
            metrics.free_cash_flow = operating_cash_flow - abs(capital_expenditures)  # CapEx is usually negative
        
        # FCF Margin
        if metrics.free_cash_flow is not None and revenue and revenue > 0:
            metrics.fcf_margin = (metrics.free_cash_flow / revenue) * 100
        
        # Cash Conversion Ratio (Operating CF / Net Income)
        if operating_cash_flow and net_income and net_income > 0:
            metrics.cash_conversion_ratio = operating_cash_flow / net_income
        
        # CapEx Intensity
        if capital_expenditures and revenue and revenue > 0:
            metrics.capex_intensity = (abs(capital_expenditures) / revenue) * 100
    
    def _calculate_efficiency_metrics(self, metrics: PerformanceMetrics, row: pd.Series):
        """Calculate efficiency metrics"""
        
        revenue = row.get('revenue', 0)
        total_assets = row.get('total_assets', 0)
        current_assets = row.get('current_assets', 0)
        current_liabilities = row.get('current_liabilities', 0)
        
        # Asset Turnover
        if total_assets and total_assets > 0 and revenue:
            metrics.asset_turnover = revenue / total_assets
        
        # Working Capital
        if current_assets is not None and current_liabilities is not None:
            metrics.working_capital = current_assets - current_liabilities
        
        # Working Capital Turnover
        if metrics.working_capital and metrics.working_capital != 0 and revenue:
            metrics.working_capital_turnover = revenue / metrics.working_capital
    
    def to_dataframe(self, metrics_list: List[PerformanceMetrics]) -> pd.DataFrame:
        """
        Convert performance metrics to pandas DataFrame
        
        Args:
            metrics_list: List of PerformanceMetrics objects
            
        Returns:
            DataFrame with performance metrics as columns, years as rows
        """
        if not metrics_list:
            return pd.DataFrame()
        
        rows = []
        for metrics in metrics_list:
            row = {
                'company_cik': metrics.company_cik,
                'company_name': metrics.company_name,
                'fiscal_year': metrics.fiscal_year,
                
                # Growth Metrics
                'revenue_growth_yoy': metrics.revenue_growth_yoy,
                'net_income_growth_yoy': metrics.net_income_growth_yoy,
                'eps_growth_yoy': metrics.eps_growth_yoy,
                'revenue_cagr_3y': metrics.revenue_cagr_3y,
                'revenue_cagr_5y': metrics.revenue_cagr_5y,
                
                # Profitability Metrics
                'gross_margin': metrics.gross_margin,
                'operating_margin': metrics.operating_margin,
                'net_profit_margin': metrics.net_profit_margin,
                'roe': metrics.roe,
                'roa': metrics.roa,
                'roic': metrics.roic,
                
                # Financial Health
                'current_ratio': metrics.current_ratio,
                'quick_ratio': metrics.quick_ratio,
                'cash_ratio': metrics.cash_ratio,
                'debt_to_equity': metrics.debt_to_equity,
                'debt_to_assets': metrics.debt_to_assets,
                'interest_coverage': metrics.interest_coverage,
                
                # Cash Flow Analytics
                'free_cash_flow': metrics.free_cash_flow,
                'fcf_margin': metrics.fcf_margin,
                'cash_conversion_ratio': metrics.cash_conversion_ratio,
                'capex_intensity': metrics.capex_intensity,
                
                # Efficiency
                'asset_turnover': metrics.asset_turnover,
                'working_capital': metrics.working_capital,
                'working_capital_turnover': metrics.working_capital_turnover
            }
            rows.append(row)
        
        df = pd.DataFrame(rows)
        
        # Sort by fiscal year (most recent first)
        if not df.empty:
            df = df.sort_values('fiscal_year', ascending=False)
            df = df.reset_index(drop=True)
        
        return df
    
    def get_performance_summary(self, metrics_list: List[PerformanceMetrics]) -> Dict[str, Any]:
        """Generate comprehensive performance analysis summary"""
        
        if not metrics_list:
            return {"error": "No metrics provided"}
        
        # Sort by year for trend analysis
        sorted_metrics = sorted(metrics_list, key=lambda x: x.fiscal_year)
        
        company_name = sorted_metrics[0].company_name
        years = [m.fiscal_year for m in sorted_metrics]
        
        # Calculate averages and trends
        summary = {
            'company_overview': {
                'name': company_name,
                'years_analyzed': years,
                'periods': len(sorted_metrics)
            },
            'growth_analysis': self._analyze_growth_trends(sorted_metrics),
            'profitability_analysis': self._analyze_profitability_trends(sorted_metrics),
            'financial_health_analysis': self._analyze_health_trends(sorted_metrics),
            'cash_flow_analysis': self._analyze_cash_flow_trends(sorted_metrics),
            'key_insights': self._generate_key_insights(sorted_metrics)
        }
        
        return summary
    
    def _analyze_growth_trends(self, metrics_list: List[PerformanceMetrics]) -> Dict[str, Any]:
        """Analyze growth trends over time"""
        
        revenue_growth = [m.revenue_growth_yoy for m in metrics_list if m.revenue_growth_yoy is not None]
        ni_growth = [m.net_income_growth_yoy for m in metrics_list if m.net_income_growth_yoy is not None]
        
        return {
            'avg_revenue_growth': np.mean(revenue_growth) if revenue_growth else None,
            'revenue_growth_volatility': np.std(revenue_growth) if len(revenue_growth) > 1 else None,
            'avg_net_income_growth': np.mean(ni_growth) if ni_growth else None,
            'consistent_growth': len([g for g in revenue_growth if g > 0]) / len(revenue_growth) if revenue_growth else 0,
            'latest_cagr_3y': metrics_list[-1].revenue_cagr_3y if metrics_list else None
        }
    
    def _analyze_profitability_trends(self, metrics_list: List[PerformanceMetrics]) -> Dict[str, Any]:
        """Analyze profitability trends"""
        
        gross_margins = [m.gross_margin for m in metrics_list if m.gross_margin is not None]
        operating_margins = [m.operating_margin for m in metrics_list if m.operating_margin is not None]
        roe_values = [m.roe for m in metrics_list if m.roe is not None]
        
        return {
            'avg_gross_margin': np.mean(gross_margins) if gross_margins else None,
            'avg_operating_margin': np.mean(operating_margins) if operating_margins else None,
            'avg_roe': np.mean(roe_values) if roe_values else None,
            'margin_trend': 'improving' if len(operating_margins) > 1 and operating_margins[-1] > operating_margins[0] else 'declining' if len(operating_margins) > 1 else 'stable'
        }
    
    def _analyze_health_trends(self, metrics_list: List[PerformanceMetrics]) -> Dict[str, Any]:
        """Analyze financial health trends"""
        
        current_ratios = [m.current_ratio for m in metrics_list if m.current_ratio is not None]
        debt_ratios = [m.debt_to_equity for m in metrics_list if m.debt_to_equity is not None]
        
        return {
            'avg_current_ratio': np.mean(current_ratios) if current_ratios else None,
            'avg_debt_to_equity': np.mean(debt_ratios) if debt_ratios else None,
            'liquidity_trend': 'improving' if len(current_ratios) > 1 and current_ratios[-1] > current_ratios[0] else 'declining' if len(current_ratios) > 1 else 'stable'
        }
    
    def _analyze_cash_flow_trends(self, metrics_list: List[PerformanceMetrics]) -> Dict[str, Any]:
        """Analyze cash flow trends"""
        
        fcf_margins = [m.fcf_margin for m in metrics_list if m.fcf_margin is not None]
        conversion_ratios = [m.cash_conversion_ratio for m in metrics_list if m.cash_conversion_ratio is not None]
        
        return {
            'avg_fcf_margin': np.mean(fcf_margins) if fcf_margins else None,
            'avg_cash_conversion': np.mean(conversion_ratios) if conversion_ratios else None,
            'fcf_trend': 'improving' if len(fcf_margins) > 1 and fcf_margins[-1] > fcf_margins[0] else 'declining' if len(fcf_margins) > 1 else 'stable'
        }
    
    def _generate_key_insights(self, metrics_list: List[PerformanceMetrics]) -> List[str]:
        """Generate key performance insights"""
        
        insights = []
        latest = metrics_list[-1] if metrics_list else None
        
        if not latest:
            return insights
        
        # Growth insights
        if latest.revenue_growth_yoy is not None:
            if latest.revenue_growth_yoy > 10:
                insights.append(f"Strong revenue growth of {latest.revenue_growth_yoy:.1f}% YoY")
            elif latest.revenue_growth_yoy < 0:
                insights.append(f"Revenue declining {abs(latest.revenue_growth_yoy):.1f}% YoY")
        
        # Profitability insights
        if latest.operating_margin is not None:
            if latest.operating_margin > 20:
                insights.append(f"High operating margin of {latest.operating_margin:.1f}%")
            elif latest.operating_margin < 5:
                insights.append(f"Low operating margin of {latest.operating_margin:.1f}%")
        
        # Financial health insights
        if latest.current_ratio is not None:
            if latest.current_ratio > 2:
                insights.append(f"Strong liquidity with current ratio of {latest.current_ratio:.2f}")
            elif latest.current_ratio < 1:
                insights.append(f"Liquidity concern with current ratio of {latest.current_ratio:.2f}")
        
        # Cash flow insights
        if latest.fcf_margin is not None:
            if latest.fcf_margin > 15:
                insights.append(f"Excellent cash generation with FCF margin of {latest.fcf_margin:.1f}%")
            elif latest.fcf_margin < 0:
                insights.append("Negative free cash flow")
        
        return insights


if __name__ == "__main__":
    # Example usage and testing
    print("Testing Performance Analytics Engine...")
    
    # Create sample financial data
    sample_data = pd.DataFrame({
        'company_cik': ['0000320193'] * 3,
        'company_name': ['Apple Inc.'] * 3,
        'fiscal_year': [2022, 2023, 2024],
        'revenue': [394328000000, 383285000000, 391035000000],
        'net_income': [99803000000, 96995000000, 93736000000],
        'total_assets': [352755000000, 352583000000, 364980000000],
        'shareholders_equity': [50672000000, 62146000000, 74100000000],
        'operating_cash_flow': [122151000000, 110543000000, 118254000000],
        'capital_expenditures': [-7709000000, -10959000000, -9447000000]
    })
    
    engine = PerformanceAnalyticsEngine()
    metrics = engine.calculate_performance_metrics(sample_data)
    
    print(f"Calculated metrics for {len(metrics)} periods")
    
    # Show latest metrics
    if metrics:
        latest = metrics[0]  # Most recent (sorted desc)
        print(f"\nApple FY{latest.fiscal_year} Performance Metrics:")
        print(f"  Revenue Growth YoY: {latest.revenue_growth_yoy:.1f}%" if latest.revenue_growth_yoy else "  Revenue Growth YoY: N/A")
        print(f"  Net Profit Margin: {latest.net_profit_margin:.1f}%" if latest.net_profit_margin else "  Net Profit Margin: N/A")
        print(f"  ROE: {latest.roe:.1f}%" if latest.roe else "  ROE: N/A")
        print(f"  Free Cash Flow: ${latest.free_cash_flow:,.0f}" if latest.free_cash_flow else "  Free Cash Flow: N/A")
    
    # Test DataFrame conversion
    df = engine.to_dataframe(metrics)
    print(f"\nDataFrame shape: {df.shape}")
    if not df.empty:
        print(f"Metrics columns: {len([col for col in df.columns if col not in ['company_cik', 'company_name', 'fiscal_year']])}")
    
    # Test summary
    summary = engine.get_performance_summary(metrics)
    print(f"\nPerformance Summary:")
    print(f"  Company: {summary['company_overview']['name']}")
    print(f"  Average Revenue Growth: {summary['growth_analysis']['avg_revenue_growth']:.1f}%" if summary['growth_analysis']['avg_revenue_growth'] else "  Average Revenue Growth: N/A")
    print(f"  Key Insights: {summary['key_insights']}")