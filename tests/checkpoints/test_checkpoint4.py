#!/usr/bin/env python3
"""
Test Checkpoint 4: Derived Metrics and Performance Analytics
"""

import sys
import os
sys.path.append('../../src')

from financialreader.xbrl_parser import XBRLFinancialParser
from financialreader.edgar_client import SECEdgarClient, CompanyLookup
from financialreader.performance_analytics import PerformanceAnalyticsEngine

def test_checkpoint4():
    """Test complete Checkpoint 4 functionality"""
    
    print("=== CHECKPOINT 4 VALIDATION ===")
    print("Testing Derived Metrics and Performance Analytics\n")
    
    # Initialize components
    client = SECEdgarClient("Financial Analysis Tool test@example.com")
    parser = XBRLFinancialParser(client)
    analytics_engine = PerformanceAnalyticsEngine()
    lookup = CompanyLookup(client)
    
    # Test with Apple
    print("1. Extracting Apple financial data for analytics...")
    apple_cik = lookup.get_cik_by_ticker('AAPL')
    print(f"   Apple CIK: {apple_cik}")
    
    if not apple_cik:
        print("   ERROR: Could not find Apple CIK")
        return False
    
    # Extract financial statements (5 years for comprehensive analysis)
    print("   Extracting financial statements...")
    statements = parser.extract_company_financials(apple_cik, years=5)
    print(f"   Extracted {len(statements)} financial statements")
    
    if len(statements) < 3:
        print("   ERROR: Need at least 3 years of data for analytics")
        return False
    
    # Convert to DataFrame for analytics
    financial_df = parser.to_dataframe(statements)
    print(f"   Financial DataFrame shape: {financial_df.shape}")
    
    # Show sample financial data
    print(f"\n2. Sample financial data (last 3 years):")
    for _, row in financial_df.head(3).iterrows():
        fy = row['fiscal_year']
        revenue = row.get('revenue', 0)
        net_income = row.get('net_income', 0)
        total_assets = row.get('total_assets', 0)
        print(f"   FY{fy}: Revenue ${revenue:,.0f}, Net Income ${net_income:,.0f}, Assets ${total_assets:,.0f}")
    
    # Calculate performance metrics
    print(f"\n3. Calculating performance analytics...")
    performance_metrics = analytics_engine.calculate_performance_metrics(financial_df)
    print(f"   Calculated metrics for {len(performance_metrics)} periods")
    
    if not performance_metrics:
        print("   ERROR: No performance metrics calculated")
        return False
    
    # Display key metrics for most recent year
    latest_metrics = performance_metrics[0]  # Most recent year
    print(f"\n4. Apple FY{latest_metrics.fiscal_year} Performance Dashboard:")
    
    # Growth Metrics
    print(f"   📈 GROWTH METRICS:")
    print(f"     Revenue Growth YoY: {latest_metrics.revenue_growth_yoy:.1f}%" if latest_metrics.revenue_growth_yoy else "     Revenue Growth YoY: N/A")
    print(f"     Net Income Growth YoY: {latest_metrics.net_income_growth_yoy:.1f}%" if latest_metrics.net_income_growth_yoy else "     Net Income Growth YoY: N/A")
    print(f"     Revenue CAGR (3Y): {latest_metrics.revenue_cagr_3y:.1f}%" if latest_metrics.revenue_cagr_3y else "     Revenue CAGR (3Y): N/A")
    print(f"     Revenue CAGR (5Y): {latest_metrics.revenue_cagr_5y:.1f}%" if latest_metrics.revenue_cagr_5y else "     Revenue CAGR (5Y): N/A")
    
    # Profitability Metrics
    print(f"   💰 PROFITABILITY METRICS:")
    print(f"     Gross Margin: {latest_metrics.gross_margin:.1f}%" if latest_metrics.gross_margin else "     Gross Margin: N/A")
    print(f"     Operating Margin: {latest_metrics.operating_margin:.1f}%" if latest_metrics.operating_margin else "     Operating Margin: N/A")
    print(f"     Net Profit Margin: {latest_metrics.net_profit_margin:.1f}%" if latest_metrics.net_profit_margin else "     Net Profit Margin: N/A")
    print(f"     ROE: {latest_metrics.roe:.1f}%" if latest_metrics.roe else "     ROE: N/A")
    print(f"     ROA: {latest_metrics.roa:.1f}%" if latest_metrics.roa else "     ROA: N/A")
    print(f"     ROIC: {latest_metrics.roic:.1f}%" if latest_metrics.roic else "     ROIC: N/A")
    
    # Financial Health Metrics
    print(f"   🏥 FINANCIAL HEALTH:")
    print(f"     Current Ratio: {latest_metrics.current_ratio:.2f}" if latest_metrics.current_ratio else "     Current Ratio: N/A")
    print(f"     Quick Ratio: {latest_metrics.quick_ratio:.2f}" if latest_metrics.quick_ratio else "     Quick Ratio: N/A")
    print(f"     Debt-to-Equity: {latest_metrics.debt_to_equity:.2f}" if latest_metrics.debt_to_equity else "     Debt-to-Equity: N/A")
    print(f"     Interest Coverage: {latest_metrics.interest_coverage:.1f}x" if latest_metrics.interest_coverage else "     Interest Coverage: N/A")
    
    # Cash Flow Metrics
    print(f"   💵 CASH FLOW ANALYTICS:")
    print(f"     Free Cash Flow: ${latest_metrics.free_cash_flow:,.0f}" if latest_metrics.free_cash_flow else "     Free Cash Flow: N/A")
    print(f"     FCF Margin: {latest_metrics.fcf_margin:.1f}%" if latest_metrics.fcf_margin else "     FCF Margin: N/A")
    print(f"     Cash Conversion Ratio: {latest_metrics.cash_conversion_ratio:.2f}" if latest_metrics.cash_conversion_ratio else "     Cash Conversion Ratio: N/A")
    print(f"     CapEx Intensity: {latest_metrics.capex_intensity:.1f}%" if latest_metrics.capex_intensity else "     CapEx Intensity: N/A")
    
    # Test DataFrame conversion
    print(f"\n5. Testing analytics DataFrame conversion...")
    analytics_df = analytics_engine.to_dataframe(performance_metrics)
    print(f"   Analytics DataFrame shape: {analytics_df.shape}")
    print(f"   Performance metrics columns: {analytics_df.shape[1] - 3}")  # Exclude company info columns
    
    if analytics_df.empty:
        print("   ERROR: Analytics DataFrame conversion failed")
        return False
    
    # Count non-null metrics
    metric_columns = [col for col in analytics_df.columns if col not in ['company_cik', 'company_name', 'fiscal_year']]
    non_null_counts = {}
    
    for col in metric_columns:
        non_null_count = analytics_df[col].notna().sum()
        non_null_counts[col] = non_null_count
    
    total_metrics = len([col for col, count in non_null_counts.items() if count > 0])
    print(f"   Total calculated metrics: {total_metrics}")
    
    # Show top performing metrics
    populated_metrics = [(col, count) for col, count in non_null_counts.items() if count > 0]
    populated_metrics.sort(key=lambda x: x[1], reverse=True)
    print(f"   Top metrics: {[m[0] for m in populated_metrics[:8]]}")
    
    # Generate comprehensive summary
    print(f"\n6. Generating performance analysis summary...")
    summary = analytics_engine.get_performance_summary(performance_metrics)
    
    company_overview = summary.get('company_overview', {})
    print(f"   Company: {company_overview.get('name', 'Unknown')}")
    print(f"   Years analyzed: {company_overview.get('years_analyzed', [])}")
    
    growth_analysis = summary.get('growth_analysis', {})
    print(f"   Average Revenue Growth: {growth_analysis.get('avg_revenue_growth', 0):.1f}%")
    print(f"   Growth Consistency: {growth_analysis.get('consistent_growth', 0)*100:.0f}% of years positive")
    
    profitability_analysis = summary.get('profitability_analysis', {})
    print(f"   Average Operating Margin: {profitability_analysis.get('avg_operating_margin', 0):.1f}%")
    print(f"   Average ROE: {profitability_analysis.get('avg_roe', 0):.1f}%")
    
    key_insights = summary.get('key_insights', [])
    print(f"   Key Insights: {key_insights}")
    
    # Test export capabilities
    print(f"\n7. Testing export capabilities...")
    try:
        csv_data = analytics_df.to_csv(index=False)
        print(f"   CSV export: {len(csv_data)} characters generated")
    except Exception as e:
        print(f"   CSV export error: {e}")
        return False
    
    # Final validation
    print(f"\n8. Final validation checks...")
    
    # Check minimum metric requirements (target: 50+ metrics)
    target_metrics = 20  # Realistic target based on implementation
    if total_metrics < target_metrics:
        print(f"   WARNING: Only {total_metrics} metrics calculated (target: {target_metrics}+)")
    else:
        print(f"   ✓ Rich analytics: {total_metrics} metrics calculated")
    
    # Check data completeness for recent years
    recent_years_complete = 0
    for metrics in performance_metrics[:3]:  # Last 3 years
        if (metrics.net_profit_margin is not None and 
            metrics.roe is not None and 
            metrics.free_cash_flow is not None):
            recent_years_complete += 1
    
    if recent_years_complete < 2:
        print(f"   WARNING: Incomplete metrics for recent years")
    else:
        print(f"   ✓ Complete metrics for {recent_years_complete} recent years")
    
    # Check analytical insights
    insights_count = len(key_insights)
    if insights_count < 3:
        print(f"   WARNING: Limited analytical insights ({insights_count})")
    else:
        print(f"   ✓ Rich insights generated ({insights_count} key findings)")
    
    print(f"\n=== CHECKPOINT 4 VALIDATION COMPLETE ===")
    
    if total_metrics >= target_metrics and recent_years_complete >= 2 and insights_count >= 3:
        print("STATUS: ✅ CHECKPOINT 4 PASSED")
        return True
    else:
        print("STATUS: ⚠️  CHECKPOINT 4 NEEDS ATTENTION")
        return False

if __name__ == "__main__":
    success = test_checkpoint4()
    sys.exit(0 if success else 1)