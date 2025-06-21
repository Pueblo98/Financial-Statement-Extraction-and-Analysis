#!/usr/bin/env python3
"""
Test Checkpoint 2: XBRL Parser and Financial Data Extraction
"""

import sys
import os
sys.path.append('../../src')

from financialreader.xbrl_parser import XBRLFinancialParser
from financialreader.edgar_client import SECEdgarClient, CompanyLookup

def test_checkpoint2():
    """Test complete XBRL parser functionality"""
    
    print("=== CHECKPOINT 2 VALIDATION ===")
    print("Testing XBRL Financial Data Extraction\n")
    
    # Initialize components
    client = SECEdgarClient("Financial Analysis Tool test@example.com")
    parser = XBRLFinancialParser(client)
    lookup = CompanyLookup(client)
    
    # Test with Apple
    print("1. Testing Apple (AAPL) data extraction...")
    apple_cik = lookup.get_cik_by_ticker('AAPL')
    print(f"   Apple CIK: {apple_cik}")
    
    if not apple_cik:
        print("   ERROR: Could not find Apple CIK")
        return False
    
    # Extract financial statements
    print("   Extracting financial statements...")
    statements = parser.extract_company_financials(apple_cik, years=5)
    print(f"   Extracted {len(statements)} financial statements")
    
    if not statements:
        print("   ERROR: No financial statements extracted")
        return False
    
    # Display key metrics for recent years
    print(f"\n2. Apple Financial Summary (Last {min(3, len(statements))} years):")
    for stmt in statements[:3]:
        revenue = stmt.income_statement.get('revenue', 0)
        net_income = stmt.income_statement.get('net_income', 0)
        total_assets = stmt.balance_sheet.get('total_assets', 0)
        ocf = stmt.cash_flow.get('operating_cash_flow', 0)
        
        print(f"   FY{stmt.fiscal_year}:")
        print(f"     Revenue: ${revenue:,.0f}")
        print(f"     Net Income: ${net_income:,.0f}")
        print(f"     Total Assets: ${total_assets:,.0f}")
        print(f"     Operating Cash Flow: ${ocf:,.0f}")
        print(f"     Data Quality Score: {stmt.data_quality_score:.2f}")
    
    # Test DataFrame conversion
    print(f"\n3. Testing DataFrame conversion...")
    df = parser.to_dataframe(statements)
    print(f"   DataFrame shape: {df.shape}")
    print(f"   Columns: {len(df.columns)} financial metrics")
    
    if df.empty:
        print("   ERROR: DataFrame conversion failed")
        return False
    
    # Show sample columns
    financial_cols = [col for col in df.columns if col not in ['company_cik', 'company_name', 'fiscal_year', 'period_end', 'form_type', 'data_quality_score']]
    print(f"   Sample metrics: {financial_cols[:8]}...")
    
    # Generate data quality report
    print(f"\n4. Generating data quality report...")
    quality_report = parser.get_data_quality_report(statements)
    
    summary = quality_report.get('summary', {})
    print(f"   Statements processed: {summary.get('statements_extracted', 0)}")
    print(f"   Average quality score: {summary.get('avg_data_quality_score', 0):.3f}")
    print(f"   Concept coverage: {summary.get('overall_concept_coverage', 0)}/{summary.get('total_concepts_available', 0)}")
    print(f"   Years covered: {summary.get('years_covered', [])}")
    
    # Check for data issues
    issues = quality_report.get('data_issues', [])
    if issues:
        print(f"   Data issues found: {len(issues)}")
        for issue in issues[:3]:  # Show first 3 issues
            print(f"     - {issue}")
    else:
        print("   No major data issues detected")
    
    # Test export capabilities
    print(f"\n5. Testing export capabilities...")
    
    # Export to CSV (test only, don't actually save)
    try:
        csv_data = df.to_csv(index=False)
        print(f"   CSV export: {len(csv_data)} characters generated")
    except Exception as e:
        print(f"   CSV export error: {e}")
        return False
    
    # Validation checks
    print(f"\n6. Final validation checks...")
    
    # Check minimum data requirements
    required_years = 3
    if len(statements) < required_years:
        print(f"   WARNING: Only {len(statements)} years of data (expected {required_years}+)")
    else:
        print(f"   ✓ Sufficient historical data ({len(statements)} years)")
    
    # Check data quality
    avg_quality = summary.get('avg_data_quality_score', 0)
    if avg_quality < 0.7:
        print(f"   WARNING: Low average data quality ({avg_quality:.2f})")
    else:
        print(f"   ✓ Good data quality ({avg_quality:.2f})")
    
    # Check key metrics presence
    key_metrics = ['revenue', 'net_income', 'total_assets']
    latest_stmt = statements[0]
    missing_metrics = []
    
    for metric in key_metrics:
        value = (latest_stmt.income_statement.get(metric) or 
                latest_stmt.balance_sheet.get(metric) or 
                latest_stmt.cash_flow.get(metric))
        if not value:
            missing_metrics.append(metric)
    
    if missing_metrics:
        print(f"   WARNING: Missing key metrics: {missing_metrics}")
    else:
        print(f"   ✓ All key metrics present")
    
    print(f"\n=== CHECKPOINT 2 VALIDATION COMPLETE ===")
    
    if avg_quality >= 0.7 and len(statements) >= required_years and not missing_metrics:
        print("STATUS: ✅ CHECKPOINT 2 PASSED")
        return True
    else:
        print("STATUS: ⚠️  CHECKPOINT 2 NEEDS ATTENTION")
        return False

if __name__ == "__main__":
    success = test_checkpoint2()
    sys.exit(0 if success else 1)