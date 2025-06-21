#!/usr/bin/env python3
"""
Generate Complete Financial Dataset
Creates the full comprehensive dataset with quantitative and qualitative data
"""

import sys
import os
sys.path.append('../src')

import pandas as pd
from dotenv import load_dotenv
from financialreader.comprehensive_builder import ComprehensiveFinancialDataBuilder

# Load environment variables
load_dotenv()

def main():
    """Generate complete dataset and export to Excel"""
    
    print("=== GENERATING COMPLETE FINANCIAL DATASET ===")
    print("Building comprehensive dataset with quantitative + qualitative data\n")
    
    # Initialize builder with Gemini API key
    builder = ComprehensiveFinancialDataBuilder(
        user_agent="Financial Analysis Tool contact@example.com",
        gemini_api_key=os.getenv('GEMINI_API_KEY')
    )
    
    # Build Apple comprehensive dataset
    print("1. Building Apple comprehensive dataset...")
    try:
        apple_df = builder.build_comprehensive_dataset('AAPL', years=5)
        print(f"   ✅ Apple dataset created: {apple_df.shape}")
        print(f"   Years covered: {sorted(apple_df['fiscal_year'].unique())}")
        print(f"   Total columns: {len(apple_df.columns)}")
        
        # Show data completeness
        summary = builder.get_dataset_summary(apple_df)
        print(f"   Financial completeness: {summary['data_completeness']['financial_metrics']:.1%}")
        print(f"   Performance completeness: {summary['data_completeness']['performance_metrics']:.1%}")
        print(f"   Narrative completeness: {summary['data_completeness']['narrative_insights']:.1%}")
        
    except Exception as e:
        print(f"   ❌ Error building Apple dataset: {e}")
        return
    
    # Export comprehensive dataset to Excel
    print(f"\n2. Exporting complete dataset to Excel...")
    try:
        export_path = builder.export_to_excel(
            apple_df, 
            'apple_complete_financial_dataset',
            export_directory='.'  # Export to examples folder
        )
        print(f"   ✅ Complete dataset exported to: {export_path}")
        
        # Show what's in the Excel file
        print(f"\n3. Excel file contains:")
        print(f"   📊 Comprehensive Data: All {apple_df.shape[1]} columns across {apple_df.shape[0]} years")
        print(f"   💰 Financial Statements: Core financial metrics")
        print(f"   📈 Performance Analytics: Derived ratios and growth metrics")
        print(f"   📝 Narrative Insights: AI-extracted business insights")
        print(f"   📋 Summary: Dataset overview and completeness statistics")
        
    except Exception as e:
        print(f"   ❌ Error exporting dataset: {e}")
        return
    
    # Show key insights from the data
    print(f"\n4. Key insights from Apple dataset:")
    
    # Latest year data
    latest_year = apple_df['fiscal_year'].max()
    latest_data = apple_df[apple_df['fiscal_year'] == latest_year].iloc[0]
    
    print(f"   📅 Latest year: {latest_year}")
    if 'revenue' in apple_df.columns:
        revenue = latest_data.get('revenue', 0)
        print(f"   💵 Revenue: ${revenue:,.0f}")
    
    if 'net_income' in apple_df.columns:
        net_income = latest_data.get('net_income', 0)
        print(f"   💰 Net Income: ${net_income:,.0f}")
    
    if 'net_profit_margin' in apple_df.columns:
        margin = latest_data.get('net_profit_margin', 0)
        print(f"   📊 Net Profit Margin: {margin:.1f}%")
    
    if 'business_segments' in apple_df.columns and pd.notna(latest_data.get('business_segments')):
        segments = latest_data.get('business_segments', '')
        print(f"   🏢 Business Segments: {segments[:100]}...")
    
    if 'key_risks' in apple_df.columns and pd.notna(latest_data.get('key_risks')):
        risks = latest_data.get('key_risks', '')
        print(f"   ⚠️  Key Risks: {risks[:100]}...")
    
    # Data quality analysis
    print(f"\n5. Data quality analysis:")
    
    # Check missing data by year
    for year in sorted(apple_df['fiscal_year'].unique()):
        year_data = apple_df[apple_df['fiscal_year'] == year]
        financial_complete = year_data[['revenue', 'net_income', 'total_assets']].notna().all(axis=1).iloc[0]
        
        if 'roe' in apple_df.columns:
            performance_complete = year_data[['roe', 'net_profit_margin']].notna().all(axis=1).iloc[0]
        else:
            performance_complete = True
            
        if 'business_segments' in apple_df.columns:
            narrative_complete = year_data[['business_segments']].notna().all(axis=1).iloc[0]
        else:
            narrative_complete = True
        
        status = "✅" if financial_complete and performance_complete else "⚠️"
        print(f"   {status} {year}: Financial={financial_complete}, Performance={performance_complete}, Narrative={narrative_complete}")
    
    print(f"\n=== COMPLETE DATASET GENERATION FINISHED ===")
    print(f"📁 Excel file ready for analysis: apple_complete_financial_dataset.xlsx")
    print(f"📊 Dataset contains {apple_df.shape[1]} columns across {apple_df.shape[0]} years")
    print(f"🎯 Ready for investment analysis, modeling, and research")

if __name__ == "__main__":
    main()