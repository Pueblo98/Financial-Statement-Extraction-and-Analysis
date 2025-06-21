#!/usr/bin/env python3
"""
Test Checkpoint 3: AI-Powered Narrative Extraction System
"""

import sys
import os
sys.path.append('src')

from financialreader.narrative_pipeline import NarrativeDataPipeline
from financialreader.edgar_client import SECEdgarClient, CompanyLookup

def test_checkpoint3():
    """Test complete Checkpoint 3 functionality"""
    
    print("=== CHECKPOINT 3 VALIDATION ===")
    print("Testing AI-Powered Narrative Extraction System\n")
    
    # Initialize pipeline with Gemini API key
    gemini_api_key = "AIzaSyCmUqFSaOPyhHO381Tbb8cqYSLLNZocmFk"
    pipeline = NarrativeDataPipeline(gemini_api_key=gemini_api_key)
    
    # Test with Apple
    print("1. Testing Apple (AAPL) narrative extraction...")
    apple_cik = "0000320193"
    
    try:
        # Analyze narrative for 2 years (to keep test manageable)
        analyses = pipeline.analyze_company_narrative(apple_cik, years=2)
        print(f"   Analyzed {len(analyses)} filings")
        
        if not analyses:
            print("   ERROR: No narrative analyses generated")
            return False
        
        # Display results for most recent analysis
        recent = analyses[0]
        print(f"\n2. Apple FY{recent.fiscal_year} Narrative Analysis:")
        print(f"   Sections analyzed: {list(recent.sections_parsed.keys())}")
        print(f"   Total word count: {sum(s.word_count for s in recent.sections_parsed.values())}")
        
        # Show research insights
        insight = recent.research_insight
        print(f"   Business segments: {insight.business_segments}")
        print(f"   Performance drivers: {insight.performance_drivers}")
        print(f"   Top risks: {insight.top_risks[:3]}")
        print(f"   Strategic initiatives: {insight.strategic_initiatives}")
        
        # Show section extractions
        print(f"\n3. Section-specific insights:")
        for section_id, extraction in recent.section_extractions.items():
            print(f"   {section_id}:")
            
            if extraction.business_overview:
                print(f"     Business model: {extraction.business_overview.get('business_model', 'N/A')[:100]}...")
            
            if extraction.risk_assessment:
                risks = extraction.risk_assessment.get('material_risks', [])
                print(f"     Material risks: {risks[:2]}")
            
            if extraction.performance_analysis:
                drivers = extraction.performance_analysis.get('revenue_drivers', [])
                print(f"     Revenue drivers: {drivers[:2]}")
        
        # Test DataFrame conversion
        print(f"\n4. Testing DataFrame conversion...")
        df = pipeline.to_dataframe(analyses)
        print(f"   DataFrame shape: {df.shape}")
        print(f"   Columns: {len(df.columns)} narrative metrics")
        
        if df.empty:
            print("   ERROR: DataFrame conversion failed")
            return False
        
        # Show sample columns
        narrative_cols = [col for col in df.columns if col not in ['company_cik', 'company_name', 'fiscal_year']]
        print(f"   Sample metrics: {narrative_cols[:5]}...")
        
        # Generate summary report
        print(f"\n5. Generating comprehensive summary report...")
        report = pipeline.get_narrative_summary_report(analyses)
        
        company_overview = report.get('company_overview', {})
        print(f"   Company: {company_overview.get('name', 'Unknown')}")
        print(f"   Years analyzed: {company_overview.get('years_analyzed', [])}")
        print(f"   Total filings: {company_overview.get('total_filings', 0)}")
        
        business_insights = report.get('business_insights', {})
        print(f"   All segments: {business_insights.get('all_segments', [])[:3]}...")
        print(f"   Recurring segments: {business_insights.get('recurring_segments', [])}")
        
        risk_analysis = report.get('risk_analysis', {})
        print(f"   Recurring risks: {risk_analysis.get('recurring_risks', [])[:3]}...")
        
        # Test export capabilities
        print(f"\n6. Testing export capabilities...")
        
        try:
            csv_data = df.to_csv(index=False)
            print(f"   CSV export: {len(csv_data)} characters generated")
        except Exception as e:
            print(f"   CSV export error: {e}")
            return False
        
        # Validation checks
        print(f"\n7. Final validation checks...")
        
        # Check minimum requirements
        required_sections = ['item1', 'item1a', 'item7']
        found_sections = list(recent.sections_parsed.keys())
        missing_sections = [s for s in required_sections if s not in found_sections]
        
        if missing_sections:
            print(f"   WARNING: Missing sections: {missing_sections}")
        else:
            print(f"   ✓ All required sections found")
        
        # Check AI insights quality
        total_insights = len(insight.business_segments) + len(insight.top_risks) + len(insight.strategic_initiatives)
        if total_insights < 5:
            print(f"   WARNING: Limited AI insights ({total_insights} items)")
        else:
            print(f"   ✓ Rich AI insights ({total_insights} items)")
        
        # Check section extractions
        extraction_count = len(recent.section_extractions)
        if extraction_count == 0:
            print(f"   WARNING: No section extractions")
        else:
            print(f"   ✓ Section extractions completed ({extraction_count} sections)")
        
        print(f"\n=== CHECKPOINT 3 VALIDATION COMPLETE ===")
        
        if not missing_sections and total_insights >= 5 and extraction_count > 0:
            print("STATUS: ✅ CHECKPOINT 3 PASSED")
            return True
        else:
            print("STATUS: ⚠️  CHECKPOINT 3 NEEDS ATTENTION")
            return False
    
    except Exception as e:
        print(f"   ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_checkpoint3()
    sys.exit(0 if success else 1)