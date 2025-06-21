"""
Simple test of XBRL parser without pandas dependency
"""

from edgar_client import SECEdgarClient, CompanyLookup
from gaap_taxonomy import GAAP_MAPPER
import json

def test_xbrl_extraction():
    """Test XBRL data extraction"""
    
    client = SECEdgarClient("Financial Analysis Tool test@example.com")
    lookup = CompanyLookup(client)
    
    # Get Apple's company facts
    apple_cik = lookup.get_cik_by_ticker('AAPL')
    print(f"Apple CIK: {apple_cik}")
    
    company_facts = client.get_company_facts(apple_cik)
    company_name = company_facts.get('entityName', 'Unknown')
    print(f"Company: {company_name}")
    
    # Get US-GAAP facts
    us_gaap_facts = company_facts.get('facts', {}).get('us-gaap', {})
    print(f"Available GAAP tags: {len(us_gaap_facts)}")
    
    # Test mapping key concepts
    key_concepts = ['revenue', 'net_income', 'total_assets', 'shareholders_equity', 'operating_cash_flow']
    
    print("\n=== KEY FINANCIAL METRICS ===")
    for concept in key_concepts:
        # Find best tag for concept
        available_tags = list(us_gaap_facts.keys())
        best_tag = GAAP_MAPPER.find_best_tag(available_tags, concept)
        
        if best_tag and best_tag in us_gaap_facts:
            tag_data = us_gaap_facts[best_tag]
            
            # Get latest annual value
            usd_units = tag_data.get('units', {}).get('USD', [])
            annual_values = []
            
            for data_point in usd_units:
                form = data_point.get('form', '')
                end_date = data_point.get('end', '')
                value = data_point.get('val', 0)
                
                if form == '10-K' and end_date:
                    try:
                        year = int(end_date[:4])
                        if year >= 2020:  # Recent years only
                            annual_values.append({
                                'year': year,
                                'value': value,
                                'end_date': end_date
                            })
                    except ValueError:
                        continue
            
            # Sort by year (most recent first)
            annual_values.sort(key=lambda x: x['year'], reverse=True)
            
            print(f"\n{concept.upper()} ({best_tag}):")
            for val in annual_values[:5]:  # Show last 5 years
                if concept in ['revenue', 'net_income', 'total_assets', 'shareholders_equity']:
                    print(f"  {val['year']}: ${val['value']:,.0f} million")
                else:
                    print(f"  {val['year']}: ${val['value']:,.0f}")
        else:
            print(f"\n{concept.upper()}: NOT FOUND")
    
    # Test data completeness
    print(f"\n=== DATA COMPLETENESS CHECK ===")
    required_concepts = GAAP_MAPPER.get_required_concepts()
    available_tags = list(us_gaap_facts.keys())
    
    found_concepts = []
    missing_concepts = []
    
    for concept in required_concepts:
        best_tag = GAAP_MAPPER.find_best_tag(available_tags, concept)
        if best_tag:
            found_concepts.append(concept)
        else:
            missing_concepts.append(concept)
    
    coverage_pct = len(found_concepts) / len(required_concepts) * 100
    print(f"Required concepts found: {len(found_concepts)}/{len(required_concepts)} ({coverage_pct:.1f}%)")
    
    if missing_concepts:
        print(f"Missing concepts: {missing_concepts[:5]}...")  # Show first 5
    
    # Test by statement type
    income_concepts = GAAP_MAPPER.get_concepts_by_statement(GAAP_MAPPER._tag_mapping['revenue'].statement_type)
    balance_concepts = GAAP_MAPPER.get_concepts_by_statement(GAAP_MAPPER._tag_mapping['total_assets'].statement_type)
    
    print(f"\nIncome Statement concepts available: {len([c for c in income_concepts if GAAP_MAPPER.find_best_tag(available_tags, c)])}/{len(income_concepts)}")
    print(f"Balance Sheet concepts available: {len([c for c in balance_concepts if GAAP_MAPPER.find_best_tag(available_tags, c)])}/{len(balance_concepts)}")
    
    return True

if __name__ == "__main__":
    test_xbrl_extraction()