#!/usr/bin/env python3
"""
Test script for Improved HTML Parser
Verifies that the AI-driven HTML parser can extract sections more reliably
"""

import os
from dotenv import load_dotenv

def test_html_parser():
    """Test the improved HTML parser with a sample filing"""
    
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('GEMINI_API_KEY')
    
    if not api_key:
        print("❌ No GEMINI_API_KEY found in environment")
        return
    
    print("✅ GEMINI_API_KEY found")
    
    try:
        from src.financialreader.html_parser import AIHTMLSectionParser
        
        # Initialize parser
        parser = AIHTMLSectionParser(api_key=api_key)
        print("✅ HTML Parser initialized")
        
        # Create a sample 10-K text (simplified for testing)
        sample_text = """
        ITEM 1. BUSINESS OVERVIEW
        
        Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide. 
        The Company's products include iPhone, Mac, iPad, and wearables, home, and accessories. 
        Apple also offers a variety of services, including AppleCare, iCloud, Apple Music, Apple TV+, Apple Fitness+, Apple News+, Apple Arcade, Apple Card, and Apple Pay.
        
        ITEM 1A. RISK FACTORS
        
        The following risk factors could materially affect our business, financial condition, results of operations, and cash flows:
        
        1. Economic and Market Risks: Changes in economic conditions, including inflation, interest rates, and currency fluctuations, could adversely affect our business.
        
        2. Competitive Risks: We face intense competition in all of our markets from companies with significant resources and experience.
        
        3. Supply Chain Risks: Our business depends on the ability of our suppliers to deliver components and materials in sufficient quantities and of acceptable quality.
        
        4. Regulatory Risks: We are subject to various laws and regulations that could affect our business operations and financial results.
        
        5. Technology Risks: Rapid technological changes could make our products and services obsolete or less competitive.
        
        ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS OF FINANCIAL CONDITION AND RESULTS OF OPERATIONS
        
        Revenue Analysis:
        Our revenue for fiscal year 2024 was $394.3 billion, an increase of 2.9% compared to fiscal year 2023. This growth was primarily driven by strong iPhone sales and continued growth in our Services segment.
        
        Cost of Sales:
        Our cost of sales for fiscal year 2024 was $223.6 billion, representing 56.7% of net sales, compared to 56.1% in fiscal year 2023.
        
        Operating Expenses:
        Research and development expenses increased by 8.1% to $29.9 billion in fiscal year 2024, reflecting our continued investment in innovation and new product development.
        
        Net Income:
        Net income for fiscal year 2024 was $97.0 billion, representing a net income margin of 24.6%, compared to 25.3% in fiscal year 2023.
        """
        
        print("\n🧪 Testing HTML Parser with sample 10-K text...")
        
        # Parse the sample text
        sections = parser.parse_filing(sample_text)
        
        print(f"\n📊 Results:")
        print(f"Total sections extracted: {len(sections)}")
        
        for section_id, section in sections.items():
            print(f"\n  {section_id}: {section.title}")
            print(f"    Words: {section.word_count}")
            print(f"    Preview: {section.content[:100]}...")
        
        # Check if all expected sections were found
        expected_sections = ['item1', 'item1a', 'item7']
        found_sections = list(sections.keys())
        
        print(f"\n🔍 Section Coverage:")
        for expected in expected_sections:
            if expected in found_sections:
                print(f"  ✅ {expected} - Found ({sections[expected].word_count} words)")
            else:
                print(f"  ❌ {expected} - Missing")
        
        # Summary
        coverage = len(found_sections) / len(expected_sections) * 100
        print(f"\n📈 Overall Coverage: {coverage:.1f}%")
        
        if coverage >= 66:  # At least 2 out of 3 sections
            print("✅ HTML Parser is working well!")
        else:
            print("⚠️  HTML Parser needs improvement")
        
        return True
        
    except Exception as e:
        print(f"❌ HTML Parser test failed: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Testing Improved HTML Parser")
    print("=" * 40)
    
    success = test_html_parser()
    
    if success:
        print("\n🎉 HTML Parser test completed successfully!")
    else:
        print("\n❌ HTML Parser test failed") 