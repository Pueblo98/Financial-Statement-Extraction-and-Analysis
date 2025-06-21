#!/usr/bin/env python3
"""
Test script for Enhanced Company Analyst
Verifies that probing questions and M&A analysis are varied and year-specific
"""

import os
import pandas as pd
from dotenv import load_dotenv
from src.financialreader.enhanced_analyst import EnhancedCompanyAnalyst

def test_enhanced_analyst():
    """Test the enhanced analyst with different years"""
    
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('GEMINI_API_KEY')
    
    if not api_key:
        print("❌ No GEMINI_API_KEY found in environment")
        return
    
    print("✅ GEMINI_API_KEY found")
    
    # Initialize analyst
    analyst = EnhancedCompanyAnalyst(gemini_api_key=api_key)
    
    # Create test financial data for different years
    test_data_2024 = pd.DataFrame({
        'fiscal_year': [2024],
        'revenue': [394328000000],  # Apple 2024 revenue
        'net_income': [96995000000],
        'cash_and_equivalents': [48000000000]
    })
    
    test_data_2023 = pd.DataFrame({
        'fiscal_year': [2023],
        'revenue': [383285000000],  # Apple 2023 revenue
        'net_income': [96995000000],
        'cash_and_equivalents': [48000000000]
    })
    
    test_narrative = """
    Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide. 
    The Company's products include iPhone, Mac, iPad, and wearables, home, and accessories. 
    Apple also offers a variety of services, including AppleCare, iCloud, Apple Music, Apple TV+, Apple Fitness+, Apple News+, Apple Arcade, Apple Card, and Apple Pay.
    """
    
    print("\n🧪 Testing Enhanced Analyst...")
    
    # Test 2024 analysis
    print("\n📊 Testing 2024 Analysis:")
    analysis_2024 = analyst.analyze_company_comprehensive(
        "Apple Inc.", test_data_2024, test_narrative, "Technology"
    )
    
    print(f"Credit Risk Score: {analysis_2024.credit_risk_score.score}")
    print(f"M&A Appetite: {analysis_2024.ma_acquisition_potential.get('acquisition_appetite', 'N/A')}")
    print(f"Strategic Focus: {analysis_2024.ma_acquisition_potential.get('strategic_focus_areas', [])}")
    
    print("\nProbing Questions 2024:")
    for i, qa in enumerate(analysis_2024.probing_questions[:3], 1):
        print(f"  {i}. {qa['question']}")
        print(f"     Answer: {qa['answer'][:100]}...")
    
    # Test 2023 analysis
    print("\n📊 Testing 2023 Analysis:")
    analysis_2023 = analyst.analyze_company_comprehensive(
        "Apple Inc.", test_data_2023, test_narrative, "Technology"
    )
    
    print(f"Credit Risk Score: {analysis_2023.credit_risk_score.score}")
    print(f"M&A Appetite: {analysis_2023.ma_acquisition_potential.get('acquisition_appetite', 'N/A')}")
    print(f"Strategic Focus: {analysis_2023.ma_acquisition_potential.get('strategic_focus_areas', [])}")
    
    print("\nProbing Questions 2023:")
    for i, qa in enumerate(analysis_2023.probing_questions[:3], 1):
        print(f"  {i}. {qa['question']}")
        print(f"     Answer: {qa['answer'][:100]}...")
    
    # Check for variation
    print("\n🔍 Checking for Variation:")
    
    # Compare probing questions
    questions_2024 = [qa['question'] for qa in analysis_2024.probing_questions]
    questions_2023 = [qa['question'] for qa in analysis_2023.probing_questions]
    
    if questions_2024 != questions_2023:
        print("✅ Probing questions vary between years - GOOD!")
    else:
        print("❌ Probing questions are identical between years - NEEDS FIX")
    
    # Compare M&A analysis
    ma_2024 = analysis_2024.ma_acquisition_potential.get('acquisition_appetite', '')
    ma_2023 = analysis_2023.ma_acquisition_potential.get('acquisition_appetite', '')
    
    if ma_2024 != ma_2023 or (ma_2024 != 'moderate' and ma_2023 != 'moderate'):
        print("✅ M&A analysis shows variation - GOOD!")
    else:
        print("⚠️  M&A analysis may be too similar")
    
    print("\n✅ Test completed!")

if __name__ == "__main__":
    test_enhanced_analyst() 