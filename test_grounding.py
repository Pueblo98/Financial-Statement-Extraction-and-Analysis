#!/usr/bin/env python3
"""
Test script for Google GenAI Grounding Tools
Verifies that the grounding configuration works for real-time data access
"""

import os
from dotenv import load_dotenv
import pandas as pd

def test_grounding_configuration():
    """Test the grounding configuration with Google GenAI"""
    
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('GEMINI_API_KEY')
    
    if not api_key:
        print("❌ No GEMINI_API_KEY found in environment")
        return
    
    print("✅ GEMINI_API_KEY found")
    
    try:
        from google import genai
        from google.genai import types
        print("✅ Google GenAI library imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import Google GenAI: {e}")
        print("Please install: pip install google-genai")
        return
    
    try:
        # Configure the client
        client = genai.Client(api_key=api_key)
        print("✅ Google GenAI client configured")
        
        # Define the grounding tool
        grounding_tool = types.Tool(
            google_search=types.GoogleSearch()
        )
        print("✅ Grounding tool configured")
        
        # Configure generation settings
        config = types.GenerateContentConfig(
            tools=[grounding_tool]
        )
        print("✅ Generation config with grounding tools created")
        
        # Test with a simple query
        print("\n🧪 Testing grounding with simple query...")
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents="What is Apple's current stock price?",
            config=config,
        )
        
        print(f"✅ Grounding test successful!")
        if response and hasattr(response, 'text') and response.text:
            print(f"Response: {response.text[:200]}...")
        else:
            print("Response: No text content available")
        
        # Test with financial data query
        print("\n🧪 Testing grounding with financial data...")
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents="What was Apple's revenue in fiscal year 2024?",
            config=config,
        )
        
        print(f"✅ Financial data grounding test successful!")
        if response and hasattr(response, 'text') and response.text:
            print(f"Response: {response.text[:200]}...")
        else:
            print("Response: No text content available")
        
        return True
        
    except Exception as e:
        print(f"❌ Grounding test failed: {e}")
        return False

def test_enhanced_analyst_grounding():
    """Test the enhanced analyst with grounding tools"""
    
    print("\n🧪 Testing Enhanced Analyst with Grounding...")
    
    try:
        from src.financialreader.enhanced_analyst import EnhancedCompanyAnalyst
        
        # Load environment variables
        load_dotenv()
        api_key = os.getenv('GEMINI_API_KEY')
        
        if not api_key:
            print("❌ No GEMINI_API_KEY found")
            return False
        
        # Initialize analyst with grounding
        analyst = EnhancedCompanyAnalyst(gemini_api_key=api_key)
        
        # Create test data
        test_data = pd.DataFrame({
            'fiscal_year': [2024],
            'revenue': [394328000000],
            'net_income': [96995000000],
            'cash_and_equivalents': [48000000000],
            'total_assets': [352755000000]
        })
        
        test_narrative = "Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide."
        
        # Test analysis with grounding
        analysis = analyst.analyze_company_comprehensive(
            "Apple Inc.", test_data, test_narrative, "Technology"
        )
        
        print(f"✅ Enhanced analyst with grounding successful!")
        print(f"Credit Risk Score: {analysis.credit_risk_score.score}")
        print(f"M&A Appetite: {analysis.ma_acquisition_potential.get('acquisition_appetite', 'N/A')}")
        
        print("\nProbing Questions (should use real-time data):")
        for i, qa in enumerate(analysis.probing_questions[:3], 1):
            print(f"  {i}. {qa['question']}")
            print(f"     Answer: {qa['answer'][:100]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Enhanced analyst grounding test failed: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Testing Google GenAI Grounding Configuration")
    print("=" * 50)
    
    # Test basic grounding
    grounding_works = test_grounding_configuration()
    
    if grounding_works:
        # Test enhanced analyst with grounding
        analyst_works = test_enhanced_analyst_grounding()
        
        if analyst_works:
            print("\n🎉 All grounding tests passed!")
            print("The enhanced analyst should now use real-time data instead of fallback responses.")
        else:
            print("\n⚠️  Enhanced analyst grounding needs attention")
    else:
        print("\n❌ Grounding configuration failed - check API key and library installation") 