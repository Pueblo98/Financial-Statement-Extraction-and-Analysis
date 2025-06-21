# Checkpoint Tests

This directory contains validation tests for each project checkpoint.

## Test Files

- `test_checkpoint2.py` - XBRL parser and financial data extraction validation
- `test_checkpoint3.py` - AI-powered narrative extraction system validation  
- `test_checkpoint3_simple.py` - Simplified narrative extraction test with manual section parsing
- `test_checkpoint4.py` - Performance analytics and derived metrics validation
- `test_html_debug.py` - HTML structure debugging utility

## Running Tests

From the project root directory:

```bash
# Run individual checkpoint tests
python tests/checkpoints/test_checkpoint2.py
python tests/checkpoints/test_checkpoint3_simple.py  
python tests/checkpoints/test_checkpoint4.py

# Set environment variables first
export GEMINI_API_KEY=your_api_key_here
# Or use .env file (recommended)
```

## Environment Setup

1. Ensure your `.env` file contains required API keys:
   ```
   GEMINI_API_KEY=your_gemini_api_key
   ```

2. Install dependencies:
   ```bash
   conda install pandas numpy python-dotenv beautifulsoup4 google-generativeai
   ```

## Test Structure

Each test validates:
- Core functionality implementation
- Data processing accuracy
- API integrations (where applicable)
- Output format and quality
- Error handling

Tests are designed to run independently and provide comprehensive validation of each checkpoint's requirements.