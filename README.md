# Financial Statement Reader

An automated pipeline for extracting and analyzing financial data from SEC EDGAR filings.

## Features

- **EDGAR Data Acquisition**: Automated retrieval of 10-K filings from SEC EDGAR database
- **XBRL Financial Parsing**: Extract structured financial data using US-GAAP taxonomy
- **Data Validation**: Comprehensive validation and quality checks for financial data
- **Storage Management**: Organized storage system with metadata tracking and deduplication
- **AI-Powered Analysis**: Extract narrative insights from MD&A and business sections

## Installation

### Using Conda (Recommended)

```bash
# Create conda environment
conda create -n financial-reader python=3.10 pandas numpy requests beautifulsoup4 lxml openpyxl
conda activate financial-reader

# Install the package
pip install -e .
```

### Using pip

```bash
pip install -r requirements.txt
pip install -e .
```

## Quick Start

```python
from financialreader import FilingManager

# Initialize the filing manager
manager = FilingManager()

# Retrieve Apple's 10-K filings for the last 10 years
result = manager.retrieve_company_filings("AAPL", years=10, form_type="10-K")

print(f"Downloaded {result.new_downloads} new filings")
print(f"Company: {result.company_info['name']}")
```

## Project Structure

```
financialstatementreader/
├── src/financialreader/          # Core package
│   ├── __init__.py
│   ├── edgar_client.py           # SEC EDGAR API client
│   ├── filing_manager.py         # Main filing management
│   ├── filing_storage.py         # Storage and organization
│   ├── gaap_taxonomy.py          # US-GAAP mapping
│   ├── xbrl_parser.py            # Financial data extraction
│   └── data_validator.py         # Data validation
├── tests/                        # Test files
├── docs/                         # Documentation
├── examples/                     # Example scripts
├── data/                         # Data storage (created at runtime)
├── requirements.txt              # Dependencies
├── setup.py                     # Package setup
└── README.md                    # This file
```

## Usage Examples

### Extract Financial Data

```python
from financialreader import XBRLFinancialParser, SECEdgarClient

client = SECEdgarClient("Your App Name contact@example.com")
parser = XBRLFinancialParser(client)

# Extract Apple's financial statements
statements = parser.extract_company_financials("0000320193", years=5)

# Convert to DataFrame
df = parser.to_dataframe(statements)
print(df[['fiscal_year', 'revenue', 'net_income', 'total_assets']].head())
```

### Validate Data Quality

```python
from financialreader import FinancialDataValidator

validator = FinancialDataValidator()
result = validator.validate_financial_statement(
    income_statement, balance_sheet, cash_flow, fiscal_year=2024
)

print(f"Data is valid: {result.is_valid}")
print(f"Quality score: {result.score}")
```

## Development

### Setting up Development Environment

```bash
# Clone the repository
git clone https://github.com/Pueblo98/financialstatementreader.git
cd financialstatementreader

# Create conda environment
conda create -n financial-reader-dev python=3.10 pandas numpy requests beautifulsoup4 lxml openpyxl pytest black flake8
conda activate financial-reader-dev

# Install in development mode
pip install -e .[dev]
```

### Running Tests

```bash
pytest tests/
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- SEC EDGAR for providing free access to financial data
- XBRL US for GAAP taxonomy documentation
- pandas and numpy communities for excellent data tools