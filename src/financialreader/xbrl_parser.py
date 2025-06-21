"""
XBRL Financial Data Parser
Extracts structured financial data from SEC Company Facts API
"""

import pandas as pd
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from datetime import datetime, date
import logging
import numpy as np

from edgar_client import SECEdgarClient
from gaap_taxonomy import GAAP_MAPPER, StatementType

@dataclass
class FinancialDataPoint:
    """Represents a single financial data point"""
    concept: str
    gaap_tag: str
    value: float
    unit: str
    end_date: str
    form: str
    frame: Optional[str] = None
    filed_date: Optional[str] = None

@dataclass
class FinancialStatement:
    """Represents a complete financial statement for a period"""
    company_cik: str
    company_name: str
    period_end: str
    fiscal_year: int
    form_type: str
    income_statement: Dict[str, float]
    balance_sheet: Dict[str, float]
    cash_flow: Dict[str, float]
    raw_data_points: List[FinancialDataPoint]
    data_quality_score: float

class XBRLFinancialParser:
    """
    Parses XBRL financial data from SEC Company Facts API
    Handles unit conversion, period alignment, and data validation
    """
    
    def __init__(self, edgar_client: SECEdgarClient = None):
        self.edgar_client = edgar_client or SECEdgarClient()
        self.logger = logging.getLogger(__name__)
        
        # Unit conversion factors (to normalize everything to dollars)
        self.unit_conversions = {
            'USD': 1,
            'USD/shares': 1,  # Keep as-is for per-share metrics
            'shares': 1,      # Keep as-is for share counts
        }
    
    def extract_company_financials(self, cik: str, years: int = 10) -> List[FinancialStatement]:
        """
        Extract financial statements for a company
        
        Args:
            cik: Company CIK
            years: Number of years to extract
            
        Returns:
            List of FinancialStatement objects, one per year
        """
        self.logger.info(f"Extracting financial data for CIK {cik}")
        
        # Get company facts from SEC API
        try:
            company_facts = self.edgar_client.get_company_facts(cik)
        except Exception as e:
            self.logger.error(f"Failed to get company facts for CIK {cik}: {e}")
            return []
        
        # Extract basic company info
        company_name = company_facts.get('entityName', 'Unknown')
        
        # Get US-GAAP facts
        us_gaap_facts = company_facts.get('facts', {}).get('us-gaap', {})
        if not us_gaap_facts:
            self.logger.error(f"No US-GAAP facts found for CIK {cik}")
            return []
        
        # Extract and organize financial data by period
        financial_statements = self._extract_statements_by_period(
            cik, company_name, us_gaap_facts, years
        )
        
        self.logger.info(f"Extracted {len(financial_statements)} financial statements")
        return financial_statements
    
    def _extract_statements_by_period(self, cik: str, company_name: str, 
                                    us_gaap_facts: Dict[str, Any], years: int) -> List[FinancialStatement]:
        """Extract financial statements organized by reporting period"""
        
        # First, collect all annual data points
        all_data_points = []
        current_year = datetime.now().year
        cutoff_year = current_year - years
        
        for concept in GAAP_MAPPER.get_all_concepts():
            # Find the best GAAP tag for this concept
            available_tags = list(us_gaap_facts.keys())
            best_tag = GAAP_MAPPER.find_best_tag(available_tags, concept)
            
            if not best_tag:
                continue
            
            # Extract data points for this tag
            tag_data = us_gaap_facts[best_tag]
            units = tag_data.get('units', {})
            
            for unit_type, data_points in units.items():
                for data_point in data_points:
                    end_date = data_point.get('end')
                    form = data_point.get('form', '')
                    
                    # Filter for annual reports and recent years
                    if not end_date or form not in ['10-K', '10-Q']:
                        continue
                    
                    try:
                        end_date_parsed = datetime.strptime(end_date, '%Y-%m-%d')
                        if end_date_parsed.year < cutoff_year:
                            continue
                    except ValueError:
                        continue
                    
                    # Create financial data point
                    financial_dp = FinancialDataPoint(
                        concept=concept,
                        gaap_tag=best_tag,
                        value=float(data_point.get('val', 0)),
                        unit=unit_type,
                        end_date=end_date,
                        form=form,
                        frame=data_point.get('frame'),
                        filed_date=data_point.get('filed')
                    )
                    
                    all_data_points.append(financial_dp)
        
        # Group data points by period and form type (prioritize 10-K over 10-Q)
        statements_by_period = {}
        
        for dp in all_data_points:
            # Use fiscal year end as the key
            try:
                end_date = datetime.strptime(dp.end_date, '%Y-%m-%d')
                # Assume fiscal year end if it's in Q4 (Oct, Nov, Dec) or close to year end
                if end_date.month >= 9 or dp.form == '10-K':
                    fiscal_year = end_date.year
                    
                    period_key = fiscal_year
                    if period_key not in statements_by_period:
                        statements_by_period[period_key] = []
                    
                    statements_by_period[period_key].append(dp)
            except ValueError:
                continue
        
        # Convert to FinancialStatement objects
        financial_statements = []
        
        for fiscal_year, data_points in statements_by_period.items():
            # Prioritize 10-K data over 10-Q
            annual_data_points = [dp for dp in data_points if dp.form == '10-K']
            if not annual_data_points:
                # Fall back to 10-Q if no 10-K available
                annual_data_points = [dp for dp in data_points if dp.form == '10-Q']
            
            if not annual_data_points:
                continue
            
            # Find the most recent filing for this fiscal year
            annual_data_points.sort(key=lambda x: x.filed_date or '', reverse=True)
            primary_filing_date = annual_data_points[0].filed_date
            
            # Filter to data points from the same filing
            same_filing_points = [
                dp for dp in annual_data_points 
                if dp.filed_date == primary_filing_date
            ]
            
            # Create financial statement
            statement = self._create_financial_statement(
                cik, company_name, fiscal_year, same_filing_points
            )
            
            if statement:
                financial_statements.append(statement)
        
        # Sort by fiscal year (most recent first)
        financial_statements.sort(key=lambda x: x.fiscal_year, reverse=True)
        
        return financial_statements
    
    def _create_financial_statement(self, cik: str, company_name: str, 
                                  fiscal_year: int, data_points: List[FinancialDataPoint]) -> Optional[FinancialStatement]:
        """Create a FinancialStatement object from data points"""
        
        if not data_points:
            return None
        
        # Organize data points by statement type
        income_statement = {}
        balance_sheet = {}
        cash_flow = {}
        
        # Track data quality
        expected_concepts = GAAP_MAPPER.get_required_concepts()
        found_concepts = set()
        
        for dp in data_points:
            # Normalize the value based on unit
            normalized_value = self._normalize_value(dp.value, dp.unit)
            
            # Get statement type for this concept
            gaap_tag_info = GAAP_MAPPER.get_gaap_tag(dp.concept)
            if not gaap_tag_info:
                continue
            
            found_concepts.add(dp.concept)
            
            if gaap_tag_info.statement_type == StatementType.INCOME_STATEMENT:
                income_statement[dp.concept] = normalized_value
            elif gaap_tag_info.statement_type == StatementType.BALANCE_SHEET:
                balance_sheet[dp.concept] = normalized_value
            elif gaap_tag_info.statement_type == StatementType.CASH_FLOW:
                cash_flow[dp.concept] = normalized_value
        
        # Calculate data quality score
        required_found = len([c for c in expected_concepts if c in found_concepts])
        data_quality_score = required_found / len(expected_concepts) if expected_concepts else 0
        
        # Get period end date (use the most common end date)
        end_dates = [dp.end_date for dp in data_points]
        period_end = max(set(end_dates), key=end_dates.count)
        
        # Get form type
        form_type = data_points[0].form
        
        return FinancialStatement(
            company_cik=cik,
            company_name=company_name,
            period_end=period_end,
            fiscal_year=fiscal_year,
            form_type=form_type,
            income_statement=income_statement,
            balance_sheet=balance_sheet,
            cash_flow=cash_flow,
            raw_data_points=data_points,
            data_quality_score=data_quality_score
        )
    
    def _normalize_value(self, value: float, unit: str) -> float:
        """Normalize financial values to standard units"""
        # Most SEC filings are already in actual dollars or per-share amounts
        # No conversion needed for most cases
        return value
    
    def to_dataframe(self, financial_statements: List[FinancialStatement]) -> pd.DataFrame:
        """
        Convert financial statements to a pandas DataFrame
        
        Args:
            financial_statements: List of FinancialStatement objects
            
        Returns:
            DataFrame with financial metrics as columns, years as rows
        """
        if not financial_statements:
            return pd.DataFrame()
        
        # Collect all possible concepts
        all_concepts = set()
        for stmt in financial_statements:
            all_concepts.update(stmt.income_statement.keys())
            all_concepts.update(stmt.balance_sheet.keys())
            all_concepts.update(stmt.cash_flow.keys())
        
        # Create DataFrame
        rows = []
        for stmt in financial_statements:
            row = {
                'company_cik': stmt.company_cik,
                'company_name': stmt.company_name,
                'fiscal_year': stmt.fiscal_year,
                'period_end': stmt.period_end,
                'form_type': stmt.form_type,
                'data_quality_score': stmt.data_quality_score
            }
            
            # Add all financial metrics
            for concept in all_concepts:
                value = (stmt.income_statement.get(concept) or 
                        stmt.balance_sheet.get(concept) or 
                        stmt.cash_flow.get(concept))
                row[concept] = value
            
            rows.append(row)
        
        df = pd.DataFrame(rows)
        
        # Sort by fiscal year
        df = df.sort_values('fiscal_year', ascending=False)
        df = df.reset_index(drop=True)
        
        return df
    
    def get_data_quality_report(self, financial_statements: List[FinancialStatement]) -> Dict[str, Any]:
        """Generate a data quality report for extracted financial statements"""
        
        if not financial_statements:
            return {"error": "No financial statements provided"}
        
        # Calculate coverage statistics
        all_required_concepts = GAAP_MAPPER.get_required_concepts()
        
        coverage_by_year = {}
        overall_coverage = set()
        
        for stmt in financial_statements:
            found_concepts = set()
            found_concepts.update(stmt.income_statement.keys())
            found_concepts.update(stmt.balance_sheet.keys())
            found_concepts.update(stmt.cash_flow.keys())
            
            required_found = [c for c in all_required_concepts if c in found_concepts]
            coverage_by_year[stmt.fiscal_year] = {
                'found_concepts': len(found_concepts),
                'required_found': len(required_found),
                'coverage_pct': len(required_found) / len(all_required_concepts) * 100,
                'data_quality_score': stmt.data_quality_score
            }
            
            overall_coverage.update(found_concepts)
        
        # Missing concepts analysis
        missing_concepts = [c for c in all_required_concepts if c not in overall_coverage]
        
        # Summary statistics
        avg_quality_score = np.mean([stmt.data_quality_score for stmt in financial_statements])
        years_covered = [stmt.fiscal_year for stmt in financial_statements]
        
        return {
            'summary': {
                'statements_extracted': len(financial_statements),
                'years_covered': sorted(years_covered, reverse=True),
                'avg_data_quality_score': round(avg_quality_score, 3),
                'overall_concept_coverage': len(overall_coverage),
                'total_concepts_available': len(all_required_concepts)
            },
            'coverage_by_year': coverage_by_year,
            'missing_concepts': missing_concepts,
            'data_issues': self._identify_data_issues(financial_statements)
        }
    
    def _identify_data_issues(self, financial_statements: List[FinancialStatement]) -> List[str]:
        """Identify potential data quality issues"""
        issues = []
        
        for stmt in financial_statements:
            # Check for basic completeness
            if not stmt.income_statement.get('revenue') and not stmt.income_statement.get('net_income'):
                issues.append(f"FY{stmt.fiscal_year}: Missing basic income statement data")
            
            if not stmt.balance_sheet.get('total_assets'):
                issues.append(f"FY{stmt.fiscal_year}: Missing total assets")
            
            if stmt.data_quality_score < 0.5:
                issues.append(f"FY{stmt.fiscal_year}: Low data quality score ({stmt.data_quality_score:.2f})")
            
            # Check for balance sheet equation
            assets = stmt.balance_sheet.get('total_assets', 0)
            liabilities = stmt.balance_sheet.get('total_liabilities', 0)
            equity = stmt.balance_sheet.get('shareholders_equity', 0)
            
            if assets > 0 and liabilities > 0 and equity > 0:
                balance_diff = abs(assets - (liabilities + equity))
                if balance_diff / assets > 0.01:  # 1% tolerance
                    issues.append(f"FY{stmt.fiscal_year}: Balance sheet equation imbalance")
        
        return issues


if __name__ == "__main__":
    # Example usage
    from edgar_client import CompanyLookup
    
    client = SECEdgarClient("Financial Analysis Tool test@example.com")
    parser = XBRLFinancialParser(client)
    lookup = CompanyLookup(client)
    
    # Test with Apple
    apple_cik = lookup.get_cik_by_ticker('AAPL')
    if apple_cik:
        print(f"Extracting financial data for Apple (CIK: {apple_cik})")
        
        statements = parser.extract_company_financials(apple_cik, years=5)
        print(f"Extracted {len(statements)} financial statements")
        
        # Show first statement
        if statements:
            stmt = statements[0]
            print(f"\nFY{stmt.fiscal_year} Summary:")
            print(f"  Revenue: ${stmt.income_statement.get('revenue', 0):,.0f}")
            print(f"  Net Income: ${stmt.income_statement.get('net_income', 0):,.0f}")
            print(f"  Total Assets: ${stmt.balance_sheet.get('total_assets', 0):,.0f}")
            print(f"  Data Quality Score: {stmt.data_quality_score:.2f}")
        
        # Generate data quality report
        quality_report = parser.get_data_quality_report(statements)
        print(f"\nData Quality Report:")
        print(f"  Average Quality Score: {quality_report['summary']['avg_data_quality_score']}")
        print(f"  Concept Coverage: {quality_report['summary']['overall_concept_coverage']}")
        
        # Convert to DataFrame
        df = parser.to_dataframe(statements)
        print(f"\nDataFrame shape: {df.shape}")
        if not df.empty:
            print(f"Columns: {list(df.columns)[:10]}...")  # Show first 10 columns