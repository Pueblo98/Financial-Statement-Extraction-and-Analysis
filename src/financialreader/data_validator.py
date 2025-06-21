"""
Financial Data Validation and Quality Checks
Validates extracted financial data for consistency and completeness
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import logging
from datetime import datetime

@dataclass
class ValidationResult:
    """Results of data validation"""
    is_valid: bool
    score: float  # 0-1, higher is better
    issues: List[str]
    warnings: List[str]
    metrics: Dict[str, Any]

@dataclass 
class FinancialRatios:
    """Common financial ratios for validation"""
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    current_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    return_on_equity: Optional[float] = None
    return_on_assets: Optional[float] = None

class FinancialDataValidator:
    """
    Validates financial data for consistency, completeness, and reasonableness
    Performs cross-statement checks and identifies potential data issues
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Reasonable ranges for financial ratios (industry-agnostic)
        self.ratio_ranges = {
            'gross_margin': (0.0, 1.0),      # 0% to 100%
            'operating_margin': (-0.5, 1.0), # -50% to 100%
            'net_margin': (-0.5, 0.6),       # -50% to 60%
            'current_ratio': (0.1, 10.0),    # 0.1 to 10.0
            'debt_to_equity': (0.0, 10.0),   # 0 to 10.0
            'return_on_equity': (-1.0, 2.0), # -100% to 200%
            'return_on_assets': (-0.5, 0.5), # -50% to 50%
        }
    
    def validate_financial_statement(self, income_statement: Dict[str, float],
                                   balance_sheet: Dict[str, float],
                                   cash_flow: Dict[str, float],
                                   fiscal_year: int) -> ValidationResult:
        """
        Validate a complete financial statement
        
        Args:
            income_statement: Income statement data
            balance_sheet: Balance sheet data  
            cash_flow: Cash flow statement data
            fiscal_year: Fiscal year of the data
            
        Returns:
            ValidationResult with validation outcome
        """
        issues = []
        warnings = []
        
        # 1. Completeness validation
        completeness_issues = self._validate_completeness(
            income_statement, balance_sheet, cash_flow
        )
        issues.extend(completeness_issues)
        
        # 2. Mathematical consistency validation
        consistency_issues = self._validate_mathematical_consistency(
            income_statement, balance_sheet, cash_flow
        )
        issues.extend(consistency_issues)
        
        # 3. Balance sheet equation validation
        balance_issues = self._validate_balance_sheet_equation(balance_sheet)
        issues.extend(balance_issues)
        
        # 4. Reasonableness validation
        reasonableness_issues, reasonableness_warnings = self._validate_reasonableness(
            income_statement, balance_sheet, cash_flow
        )
        issues.extend(reasonableness_issues)
        warnings.extend(reasonableness_warnings)
        
        # 5. Calculate financial ratios for additional validation
        ratios = self._calculate_financial_ratios(
            income_statement, balance_sheet, cash_flow
        )
        
        ratio_issues = self._validate_financial_ratios(ratios)
        warnings.extend(ratio_issues)
        
        # Calculate overall validation score
        total_checks = 20  # Approximate number of validation checks
        failed_checks = len(issues)
        warning_weight = 0.5
        weighted_warnings = len(warnings) * warning_weight
        
        score = max(0, (total_checks - failed_checks - weighted_warnings) / total_checks)
        
        # Determine if data is valid (score > 0.7 and no critical issues)
        critical_issue_keywords = ['balance sheet equation', 'negative', 'missing required']
        has_critical_issues = any(
            any(keyword in issue.lower() for keyword in critical_issue_keywords)
            for issue in issues
        )
        
        is_valid = score > 0.7 and not has_critical_issues
        
        # Compile metrics
        metrics = {
            'fiscal_year': fiscal_year,
            'total_issues': len(issues),
            'total_warnings': len(warnings),
            'financial_ratios': ratios,
            'completeness_score': self._calculate_completeness_score(
                income_statement, balance_sheet, cash_flow
            )
        }
        
        return ValidationResult(
            is_valid=is_valid,
            score=score,
            issues=issues,
            warnings=warnings,
            metrics=metrics
        )
    
    def _validate_completeness(self, income_statement: Dict[str, float],
                             balance_sheet: Dict[str, float],
                             cash_flow: Dict[str, float]) -> List[str]:
        """Validate data completeness"""
        issues = []
        
        # Required income statement items
        required_income = ['revenue', 'net_income']
        for item in required_income:
            if item not in income_statement or income_statement[item] is None:
                issues.append(f"Missing required income statement item: {item}")
        
        # Required balance sheet items
        required_balance = ['total_assets', 'total_liabilities', 'shareholders_equity']
        for item in required_balance:
            if item not in balance_sheet or balance_sheet[item] is None:
                issues.append(f"Missing required balance sheet item: {item}")
        
        # Required cash flow items
        required_cashflow = ['operating_cash_flow']
        for item in required_cashflow:
            if item not in cash_flow or cash_flow[item] is None:
                issues.append(f"Missing required cash flow item: {item}")
        
        return issues
    
    def _validate_mathematical_consistency(self, income_statement: Dict[str, float],
                                         balance_sheet: Dict[str, float],
                                         cash_flow: Dict[str, float]) -> List[str]:
        """Validate mathematical relationships between line items"""
        issues = []
        
        # Income statement relationships
        revenue = income_statement.get('revenue', 0)
        cost_of_goods_sold = income_statement.get('cost_of_goods_sold', 0)
        gross_profit = income_statement.get('gross_profit')
        
        if revenue > 0 and cost_of_goods_sold > 0 and gross_profit is not None:
            expected_gross_profit = revenue - cost_of_goods_sold
            if abs(gross_profit - expected_gross_profit) / revenue > 0.01:  # 1% tolerance
                issues.append("Gross profit calculation inconsistency")
        
        # Operating income consistency
        gross_profit_calc = revenue - cost_of_goods_sold if revenue > 0 and cost_of_goods_sold > 0 else None
        rd_expense = income_statement.get('research_and_development', 0)
        sga_expense = income_statement.get('selling_general_administrative', 0)
        operating_income = income_statement.get('operating_income')
        
        if (gross_profit_calc is not None and operating_income is not None and
            gross_profit_calc > 0):
            expected_operating = gross_profit_calc - rd_expense - sga_expense
            if abs(operating_income - expected_operating) / gross_profit_calc > 0.05:  # 5% tolerance
                issues.append("Operating income calculation may be inconsistent")
        
        return issues
    
    def _validate_balance_sheet_equation(self, balance_sheet: Dict[str, float]) -> List[str]:
        """Validate that Assets = Liabilities + Equity"""
        issues = []
        
        assets = balance_sheet.get('total_assets', 0)
        liabilities = balance_sheet.get('total_liabilities', 0)
        equity = balance_sheet.get('shareholders_equity', 0)
        
        if assets > 0 and liabilities >= 0 and equity != 0:
            # Check balance sheet equation: Assets = Liabilities + Equity
            expected_assets = liabilities + equity
            difference = abs(assets - expected_assets)
            tolerance = assets * 0.01  # 1% tolerance
            
            if difference > tolerance:
                percentage_diff = (difference / assets) * 100
                issues.append(
                    f"Balance sheet equation imbalance: {percentage_diff:.2f}% "
                    f"(Assets: {assets:,.0f}, Liabilities + Equity: {expected_assets:,.0f})"
                )
        
        return issues
    
    def _validate_reasonableness(self, income_statement: Dict[str, float],
                               balance_sheet: Dict[str, float],
                               cash_flow: Dict[str, float]) -> Tuple[List[str], List[str]]:
        """Validate reasonableness of financial values"""
        issues = []
        warnings = []
        
        # Check for negative values where they shouldn't be
        non_negative_items = {
            'revenue': income_statement.get('revenue', 0),
            'total_assets': balance_sheet.get('total_assets', 0),
            'cash_and_equivalents': balance_sheet.get('cash_and_equivalents', 0),
        }
        
        for item, value in non_negative_items.items():
            if value < 0:
                issues.append(f"Negative value for {item}: {value:,.0f}")
        
        # Check for unusually large values (potential data entry errors)
        revenue = income_statement.get('revenue', 0)
        if revenue > 0:
            # Check if other values are reasonable relative to revenue
            total_assets = balance_sheet.get('total_assets', 0)
            if total_assets > revenue * 20:  # Assets more than 20x revenue
                warnings.append(f"Unusually high asset-to-revenue ratio: {total_assets/revenue:.1f}x")
            
            net_income = income_statement.get('net_income', 0)
            if abs(net_income) > revenue * 2:  # Net income more than 2x revenue
                warnings.append(f"Unusually high net income relative to revenue")
        
        # Check cash flow reasonableness
        operating_cf = cash_flow.get('operating_cash_flow', 0)
        net_income = income_statement.get('net_income', 0)
        if operating_cf != 0 and net_income != 0:
            cf_to_ni_ratio = operating_cf / net_income
            if cf_to_ni_ratio < 0.3 or cf_to_ni_ratio > 3.0:
                warnings.append(
                    f"Operating cash flow to net income ratio seems unusual: {cf_to_ni_ratio:.2f}"
                )
        
        return issues, warnings
    
    def _calculate_financial_ratios(self, income_statement: Dict[str, float],
                                  balance_sheet: Dict[str, float],
                                  cash_flow: Dict[str, float]) -> FinancialRatios:
        """Calculate key financial ratios"""
        ratios = FinancialRatios()
        
        # Margin ratios
        revenue = income_statement.get('revenue', 0)
        if revenue > 0:
            cost_of_goods_sold = income_statement.get('cost_of_goods_sold', 0)
            if cost_of_goods_sold > 0:
                ratios.gross_margin = (revenue - cost_of_goods_sold) / revenue
            
            operating_income = income_statement.get('operating_income', 0)
            ratios.operating_margin = operating_income / revenue
            
            net_income = income_statement.get('net_income', 0)
            ratios.net_margin = net_income / revenue
        
        # Liquidity ratios
        current_assets = balance_sheet.get('current_assets', 0)
        current_liabilities = balance_sheet.get('current_liabilities', 0)
        if current_liabilities > 0:
            ratios.current_ratio = current_assets / current_liabilities
        
        # Leverage ratios
        total_liabilities = balance_sheet.get('total_liabilities', 0)
        shareholders_equity = balance_sheet.get('shareholders_equity', 0)
        if shareholders_equity > 0:
            ratios.debt_to_equity = total_liabilities / shareholders_equity
            
            # Return ratios
            net_income = income_statement.get('net_income', 0)
            ratios.return_on_equity = net_income / shareholders_equity
        
        total_assets = balance_sheet.get('total_assets', 0)
        if total_assets > 0:
            net_income = income_statement.get('net_income', 0)
            ratios.return_on_assets = net_income / total_assets
        
        return ratios
    
    def _validate_financial_ratios(self, ratios: FinancialRatios) -> List[str]:
        """Validate that financial ratios are within reasonable ranges"""
        warnings = []
        
        ratio_values = {
            'gross_margin': ratios.gross_margin,
            'operating_margin': ratios.operating_margin,
            'net_margin': ratios.net_margin,
            'current_ratio': ratios.current_ratio,
            'debt_to_equity': ratios.debt_to_equity,
            'return_on_equity': ratios.return_on_equity,
            'return_on_assets': ratios.return_on_assets,
        }
        
        for ratio_name, ratio_value in ratio_values.items():
            if ratio_value is not None:
                min_val, max_val = self.ratio_ranges.get(ratio_name, (None, None))
                if min_val is not None and ratio_value < min_val:
                    warnings.append(f"{ratio_name} unusually low: {ratio_value:.3f}")
                if max_val is not None and ratio_value > max_val:
                    warnings.append(f"{ratio_name} unusually high: {ratio_value:.3f}")
        
        return warnings
    
    def _calculate_completeness_score(self, income_statement: Dict[str, float],
                                    balance_sheet: Dict[str, float],
                                    cash_flow: Dict[str, float]) -> float:
        """Calculate data completeness score (0-1)"""
        
        # Define expected fields for each statement
        expected_fields = {
            'income_statement': [
                'revenue', 'cost_of_goods_sold', 'gross_profit', 
                'operating_income', 'net_income', 'earnings_per_share_diluted'
            ],
            'balance_sheet': [
                'cash_and_equivalents', 'current_assets', 'total_assets',
                'current_liabilities', 'total_liabilities', 'shareholders_equity'
            ],
            'cash_flow': [
                'operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow'
            ]
        }
        
        total_expected = 0
        total_found = 0
        
        statements = {
            'income_statement': income_statement,
            'balance_sheet': balance_sheet,
            'cash_flow': cash_flow
        }
        
        for stmt_name, expected in expected_fields.items():
            statement_data = statements[stmt_name]
            total_expected += len(expected)
            
            for field in expected:
                if field in statement_data and statement_data[field] is not None:
                    total_found += 1
        
        return total_found / total_expected if total_expected > 0 else 0
    
    def validate_time_series(self, statements_by_year: Dict[int, Dict[str, Any]]) -> ValidationResult:
        """Validate consistency across multiple years of data"""
        issues = []
        warnings = []
        
        if len(statements_by_year) < 2:
            return ValidationResult(True, 1.0, [], ["Insufficient data for time series validation"], {})
        
        sorted_years = sorted(statements_by_year.keys())
        
        # Check for reasonable year-over-year changes
        for i in range(1, len(sorted_years)):
            prev_year = sorted_years[i-1]
            curr_year = sorted_years[i]
            
            prev_data = statements_by_year[prev_year]
            curr_data = statements_by_year[curr_year]
            
            # Check revenue growth reasonableness
            prev_revenue = prev_data.get('income_statement', {}).get('revenue', 0)
            curr_revenue = curr_data.get('income_statement', {}).get('revenue', 0)
            
            if prev_revenue > 0 and curr_revenue > 0:
                growth_rate = (curr_revenue - prev_revenue) / prev_revenue
                if abs(growth_rate) > 1.0:  # More than 100% change
                    warnings.append(
                        f"Large revenue change {prev_year}-{curr_year}: {growth_rate*100:.1f}%"
                    )
        
        # Overall time series score
        score = max(0, 1.0 - len(warnings) * 0.1)
        
        return ValidationResult(
            is_valid=len(issues) == 0,
            score=score,
            issues=issues,
            warnings=warnings,
            metrics={'years_validated': len(statements_by_year)}
        )


if __name__ == "__main__":
    # Example usage
    validator = FinancialDataValidator()
    
    # Sample data for testing
    sample_income = {
        'revenue': 100000000,
        'cost_of_goods_sold': 60000000,
        'gross_profit': 40000000,
        'operating_income': 25000000,
        'net_income': 20000000,
    }
    
    sample_balance = {
        'cash_and_equivalents': 10000000,
        'current_assets': 50000000,
        'total_assets': 200000000,
        'current_liabilities': 30000000,
        'total_liabilities': 100000000,
        'shareholders_equity': 100000000,
    }
    
    sample_cashflow = {
        'operating_cash_flow': 25000000,
        'investing_cash_flow': -10000000,
        'financing_cash_flow': -5000000,
    }
    
    result = validator.validate_financial_statement(
        sample_income, sample_balance, sample_cashflow, 2024
    )
    
    print(f"Validation Result:")
    print(f"  Valid: {result.is_valid}")
    print(f"  Score: {result.score:.2f}")
    print(f"  Issues: {len(result.issues)}")
    print(f"  Warnings: {len(result.warnings)}")
    
    if result.issues:
        print(f"  Issues: {result.issues}")
    if result.warnings:
        print(f"  Warnings: {result.warnings[:3]}...")  # Show first 3