"""
US GAAP Taxonomy Mapping for Financial Data Extraction
Maps standard financial metrics to US-GAAP XBRL tags
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

class StatementType(Enum):
    """Financial statement types"""
    INCOME_STATEMENT = "income_statement"
    BALANCE_SHEET = "balance_sheet"
    CASH_FLOW = "cash_flow"
    OTHER = "other"

@dataclass
class GAAPTag:
    """Represents a US-GAAP XBRL tag with metadata"""
    tag: str
    statement_type: StatementType
    description: str
    unit_type: str = "USD"  # USD, USD/shares, shares, etc.
    alternative_tags: List[str] = None
    required: bool = True
    
    def __post_init__(self):
        if self.alternative_tags is None:
            self.alternative_tags = []

class GAAPTaxonomyMapper:
    """
    Maps financial concepts to US-GAAP XBRL tags
    Handles tag variations and provides fallback options
    """
    
    def __init__(self):
        """Initialize the GAAP taxonomy mapping"""
        self._tag_mapping = self._build_tag_mapping()
        self._reverse_mapping = self._build_reverse_mapping()
    
    def _build_tag_mapping(self) -> Dict[str, GAAPTag]:
        """Build comprehensive mapping of financial concepts to GAAP tags"""
        
        # Income Statement Tags
        income_statement_tags = {
            # Revenue
            "revenue": GAAPTag(
                tag="RevenueFromContractWithCustomerExcludingAssessedTax",
                statement_type=StatementType.INCOME_STATEMENT,
                description="Total revenue/net sales",
                alternative_tags=["Revenues", "SalesRevenueNet"]
            ),
            
            # Cost of Goods/Services Sold
            "cost_of_goods_sold": GAAPTag(
                tag="CostOfGoodsAndServicesSold",
                statement_type=StatementType.INCOME_STATEMENT,
                description="Direct costs of producing goods/services",
                alternative_tags=["CostOfRevenue", "CostOfGoodsSold"]
            ),
            
            # Gross Profit (usually calculated)
            "gross_profit": GAAPTag(
                tag="GrossProfit",
                statement_type=StatementType.INCOME_STATEMENT,
                description="Revenue minus cost of goods sold",
                required=False
            ),
            
            # Operating Expenses
            "research_and_development": GAAPTag(
                tag="ResearchAndDevelopmentExpense",
                statement_type=StatementType.INCOME_STATEMENT,
                description="R&D expenses",
                alternative_tags=["ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"]
            ),
            
            "selling_general_administrative": GAAPTag(
                tag="SellingGeneralAndAdministrativeExpense",
                statement_type=StatementType.INCOME_STATEMENT,
                description="SG&A expenses",
                alternative_tags=["SellingAndMarketingExpense", "GeneralAndAdministrativeExpense"]
            ),
            
            # Operating Income
            "operating_income": GAAPTag(
                tag="OperatingIncomeLoss",
                statement_type=StatementType.INCOME_STATEMENT,
                description="Income from operations"
            ),
            
            # Net Income
            "net_income": GAAPTag(
                tag="NetIncomeLoss",
                statement_type=StatementType.INCOME_STATEMENT,
                description="Net income/loss"
            ),
            
            # Earnings Per Share
            "earnings_per_share_basic": GAAPTag(
                tag="EarningsPerShareBasic",
                statement_type=StatementType.INCOME_STATEMENT,
                description="Basic earnings per share",
                unit_type="USD/shares"
            ),
            
            "earnings_per_share_diluted": GAAPTag(
                tag="EarningsPerShareDiluted",
                statement_type=StatementType.INCOME_STATEMENT,
                description="Diluted earnings per share",
                unit_type="USD/shares"
            ),
            
            # Additional Income Statement Items
            "interest_expense": GAAPTag(
                tag="InterestExpense",
                statement_type=StatementType.INCOME_STATEMENT,
                description="Interest expense",
                required=False
            ),
            
            "income_tax_expense": GAAPTag(
                tag="IncomeTaxExpenseBenefit",
                statement_type=StatementType.INCOME_STATEMENT,
                description="Income tax expense/benefit",
                required=False
            ),
        }
        
        # Balance Sheet Tags
        balance_sheet_tags = {
            # Assets
            "cash_and_equivalents": GAAPTag(
                tag="CashAndCashEquivalentsAtCarryingValue",
                statement_type=StatementType.BALANCE_SHEET,
                description="Cash and cash equivalents",
                alternative_tags=["CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]
            ),
            
            "accounts_receivable": GAAPTag(
                tag="AccountsReceivableNetCurrent",
                statement_type=StatementType.BALANCE_SHEET,
                description="Net accounts receivable",
                required=False
            ),
            
            "inventory": GAAPTag(
                tag="InventoryNet",
                statement_type=StatementType.BALANCE_SHEET,
                description="Inventory",
                alternative_tags=["Inventory"],
                required=False
            ),
            
            "current_assets": GAAPTag(
                tag="AssetsCurrent",
                statement_type=StatementType.BALANCE_SHEET,
                description="Total current assets"
            ),
            
            "property_plant_equipment": GAAPTag(
                tag="PropertyPlantAndEquipmentNet",
                statement_type=StatementType.BALANCE_SHEET,
                description="Net property, plant & equipment"
            ),
            
            "total_assets": GAAPTag(
                tag="Assets",
                statement_type=StatementType.BALANCE_SHEET,
                description="Total assets"
            ),
            
            # Liabilities
            "accounts_payable": GAAPTag(
                tag="AccountsPayableCurrent",
                statement_type=StatementType.BALANCE_SHEET,
                description="Current accounts payable",
                required=False
            ),
            
            "current_liabilities": GAAPTag(
                tag="LiabilitiesCurrent",
                statement_type=StatementType.BALANCE_SHEET,
                description="Total current liabilities"
            ),
            
            "long_term_debt": GAAPTag(
                tag="LongTermDebt",
                statement_type=StatementType.BALANCE_SHEET,
                description="Long-term debt",
                alternative_tags=["LongTermDebtNoncurrent"]
            ),
            
            "total_liabilities": GAAPTag(
                tag="Liabilities",
                statement_type=StatementType.BALANCE_SHEET,
                description="Total liabilities"
            ),
            
            # Equity
            "shareholders_equity": GAAPTag(
                tag="StockholdersEquity",
                statement_type=StatementType.BALANCE_SHEET,
                description="Total shareholders' equity",
                alternative_tags=["StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"]
            ),
            
            "retained_earnings": GAAPTag(
                tag="RetainedEarningsAccumulatedDeficit",
                statement_type=StatementType.BALANCE_SHEET,
                description="Retained earnings",
                required=False
            ),
            
            # Share information
            "common_shares_outstanding": GAAPTag(
                tag="CommonStockSharesOutstanding",
                statement_type=StatementType.BALANCE_SHEET,
                description="Common shares outstanding",
                unit_type="shares",
                required=False
            ),
        }
        
        # Cash Flow Statement Tags
        cash_flow_tags = {
            "operating_cash_flow": GAAPTag(
                tag="NetCashProvidedByUsedInOperatingActivities",
                statement_type=StatementType.CASH_FLOW,
                description="Net cash from operating activities"
            ),
            
            "investing_cash_flow": GAAPTag(
                tag="NetCashProvidedByUsedInInvestingActivities",
                statement_type=StatementType.CASH_FLOW,
                description="Net cash from investing activities"
            ),
            
            "financing_cash_flow": GAAPTag(
                tag="NetCashProvidedByUsedInFinancingActivities",
                statement_type=StatementType.CASH_FLOW,
                description="Net cash from financing activities"
            ),
            
            "capital_expenditures": GAAPTag(
                tag="PaymentsToAcquirePropertyPlantAndEquipment",
                statement_type=StatementType.CASH_FLOW,
                description="Capital expenditures (CapEx)",
                alternative_tags=["CapitalExpenditures"]
            ),
            
            "dividends_paid": GAAPTag(
                tag="PaymentsOfDividendsCommonStock",
                statement_type=StatementType.CASH_FLOW,
                description="Dividends paid",
                alternative_tags=["PaymentsOfDividends"],
                required=False
            ),
            
            "stock_repurchases": GAAPTag(
                tag="PaymentsForRepurchaseOfCommonStock",
                statement_type=StatementType.CASH_FLOW,
                description="Stock repurchases/buybacks",
                required=False
            ),
            
            "depreciation_amortization": GAAPTag(
                tag="DepreciationDepletionAndAmortization",
                statement_type=StatementType.CASH_FLOW,
                description="Depreciation and amortization",
                alternative_tags=["Depreciation", "DepreciationAndAmortization"],
                required=False
            ),
        }
        
        # Combine all mappings
        all_tags = {}
        all_tags.update(income_statement_tags)
        all_tags.update(balance_sheet_tags)
        all_tags.update(cash_flow_tags)
        
        return all_tags
    
    def _build_reverse_mapping(self) -> Dict[str, str]:
        """Build reverse mapping from GAAP tag to concept name"""
        reverse_map = {}
        for concept, gaap_tag in self._tag_mapping.items():
            reverse_map[gaap_tag.tag] = concept
            # Also map alternative tags
            for alt_tag in gaap_tag.alternative_tags:
                reverse_map[alt_tag] = concept
        return reverse_map
    
    def get_gaap_tag(self, concept: str) -> Optional[GAAPTag]:
        """Get GAAP tag for a financial concept"""
        return self._tag_mapping.get(concept.lower())
    
    def get_concept_name(self, gaap_tag: str) -> Optional[str]:
        """Get concept name for a GAAP tag"""
        return self._reverse_mapping.get(gaap_tag)
    
    def get_all_concepts(self) -> List[str]:
        """Get all available financial concepts"""
        return list(self._tag_mapping.keys())
    
    def get_concepts_by_statement(self, statement_type: StatementType) -> List[str]:
        """Get concepts for a specific financial statement"""
        return [
            concept for concept, tag in self._tag_mapping.items()
            if tag.statement_type == statement_type
        ]
    
    def get_required_concepts(self) -> List[str]:
        """Get all required financial concepts"""
        return [
            concept for concept, tag in self._tag_mapping.items()
            if tag.required
        ]
    
    def find_best_tag(self, available_tags: List[str], concept: str) -> Optional[str]:
        """
        Find the best available GAAP tag for a concept from available tags
        
        Args:
            available_tags: List of available GAAP tags in the data
            concept: Financial concept to find tag for
            
        Returns:
            Best matching GAAP tag or None
        """
        gaap_tag = self.get_gaap_tag(concept)
        if not gaap_tag:
            return None
        
        # Check primary tag first
        if gaap_tag.tag in available_tags:
            return gaap_tag.tag
        
        # Check alternative tags
        for alt_tag in gaap_tag.alternative_tags:
            if alt_tag in available_tags:
                return alt_tag
        
        return None
    
    def get_tag_info(self, tag: str) -> Dict[str, Any]:
        """Get comprehensive information about a GAAP tag"""
        concept = self.get_concept_name(tag)
        if not concept:
            return {"tag": tag, "concept": None, "found": False}
        
        gaap_tag = self.get_gaap_tag(concept)
        return {
            "tag": tag,
            "concept": concept,
            "description": gaap_tag.description,
            "statement_type": gaap_tag.statement_type.value,
            "unit_type": gaap_tag.unit_type,
            "required": gaap_tag.required,
            "found": True
        }


# Global instance for easy access
GAAP_MAPPER = GAAPTaxonomyMapper()


if __name__ == "__main__":
    # Example usage and testing
    mapper = GAAPTaxonomyMapper()
    
    print("Available Financial Concepts:")
    for i, concept in enumerate(mapper.get_all_concepts()):
        if i < 10:  # Show first 10
            tag = mapper.get_gaap_tag(concept)
            print(f"  {concept}: {tag.tag} ({tag.description})")
    
    print(f"\nTotal concepts: {len(mapper.get_all_concepts())}")
    
    # Test by statement type
    print(f"\nIncome Statement concepts: {len(mapper.get_concepts_by_statement(StatementType.INCOME_STATEMENT))}")
    print(f"Balance Sheet concepts: {len(mapper.get_concepts_by_statement(StatementType.BALANCE_SHEET))}")
    print(f"Cash Flow concepts: {len(mapper.get_concepts_by_statement(StatementType.CASH_FLOW))}")
    
    # Test tag finding
    available_tags = ["Revenues", "NetIncomeLoss", "Assets"]
    for concept in ["revenue", "net_income", "total_assets"]:
        best_tag = mapper.find_best_tag(available_tags, concept)
        print(f"\nBest tag for '{concept}': {best_tag}")