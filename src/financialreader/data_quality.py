"""
Data Quality and Completeness Engine
Validates data consistency, detects outliers, and generates quality reports
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import logging
from datetime import datetime
from scipy import stats

@dataclass
class QualityMetric:
    """Individual data quality metric"""
    name: str
    value: float
    threshold: float
    status: str  # 'pass', 'warning', 'fail'
    description: str
    recommendations: List[str]

@dataclass
class OutlierDetection:
    """Outlier detection results"""
    column: str
    outlier_count: int
    outlier_percentage: float
    outlier_indices: List[int]
    method: str
    threshold_used: float

@dataclass
class DataQualityReport:
    """Comprehensive data quality assessment"""
    overall_score: float
    quality_metrics: List[QualityMetric]
    completeness_analysis: Dict[str, Any]
    consistency_analysis: Dict[str, Any]
    outlier_analysis: List[OutlierDetection]
    temporal_analysis: Dict[str, Any]
    recommendations: List[str]
    generated_at: str

class DataQualityEngine:
    """
    Comprehensive data quality assessment and validation engine
    Checks consistency, completeness, outliers, and logical constraints
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Quality thresholds
        self.completeness_threshold = 0.80  # 80% completeness required
        self.outlier_threshold = 3.0        # 3 standard deviations
        self.consistency_threshold = 0.95   # 95% consistency required
        
        # Financial data constraints
        self.logical_constraints = {
            'revenue': {'min': 0, 'type': 'positive'},
            'total_assets': {'min': 0, 'type': 'positive'},
            'current_ratio': {'min': 0, 'max': 50, 'type': 'ratio'},
            'debt_to_equity': {'min': 0, 'max': 100, 'type': 'ratio'},
            'operating_margin': {'min': -100, 'max': 100, 'type': 'percentage'},
            'net_profit_margin': {'min': -100, 'max': 100, 'type': 'percentage'},
            'roe': {'min': -1000, 'max': 1000, 'type': 'percentage'},
            'roa': {'min': -100, 'max': 100, 'type': 'percentage'}
        }
    
    def assess_data_quality(self, data: pd.DataFrame) -> DataQualityReport:
        """
        Comprehensive data quality assessment
        
        Args:
            data: Dataset to assess
            
        Returns:
            DataQualityReport with detailed quality analysis
        """
        self.logger.info(f"Assessing data quality for dataset: {data.shape}")
        
        # Run all quality assessments
        quality_metrics = []
        
        # 1. Completeness Analysis
        completeness_analysis = self._assess_completeness(data)
        quality_metrics.append(QualityMetric(
            name="data_completeness",
            value=completeness_analysis['overall_completeness'],
            threshold=self.completeness_threshold,
            status="pass" if completeness_analysis['overall_completeness'] >= self.completeness_threshold else "warning",
            description="Overall data completeness percentage",
            recommendations=self._get_completeness_recommendations(completeness_analysis)
        ))
        
        # 2. Consistency Analysis
        consistency_analysis = self._assess_consistency(data)
        quality_metrics.append(QualityMetric(
            name="data_consistency",
            value=consistency_analysis['overall_consistency'],
            threshold=self.consistency_threshold,
            status="pass" if consistency_analysis['overall_consistency'] >= self.consistency_threshold else "warning",
            description="Cross-metric consistency validation",
            recommendations=self._get_consistency_recommendations(consistency_analysis)
        ))
        
        # 3. Outlier Detection
        outlier_analysis = self._detect_outliers(data)
        outlier_percentage = sum(o.outlier_percentage for o in outlier_analysis) / len(outlier_analysis) if outlier_analysis else 0
        quality_metrics.append(QualityMetric(
            name="outlier_percentage",
            value=outlier_percentage,
            threshold=5.0,  # 5% outlier threshold
            status="pass" if outlier_percentage <= 5.0 else "warning",
            description="Percentage of potential outliers detected",
            recommendations=self._get_outlier_recommendations(outlier_analysis)
        ))
        
        # 4. Temporal Analysis (if time series data)
        temporal_analysis = self._assess_temporal_quality(data)
        if temporal_analysis['has_temporal_data']:
            quality_metrics.append(QualityMetric(
                name="temporal_continuity",
                value=temporal_analysis['continuity_score'],
                threshold=0.80,
                status="pass" if temporal_analysis['continuity_score'] >= 0.80 else "warning",
                description="Time series data continuity",
                recommendations=self._get_temporal_recommendations(temporal_analysis)
            ))
        
        # 5. Logical Constraints
        constraints_analysis = self._validate_logical_constraints(data)
        quality_metrics.append(QualityMetric(
            name="logical_constraints",
            value=constraints_analysis['constraint_compliance'],
            threshold=0.95,
            status="pass" if constraints_analysis['constraint_compliance'] >= 0.95 else "fail",
            description="Financial metric logical constraint compliance",
            recommendations=self._get_constraint_recommendations(constraints_analysis)
        ))
        
        # Calculate overall quality score
        overall_score = self._calculate_overall_score(quality_metrics)
        
        # Generate comprehensive recommendations
        recommendations = self._generate_comprehensive_recommendations(quality_metrics)
        
        report = DataQualityReport(
            overall_score=overall_score,
            quality_metrics=quality_metrics,
            completeness_analysis=completeness_analysis,
            consistency_analysis=consistency_analysis,
            outlier_analysis=outlier_analysis,
            temporal_analysis=temporal_analysis,
            recommendations=recommendations,
            generated_at=datetime.now().isoformat()
        )
        
        self.logger.info(f"Data quality assessment complete. Overall score: {overall_score:.1%}")
        
        return report
    
    def _assess_completeness(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Assess data completeness patterns"""
        
        # Overall completeness
        overall_completeness = data.notna().mean().mean()
        
        # Per-column completeness
        column_completeness = data.notna().mean().to_dict()
        
        # Per-row completeness
        row_completeness = data.notna().mean(axis=1)
        
        # Identify columns with significant missing data
        low_completeness_columns = [
            col for col, completeness in column_completeness.items()
            if completeness < self.completeness_threshold
        ]
        
        # Missing data patterns
        missing_patterns = {}
        for col in data.columns:
            if data[col].isnull().any():
                missing_patterns[col] = {
                    'missing_count': data[col].isnull().sum(),
                    'missing_percentage': data[col].isnull().mean() * 100,
                    'consecutive_missing': self._find_consecutive_missing(data[col])
                }
        
        return {
            'overall_completeness': overall_completeness,
            'column_completeness': column_completeness,
            'low_completeness_columns': low_completeness_columns,
            'missing_patterns': missing_patterns,
            'rows_with_missing': len(data[data.isnull().any(axis=1)]),
            'completely_empty_rows': len(data[data.isnull().all(axis=1)])
        }
    
    def _assess_consistency(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Assess cross-metric consistency and logical relationships"""
        
        consistency_checks = []
        
        # Balance Sheet Equation: Assets = Liabilities + Equity
        if all(col in data.columns for col in ['total_assets', 'total_liabilities', 'shareholders_equity']):
            assets = data['total_assets']
            liabilities_equity = data['total_liabilities'] + data['shareholders_equity']
            balance_sheet_consistency = (
                (np.abs(assets - liabilities_equity) / assets <= 0.05).sum() / 
                len(data[assets.notna() & liabilities_equity.notna()])
            )
            consistency_checks.append(('balance_sheet_equation', balance_sheet_consistency))
        
        # Revenue vs Cash Flow reasonableness
        if all(col in data.columns for col in ['revenue', 'operating_cash_flow']):
            revenue = data['revenue']
            ocf = data['operating_cash_flow']
            # OCF should generally be within reasonable range of revenue
            reasonable_ocf = (
                (np.abs(ocf / revenue) <= 2.0).sum() / 
                len(data[revenue.notna() & ocf.notna() & (revenue > 0)])
            ) if len(data[revenue.notna() & ocf.notna() & (revenue > 0)]) > 0 else 1.0
            consistency_checks.append(('revenue_ocf_reasonableness', reasonable_ocf))
        
        # Margin relationships (gross >= operating >= net)
        margin_cols = ['gross_margin', 'operating_margin', 'net_profit_margin']
        available_margins = [col for col in margin_cols if col in data.columns]
        if len(available_margins) >= 2:
            margin_consistency = self._check_margin_relationships(data, available_margins)
            consistency_checks.append(('margin_relationships', margin_consistency))
        
        # ROE vs ROA relationship (ROE should generally be >= ROA for leveraged companies)
        if all(col in data.columns for col in ['roe', 'roa', 'debt_to_equity']):
            leveraged_companies = data['debt_to_equity'] > 0.1
            roe_roa_consistent = (
                (data.loc[leveraged_companies, 'roe'] >= data.loc[leveraged_companies, 'roa']).sum() /
                leveraged_companies.sum()
            ) if leveraged_companies.sum() > 0 else 1.0
            consistency_checks.append(('roe_roa_relationship', roe_roa_consistent))
        
        # Calculate overall consistency
        overall_consistency = np.mean([score for _, score in consistency_checks]) if consistency_checks else 1.0
        
        return {
            'overall_consistency': overall_consistency,
            'consistency_checks': dict(consistency_checks),
            'failed_consistency_rules': [
                rule for rule, score in consistency_checks if score < 0.95
            ]
        }
    
    def _detect_outliers(self, data: pd.DataFrame) -> List[OutlierDetection]:
        """Detect outliers using multiple methods"""
        
        outlier_results = []
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if col in ['company_cik', 'fiscal_year']:  # Skip ID columns
                continue
                
            series = data[col].dropna()
            if len(series) < 3:  # Need at least 3 values
                continue
            
            # Z-score method
            z_scores = np.abs(stats.zscore(series))
            z_outliers = np.where(z_scores > self.outlier_threshold)[0]
            
            if len(z_outliers) > 0:
                outlier_results.append(OutlierDetection(
                    column=col,
                    outlier_count=len(z_outliers),
                    outlier_percentage=(len(z_outliers) / len(series)) * 100,
                    outlier_indices=series.iloc[z_outliers].index.tolist(),
                    method="z_score",
                    threshold_used=self.outlier_threshold
                ))
            
            # IQR method for additional validation
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            iqr_outliers = series[(series < lower_bound) | (series > upper_bound)]
            
            if len(iqr_outliers) > 0:
                outlier_results.append(OutlierDetection(
                    column=col,
                    outlier_count=len(iqr_outliers),
                    outlier_percentage=(len(iqr_outliers) / len(series)) * 100,
                    outlier_indices=iqr_outliers.index.tolist(),
                    method="iqr",
                    threshold_used=1.5
                ))
        
        return outlier_results
    
    def _assess_temporal_quality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Assess temporal data quality for time series"""
        
        if 'fiscal_year' not in data.columns:
            return {'has_temporal_data': False}
        
        # Check for temporal continuity
        if 'company_cik' in data.columns:
            continuity_scores = []
            for company in data['company_cik'].unique():
                company_data = data[data['company_cik'] == company]['fiscal_year'].sort_values()
                if len(company_data) > 1:
                    expected_years = set(range(company_data.min(), company_data.max() + 1))
                    actual_years = set(company_data)
                    continuity = len(actual_years.intersection(expected_years)) / len(expected_years)
                    continuity_scores.append(continuity)
            
            overall_continuity = np.mean(continuity_scores) if continuity_scores else 1.0
        else:
            years = data['fiscal_year'].sort_values().unique()
            if len(years) > 1:
                expected_years = set(range(years.min(), years.max() + 1))
                actual_years = set(years)
                overall_continuity = len(actual_years.intersection(expected_years)) / len(expected_years)
            else:
                overall_continuity = 1.0
        
        # Detect temporal anomalies
        temporal_anomalies = []
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if col in ['fiscal_year', 'company_cik']:
                continue
                
            if 'company_cik' in data.columns:
                for company in data['company_cik'].unique():
                    company_data = data[data['company_cik'] == company].sort_values('fiscal_year')
                    if len(company_data) > 2:
                        pct_changes = company_data[col].pct_change().abs()
                        extreme_changes = pct_changes > 2.0  # 200% change threshold
                        if extreme_changes.any():
                            temporal_anomalies.append({
                                'company': company,
                                'metric': col,
                                'extreme_changes': extreme_changes.sum()
                            })
        
        return {
            'has_temporal_data': True,
            'continuity_score': overall_continuity,
            'temporal_anomalies': temporal_anomalies,
            'years_covered': len(data['fiscal_year'].unique()),
            'companies_tracked': data['company_cik'].nunique() if 'company_cik' in data.columns else 1
        }
    
    def _validate_logical_constraints(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Validate financial metric logical constraints"""
        
        constraint_violations = {}
        total_constraints = 0
        violations = 0
        
        for column, constraints in self.logical_constraints.items():
            if column not in data.columns:
                continue
                
            col_data = data[column].dropna()
            if len(col_data) == 0:
                continue
            
            total_constraints += len(col_data)
            
            # Check minimum value constraint
            if 'min' in constraints:
                min_violations = (col_data < constraints['min']).sum()
                if min_violations > 0:
                    constraint_violations[f"{column}_min"] = min_violations
                    violations += min_violations
            
            # Check maximum value constraint
            if 'max' in constraints:
                max_violations = (col_data > constraints['max']).sum()
                if max_violations > 0:
                    constraint_violations[f"{column}_max"] = max_violations
                    violations += max_violations
            
            # Check type-specific constraints
            if constraints['type'] == 'positive':
                negative_violations = (col_data <= 0).sum()
                if negative_violations > 0:
                    constraint_violations[f"{column}_positive"] = negative_violations
                    violations += negative_violations
        
        constraint_compliance = 1.0 - (violations / total_constraints) if total_constraints > 0 else 1.0
        
        return {
            'constraint_compliance': constraint_compliance,
            'total_constraints_checked': total_constraints,
            'total_violations': violations,
            'violation_details': constraint_violations
        }
    
    # Helper methods
    
    def _find_consecutive_missing(self, series: pd.Series) -> int:
        """Find maximum consecutive missing values"""
        is_null = series.isnull()
        consecutive_groups = (is_null != is_null.shift()).cumsum()
        max_consecutive = is_null.groupby(consecutive_groups).sum().max() if is_null.any() else 0
        return max_consecutive
    
    def _check_margin_relationships(self, data: pd.DataFrame, margin_cols: List[str]) -> float:
        """Check that margin relationships are logical"""
        
        valid_relationships = 0
        total_comparisons = 0
        
        for i in range(len(margin_cols)):
            for j in range(i + 1, len(margin_cols)):
                col1, col2 = margin_cols[i], margin_cols[j]
                valid_data = data[[col1, col2]].dropna()
                
                if len(valid_data) > 0:
                    # For financial margins, gross >= operating >= net (generally)
                    if ('gross' in col1 and 'operating' in col2) or ('operating' in col1 and 'net' in col2):
                        valid_relationships += (valid_data[col1] >= valid_data[col2]).sum()
                    elif ('gross' in col2 and 'operating' in col1) or ('operating' in col2 and 'net' in col1):
                        valid_relationships += (valid_data[col2] >= valid_data[col1]).sum()
                    else:
                        # For other combinations, just check they're reasonable
                        valid_relationships += len(valid_data)
                    
                    total_comparisons += len(valid_data)
        
        return valid_relationships / total_comparisons if total_comparisons > 0 else 1.0
    
    def _calculate_overall_score(self, quality_metrics: List[QualityMetric]) -> float:
        """Calculate weighted overall quality score"""
        
        weights = {
            'data_completeness': 0.25,
            'data_consistency': 0.25,
            'outlier_percentage': 0.20,
            'temporal_continuity': 0.15,
            'logical_constraints': 0.15
        }
        
        weighted_scores = []
        for metric in quality_metrics:
            weight = weights.get(metric.name, 0.1)
            
            # Convert to 0-1 score based on status
            if metric.status == 'pass':
                score = 1.0
            elif metric.status == 'warning':
                score = 0.7
            else:  # fail
                score = 0.3
            
            weighted_scores.append(score * weight)
        
        return sum(weighted_scores)
    
    def _get_completeness_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate completeness improvement recommendations"""
        
        recommendations = []
        
        if analysis['overall_completeness'] < self.completeness_threshold:
            recommendations.append(f"Overall completeness is {analysis['overall_completeness']:.1%}, below {self.completeness_threshold:.1%} threshold")
        
        if analysis['low_completeness_columns']:
            recommendations.append(f"Focus on improving completeness for: {', '.join(analysis['low_completeness_columns'][:3])}")
        
        if analysis['completely_empty_rows'] > 0:
            recommendations.append(f"Remove {analysis['completely_empty_rows']} completely empty rows")
        
        return recommendations
    
    def _get_consistency_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate consistency improvement recommendations"""
        
        recommendations = []
        
        if analysis['failed_consistency_rules']:
            recommendations.append(f"Address consistency issues in: {', '.join(analysis['failed_consistency_rules'])}")
        
        if 'balance_sheet_equation' in analysis['failed_consistency_rules']:
            recommendations.append("Verify balance sheet equation: Assets = Liabilities + Equity")
        
        return recommendations
    
    def _get_outlier_recommendations(self, outliers: List[OutlierDetection]) -> List[str]:
        """Generate outlier handling recommendations"""
        
        recommendations = []
        
        high_outlier_columns = [o.column for o in outliers if o.outlier_percentage > 10]
        if high_outlier_columns:
            recommendations.append(f"Investigate high outlier rates in: {', '.join(high_outlier_columns[:3])}")
        
        if len(outliers) > 5:
            recommendations.append("Consider data transformation or normalization for outlier-prone metrics")
        
        return recommendations
    
    def _get_temporal_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate temporal quality recommendations"""
        
        recommendations = []
        
        if analysis['continuity_score'] < 0.8:
            recommendations.append("Address missing years in time series data")
        
        if analysis['temporal_anomalies']:
            recommendations.append("Investigate extreme year-over-year changes")
        
        return recommendations
    
    def _get_constraint_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate logical constraint recommendations"""
        
        recommendations = []
        
        if analysis['total_violations'] > 0:
            recommendations.append(f"Fix {analysis['total_violations']} logical constraint violations")
        
        for violation, count in analysis['violation_details'].items():
            recommendations.append(f"Address {count} violations in {violation}")
        
        return recommendations
    
    def _generate_comprehensive_recommendations(self, quality_metrics: List[QualityMetric]) -> List[str]:
        """Generate comprehensive improvement recommendations"""
        
        all_recommendations = []
        
        for metric in quality_metrics:
            all_recommendations.extend(metric.recommendations)
        
        # Add general recommendations
        all_recommendations.append("Implement automated data validation checks")
        all_recommendations.append("Consider data quality monitoring dashboards")
        
        return list(set(all_recommendations))  # Remove duplicates


if __name__ == "__main__":
    # Example usage and testing
    print("Testing Data Quality Engine...")
    
    # Create sample data with quality issues
    sample_data = pd.DataFrame({
        'company_cik': ['0000320193'] * 6 + ['0000789019'] * 4,
        'fiscal_year': [2019, 2020, 2021, 2022, 2023, 2024] + [2021, 2022, 2023, 2024],
        'revenue': [260174000000, 274515000000, 365817000000, 394328000000, 383285000000, 391035000000] + 
                  [53823000000, 61877000000, 73723000000, np.nan],  # Missing value
        'total_assets': [338516000000, 323888000000, 351002000000, 352755000000, 352583000000, 364980000000] +
                       [47035000000, 73896000000, 95292000000, 108989000000],
        'operating_margin': [24.57, 24.15, 29.78, 30.29, 29.87, 31.49] + [25.89, 24.26, 24.57, 26.15],
        'current_ratio': [1.54, 1.36, 1.07, 0.88, 0.98, 0.87] + [2.85, 1.73, 1.38, 1.75],
        'debt_to_equity': [1.73, 1.67, 1.95, 1.84, 1.75, 1.84] + [0.31, 0.56, 0.47, 0.51],
        'roe': [55.92, 73.69, 147.44, 196.35, 172.89, 160.59] + [27.83, 41.83, 44.64, 36.25],
        'net_profit_margin': [21.04, 20.91, 25.88, 25.31, 25.31, 23.97] + [21.05, 20.72, 20.51, np.nan]  # Missing value
    })
    
    # Add some outliers
    sample_data.loc[2, 'roe'] = 500.0  # Extreme outlier
    sample_data.loc[7, 'current_ratio'] = 50.0  # Another outlier
    
    # Test the quality engine
    engine = DataQualityEngine()
    report = engine.assess_data_quality(sample_data)
    
    print(f"\nData Quality Report:")
    print(f"Overall Score: {report.overall_score:.1%}")
    print(f"Quality Metrics:")
    for metric in report.quality_metrics:
        print(f"  {metric.name}: {metric.value:.1%} ({metric.status})")
    
    print(f"\nCompleteness: {report.completeness_analysis['overall_completeness']:.1%}")
    print(f"Consistency: {report.consistency_analysis['overall_consistency']:.1%}")
    print(f"Outliers detected: {len(report.outlier_analysis)} metrics with outliers")
    
    print(f"\nTop Recommendations:")
    for i, rec in enumerate(report.recommendations[:5], 1):
        print(f"  {i}. {rec}")