"""
Automated Feature Engineering Pipeline
Implements AI-recommended feature transformations and generates advanced financial features
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import logging
from datetime import datetime

from .feature_planning_agent import FeaturePlan, FeatureRecommendation

@dataclass
class FeatureEngineeredDataset:
    """Container for feature-engineered dataset with metadata"""
    data: pd.DataFrame
    feature_metadata: Dict[str, Any]
    generation_summary: Dict[str, Any]
    data_quality_report: Dict[str, Any]

class FeatureEngineeringPipeline:
    """
    Automated feature engineering pipeline that implements AI recommendations
    Generates advanced financial features from raw financial and performance data
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.generated_features = {}
        self.feature_metadata = {}
    
    def engineer_features(self, 
                         financial_data: pd.DataFrame,
                         performance_data: pd.DataFrame,
                         feature_plan: FeaturePlan,
                         narrative_data: Optional[pd.DataFrame] = None) -> FeatureEngineeredDataset:
        """
        Execute complete feature engineering pipeline based on AI recommendations
        
        Args:
            financial_data: Raw financial statements
            performance_data: Performance analytics metrics
            feature_plan: AI-generated feature engineering plan
            narrative_data: Optional narrative insights
            
        Returns:
            FeatureEngineeredDataset with enhanced features
        """
        self.logger.info("Starting automated feature engineering pipeline")
        
        # Start with merged base dataset
        base_dataset = self._merge_base_datasets(financial_data, performance_data, narrative_data)
        
        # Apply feature engineering in recommended priority order
        enhanced_dataset = base_dataset.copy()
        
        # 1. Transformation features (mathematical transformations)
        enhanced_dataset = self._apply_transformation_features(enhanced_dataset, feature_plan.transformation_features)
        
        # 2. Temporal features (time-series patterns)
        enhanced_dataset = self._apply_temporal_features(enhanced_dataset, feature_plan.temporal_features)
        
        # 3. Interaction features (cross-metric relationships)
        enhanced_dataset = self._apply_interaction_features(enhanced_dataset, feature_plan.interaction_features)
        
        # 4. Narrative features (text-based features)
        if narrative_data is not None:
            enhanced_dataset = self._apply_narrative_features(enhanced_dataset, feature_plan.narrative_features)
        
        # Generate feature metadata and quality report
        feature_metadata = self._generate_feature_metadata()
        generation_summary = self._generate_summary(base_dataset, enhanced_dataset)
        data_quality_report = self._generate_data_quality_report(enhanced_dataset)
        
        self.logger.info(f"Feature engineering complete: {enhanced_dataset.shape[1]} total features")
        
        return FeatureEngineeredDataset(
            data=enhanced_dataset,
            feature_metadata=feature_metadata,
            generation_summary=generation_summary,
            data_quality_report=data_quality_report
        )
    
    def _merge_base_datasets(self, financial_data: pd.DataFrame, performance_data: pd.DataFrame, 
                           narrative_data: Optional[pd.DataFrame]) -> pd.DataFrame:
        """Merge all input datasets into unified base dataset"""
        
        # Start with financial data as base
        base = financial_data.copy()
        
        # Merge performance data
        merge_keys = ['company_cik', 'fiscal_year']
        if all(key in performance_data.columns for key in merge_keys):
            base = base.merge(performance_data, on=merge_keys, how='left', suffixes=('', '_perf'))
        
        # Merge narrative data if available
        if narrative_data is not None and all(key in narrative_data.columns for key in merge_keys):
            base = base.merge(narrative_data, on=merge_keys, how='left', suffixes=('', '_narr'))
        
        # Sort by company and year for time-series operations
        if 'company_cik' in base.columns and 'fiscal_year' in base.columns:
            base = base.sort_values(['company_cik', 'fiscal_year']).reset_index(drop=True)
        
        self.logger.info(f"Base dataset created: {base.shape}")
        return base
    
    def _apply_transformation_features(self, data: pd.DataFrame, features: List[FeatureRecommendation]) -> pd.DataFrame:
        """Apply mathematical transformation features"""
        
        self.logger.info(f"Applying {len(features)} transformation features")
        
        for feature in features:
            try:
                if feature.feature_name == "revenue_volatility_3y":
                    data = self._add_revenue_volatility(data)
                elif feature.feature_name == "margin_momentum": 
                    data = self._add_margin_momentum(data)
                elif feature.feature_name == "asset_efficiency_percentile":
                    data = self._add_asset_efficiency_percentile(data)
                elif "log_" in feature.feature_name:
                    data = self._add_log_transformation(data, feature)
                elif "volatility" in feature.feature_name.lower():
                    data = self._add_volatility_feature(data, feature)
                elif "normalized" in feature.feature_name.lower():
                    data = self._add_normalized_feature(data, feature)
                
                self._track_feature(feature.feature_name, "transformation", feature.description)
                
            except Exception as e:
                self.logger.warning(f"Failed to create transformation feature {feature.feature_name}: {e}")
        
        return data
    
    def _apply_temporal_features(self, data: pd.DataFrame, features: List[FeatureRecommendation]) -> pd.DataFrame:
        """Apply time-series and temporal features"""
        
        self.logger.info(f"Applying {len(features)} temporal features")
        
        for feature in features:
            try:
                if "rolling" in feature.feature_name or "_avg" in feature.feature_name:
                    data = self._add_rolling_average(data, feature)
                elif "lagged" in feature.feature_name or "_lag" in feature.feature_name:
                    data = self._add_lagged_feature(data, feature)
                elif "momentum" in feature.feature_name.lower():
                    data = self._add_momentum_feature(data, feature)
                elif "acceleration" in feature.feature_name.lower():
                    data = self._add_acceleration_feature(data, feature)
                
                self._track_feature(feature.feature_name, "temporal", feature.description)
                
            except Exception as e:
                self.logger.warning(f"Failed to create temporal feature {feature.feature_name}: {e}")
        
        return data
    
    def _apply_interaction_features(self, data: pd.DataFrame, features: List[FeatureRecommendation]) -> pd.DataFrame:
        """Apply interaction and cross-metric features"""
        
        self.logger.info(f"Applying {len(features)} interaction features")
        
        for feature in features:
            try:
                if feature.feature_name == "growth_quality_score":
                    data = self._add_growth_quality_score(data)
                elif feature.feature_name == "leverage_profitability_ratio":
                    data = self._add_leverage_profitability_ratio(data)
                elif "ratio" in feature.feature_name.lower():
                    data = self._add_ratio_feature(data, feature)
                elif "score" in feature.feature_name.lower():
                    data = self._add_composite_score(data, feature)
                
                self._track_feature(feature.feature_name, "interaction", feature.description)
                
            except Exception as e:
                self.logger.warning(f"Failed to create interaction feature {feature.feature_name}: {e}")
        
        return data
    
    def _apply_narrative_features(self, data: pd.DataFrame, features: List[FeatureRecommendation]) -> pd.DataFrame:
        """Apply narrative and text-based features"""
        
        self.logger.info(f"Applying {len(features)} narrative features")
        
        for feature in features:
            try:
                if "sentiment" in feature.feature_name.lower():
                    data = self._add_sentiment_feature(data, feature)
                elif "risk" in feature.feature_name.lower():
                    data = self._add_risk_feature(data, feature)
                elif "theme" in feature.feature_name.lower():
                    data = self._add_theme_feature(data, feature)
                
                self._track_feature(feature.feature_name, "narrative", feature.description)
                
            except Exception as e:
                self.logger.warning(f"Failed to create narrative feature {feature.feature_name}: {e}")
        
        return data
    
    # Specific feature implementation methods
    
    def _add_revenue_volatility(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add 3-year rolling volatility of revenue"""
        if 'revenue' in data.columns and 'company_cik' in data.columns:
            data['revenue_volatility_3y'] = data.groupby('company_cik')['revenue'].rolling(
                window=3, min_periods=2
            ).std().reset_index(0, drop=True)
        return data
    
    def _add_margin_momentum(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add operating margin momentum (rate of change)"""
        if 'operating_margin' in data.columns and 'company_cik' in data.columns:
            data['margin_momentum'] = data.groupby('company_cik')['operating_margin'].pct_change()
        return data
    
    def _add_asset_efficiency_percentile(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add percentile ranking of asset turnover within each year"""
        if 'asset_turnover' in data.columns and 'fiscal_year' in data.columns:
            data['asset_efficiency_percentile'] = data.groupby('fiscal_year')['asset_turnover'].rank(pct=True)
        return data
    
    def _add_growth_quality_score(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add growth quality score (revenue growth × cash conversion)"""
        if 'revenue_growth_yoy' in data.columns and 'cash_conversion_ratio' in data.columns:
            data['growth_quality_score'] = data['revenue_growth_yoy'] * data['cash_conversion_ratio']
        return data
    
    def _add_leverage_profitability_ratio(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add interaction between leverage and profitability"""
        if 'debt_to_equity' in data.columns and 'roe' in data.columns:
            data['leverage_profitability_ratio'] = data['debt_to_equity'] * data['roe']
        return data
    
    def _add_rolling_average(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add rolling average feature based on specification"""
        
        # Extract window size from feature name (e.g., "revenue_3y_avg" -> 3)
        try:
            window = int(''.join(filter(str.isdigit, feature.feature_name)))
            if window == 0:
                window = 3  # Default
        except:
            window = 3
        
        # Identify base metric from data requirements or feature name
        base_metrics = feature.data_requirements if feature.data_requirements else ['revenue']
        
        for metric in base_metrics:
            if metric in data.columns and metric != 'fiscal_year':
                if 'company_cik' in data.columns:
                    data[feature.feature_name] = data.groupby('company_cik')[metric].rolling(
                        window=window, min_periods=2
                    ).mean().reset_index(0, drop=True)
                break
        
        return data
    
    def _add_lagged_feature(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add lagged feature (previous period values)"""
        
        # Extract lag period from feature name
        try:
            lag = int(''.join(filter(str.isdigit, feature.feature_name)))
            if lag == 0:
                lag = 1  # Default
        except:
            lag = 1
        
        # Identify base metric
        base_metrics = feature.data_requirements if feature.data_requirements else ['operating_margin']
        
        for metric in base_metrics:
            if metric in data.columns and metric != 'fiscal_year':
                if 'company_cik' in data.columns:
                    data[feature.feature_name] = data.groupby('company_cik')[metric].shift(lag)
                break
        
        return data
    
    def _add_log_transformation(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add log transformation of specified metric"""
        
        base_metrics = feature.data_requirements if feature.data_requirements else ['revenue']
        
        for metric in base_metrics:
            if metric in data.columns:
                # Only log transform positive values
                positive_values = data[metric] > 0
                data[feature.feature_name] = np.nan
                data.loc[positive_values, feature.feature_name] = np.log(data.loc[positive_values, metric])
                break
        
        return data
    
    def _add_volatility_feature(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add generic volatility feature"""
        
        base_metrics = feature.data_requirements if feature.data_requirements else ['revenue']
        window = 3  # Default window
        
        for metric in base_metrics:
            if metric in data.columns and 'company_cik' in data.columns:
                data[feature.feature_name] = data.groupby('company_cik')[metric].rolling(
                    window=window, min_periods=2
                ).std().reset_index(0, drop=True)
                break
        
        return data
    
    def _add_normalized_feature(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add normalized/standardized feature"""
        
        base_metrics = feature.data_requirements if feature.data_requirements else []
        
        for metric in base_metrics:
            if metric in data.columns:
                data[feature.feature_name] = (data[metric] - data[metric].mean()) / data[metric].std()
                break
        
        return data
    
    def _add_momentum_feature(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add momentum (rate of change) feature"""
        
        base_metrics = feature.data_requirements if feature.data_requirements else []
        
        for metric in base_metrics:
            if metric in data.columns and 'company_cik' in data.columns:
                data[feature.feature_name] = data.groupby('company_cik')[metric].pct_change()
                break
        
        return data
    
    def _add_acceleration_feature(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add acceleration (change in rate of change) feature"""
        
        base_metrics = feature.data_requirements if feature.data_requirements else []
        
        for metric in base_metrics:
            if metric in data.columns and 'company_cik' in data.columns:
                pct_change = data.groupby('company_cik')[metric].pct_change()
                data[feature.feature_name] = pct_change.groupby(data['company_cik']).diff()
                break
        
        return data
    
    def _add_ratio_feature(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add ratio between two metrics"""
        
        if len(feature.data_requirements) >= 2:
            metric1, metric2 = feature.data_requirements[0], feature.data_requirements[1]
            if metric1 in data.columns and metric2 in data.columns:
                # Avoid division by zero
                denominator_nonzero = data[metric2] != 0
                data[feature.feature_name] = np.nan
                data.loc[denominator_nonzero, feature.feature_name] = (
                    data.loc[denominator_nonzero, metric1] / data.loc[denominator_nonzero, metric2]
                )
        
        return data
    
    def _add_composite_score(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add composite score from multiple metrics"""
        
        if feature.data_requirements:
            available_metrics = [m for m in feature.data_requirements if m in data.columns]
            if available_metrics:
                # Simple average composite score
                data[feature.feature_name] = data[available_metrics].mean(axis=1)
        
        return data
    
    def _add_sentiment_feature(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add sentiment analysis feature (mock implementation)"""
        
        # Mock sentiment scoring - would use actual NLP in production
        if 'risk_factors' in data.columns or 'md_a_text' in data.columns:
            # Simple mock: random sentiment between -1 and 1
            np.random.seed(42)  # Reproducible
            data[feature.feature_name] = np.random.uniform(-1, 1, len(data))
        
        return data
    
    def _add_risk_feature(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add risk scoring feature (mock implementation)"""
        
        # Mock risk scoring - would use actual text analysis in production
        if 'risk_factors' in data.columns:
            # Simple mock: risk score based on volatility
            if 'revenue_volatility_3y' in data.columns:
                data[feature.feature_name] = data['revenue_volatility_3y'] / data['revenue_volatility_3y'].std()
        
        return data
    
    def _add_theme_feature(self, data: pd.DataFrame, feature: FeatureRecommendation) -> pd.DataFrame:
        """Add strategic theme feature (mock implementation)"""
        
        # Mock theme extraction - would use actual NLP in production
        if 'strategic_focus' in data.columns or 'business_segments' in data.columns:
            # Simple mock: theme intensity score
            np.random.seed(42)
            data[feature.feature_name] = np.random.uniform(0, 1, len(data))
        
        return data
    
    def _track_feature(self, feature_name: str, feature_type: str, description: str):
        """Track generated features for metadata"""
        self.generated_features[feature_name] = {
            'type': feature_type,
            'description': description,
            'created_at': datetime.now().isoformat()
        }
    
    def _generate_feature_metadata(self) -> Dict[str, Any]:
        """Generate comprehensive feature metadata"""
        return {
            'generated_features': self.generated_features,
            'feature_count_by_type': {
                'transformation': len([f for f in self.generated_features.values() if f['type'] == 'transformation']),
                'temporal': len([f for f in self.generated_features.values() if f['type'] == 'temporal']),
                'interaction': len([f for f in self.generated_features.values() if f['type'] == 'interaction']),
                'narrative': len([f for f in self.generated_features.values() if f['type'] == 'narrative'])
            },
            'generation_timestamp': datetime.now().isoformat()
        }
    
    def _generate_summary(self, base_data: pd.DataFrame, enhanced_data: pd.DataFrame) -> Dict[str, Any]:
        """Generate feature engineering summary"""
        
        return {
            'original_features': base_data.shape[1],
            'enhanced_features': enhanced_data.shape[1],
            'new_features_added': enhanced_data.shape[1] - base_data.shape[1],
            'feature_addition_rate': (enhanced_data.shape[1] - base_data.shape[1]) / base_data.shape[1],
            'total_records': enhanced_data.shape[0],
            'companies_processed': enhanced_data['company_cik'].nunique() if 'company_cik' in enhanced_data.columns else 1,
            'time_periods': enhanced_data['fiscal_year'].nunique() if 'fiscal_year' in enhanced_data.columns else 1
        }
    
    def _generate_data_quality_report(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Generate data quality assessment"""
        
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        
        return {
            'completeness': {
                'overall': data.notna().mean().mean(),
                'by_column': data.notna().mean().to_dict()
            },
            'feature_statistics': {
                'total_features': len(data.columns),
                'numeric_features': len(numeric_columns),
                'categorical_features': len(data.columns) - len(numeric_columns)
            },
            'outlier_detection': {
                'potential_outliers': len(data[(np.abs((data[numeric_columns] - data[numeric_columns].mean()) / data[numeric_columns].std()) > 3).any(axis=1)]) if len(numeric_columns) > 0 else 0
            },
            'missing_data_patterns': {
                'columns_with_missing': len(data.columns[data.isnull().any()]),
                'rows_with_missing': len(data[data.isnull().any(axis=1)])
            }
        }


if __name__ == "__main__":
    # Example usage and testing
    print("Testing Feature Engineering Pipeline...")
    
    # Create sample data for testing
    sample_data = pd.DataFrame({
        'company_cik': ['0000320193'] * 5,
        'company_name': ['Apple Inc.'] * 5,
        'fiscal_year': [2020, 2021, 2022, 2023, 2024],
        'revenue': [274515000000, 365817000000, 394328000000, 383285000000, 391035000000],
        'operating_margin': [24.15, 29.78, 30.29, 29.87, 31.49],
        'asset_turnover': [0.82, 1.08, 1.12, 1.09, 1.07],
        'debt_to_equity': [1.73, 1.67, 1.95, 1.75, 1.84],
        'roe': [73.69, 147.44, 196.35, 172.89, 160.59]
    })
    
    # Create mock feature plan
    from feature_planning_agent import FeaturePlan, FeatureRecommendation
    
    mock_features = [
        FeatureRecommendation(
            feature_name="revenue_volatility_3y",
            feature_type="transformation",
            description="3-year revenue volatility",
            implementation="rolling std",
            priority=4,
            complexity="low",
            expected_value="risk measure",
            data_requirements=["revenue"]
        ),
        FeatureRecommendation(
            feature_name="growth_quality_score",
            feature_type="interaction", 
            description="growth × quality",
            implementation="multiplication",
            priority=5,
            complexity="low",
            expected_value="comprehensive growth",
            data_requirements=["revenue_growth_yoy", "cash_conversion_ratio"]
        )
    ]
    
    mock_plan = FeaturePlan(
        dataset_summary={},
        transformation_features=[mock_features[0]],
        interaction_features=[mock_features[1]],
        narrative_features=[],
        temporal_features=[],
        quality_recommendations=[],
        implementation_priority=[]
    )
    
    # Test pipeline
    pipeline = FeatureEngineeringPipeline()
    result = pipeline.engineer_features(
        financial_data=sample_data,
        performance_data=sample_data,  # Using same for test
        feature_plan=mock_plan
    )
    
    print(f"Enhanced dataset shape: {result.data.shape}")
    print(f"New features added: {result.generation_summary['new_features_added']}")
    print(f"Data quality score: {result.data_quality_report['completeness']['overall']:.2%}")
    print(f"Generated features: {list(result.feature_metadata['generated_features'].keys())}")