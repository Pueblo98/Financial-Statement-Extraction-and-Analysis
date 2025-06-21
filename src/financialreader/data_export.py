"""
Data Export and API Interface
Provides multiple export formats and simple REST API for dataset access
"""

import os
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import logging

try:
    from flask import Flask, jsonify, request, send_file
    from flask_cors import CORS
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    Flask = None

from .feature_engineering import FeatureEngineeredDataset
from .data_quality import DataQualityReport

@dataclass
class ExportMetadata:
    """Metadata for exported datasets"""
    export_format: str
    export_timestamp: str
    dataset_shape: tuple
    feature_count: int
    data_quality_score: float
    version: str
    source_files: List[str]

@dataclass
class DatasetVersion:
    """Dataset version tracking"""
    version_id: str
    created_at: str
    description: str
    feature_count: int
    quality_score: float
    file_path: str

class DataExportManager:
    """
    Manages data export in multiple formats and provides dataset versioning
    Supports CSV, JSON, Parquet formats with comprehensive metadata
    """
    
    def __init__(self, export_directory: str = "exports"):
        self.logger = logging.getLogger(__name__)
        self.export_dir = Path(export_directory)
        self.export_dir.mkdir(exist_ok=True)
        
        # Version tracking
        self.versions_file = self.export_dir / "dataset_versions.json"
        self.versions = self._load_versions()
    
    def export_dataset(self, 
                      dataset: FeatureEngineeredDataset,
                      format: str = "csv",
                      filename: Optional[str] = None,
                      version_description: str = "Feature engineered dataset") -> Dict[str, Any]:
        """
        Export feature-engineered dataset with metadata
        
        Args:
            dataset: FeatureEngineeredDataset to export
            format: Export format ('csv', 'json', 'parquet', 'excel')
            filename: Optional custom filename
            version_description: Description for version tracking
            
        Returns:
            Export metadata and file paths
        """
        self.logger.info(f"Exporting dataset in {format} format")
        
        # Generate filename if not provided
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"financial_dataset_{timestamp}"
        
        # Export main dataset
        main_file_path = self._export_main_dataset(dataset.data, format, filename)
        
        # Export metadata
        metadata_path = self._export_metadata(dataset, format, filename, main_file_path)
        
        # Export quality report
        quality_report_path = self._export_quality_report(dataset.data_quality_report, filename)
        
        # Create version entry
        version_id = self._create_version_entry(
            dataset, main_file_path, version_description
        )
        
        export_summary = {
            'version_id': version_id,
            'main_file': str(main_file_path),
            'metadata_file': str(metadata_path),
            'quality_report_file': str(quality_report_path),
            'format': format,
            'dataset_shape': dataset.data.shape,
            'feature_count': len(dataset.data.columns),
            'quality_score': dataset.data_quality_report.overall_score,
            'export_timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"Dataset exported successfully: {main_file_path}")
        return export_summary
    
    def _export_main_dataset(self, data: pd.DataFrame, format: str, filename: str) -> Path:
        """Export main dataset in specified format"""
        
        if format.lower() == "csv":
            file_path = self.export_dir / f"{filename}.csv"
            data.to_csv(file_path, index=False)
            
        elif format.lower() == "json":
            file_path = self.export_dir / f"{filename}.json"
            # Convert to records format for JSON
            data_dict = data.to_dict('records')
            with open(file_path, 'w') as f:
                json.dump(data_dict, f, indent=2, default=self._json_serializer)
                
        elif format.lower() == "parquet":
            file_path = self.export_dir / f"{filename}.parquet"
            data.to_parquet(file_path, index=False)
            
        elif format.lower() == "excel":
            file_path = self.export_dir / f"{filename}.xlsx"
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                data.to_excel(writer, sheet_name='Financial_Data', index=False)
                
                # Add summary sheet
                summary_df = pd.DataFrame({
                    'Metric': ['Total Records', 'Total Features', 'Companies', 'Years Covered'],
                    'Value': [
                        len(data),
                        len(data.columns),
                        data['company_cik'].nunique() if 'company_cik' in data.columns else 1,
                        data['fiscal_year'].nunique() if 'fiscal_year' in data.columns else 1
                    ]
                })
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
        else:
            raise ValueError(f"Unsupported export format: {format}")
        
        return file_path
    
    def _export_metadata(self, dataset: FeatureEngineeredDataset, format: str, 
                        filename: str, main_file_path: Path) -> Path:
        """Export comprehensive dataset metadata"""
        
        metadata = {
            'export_info': {
                'format': format,
                'export_timestamp': datetime.now().isoformat(),
                'main_file': str(main_file_path),
                'dataset_shape': dataset.data.shape,
                'feature_count': len(dataset.data.columns)
            },
            'feature_metadata': dataset.feature_metadata,
            'generation_summary': dataset.generation_summary,
            'data_quality_summary': {
                'overall_score': dataset.data_quality_report.overall_score,
                'quality_metrics_count': len(dataset.data_quality_report.quality_metrics),
                'recommendations_count': len(dataset.data_quality_report.recommendations)
            },
            'column_info': {
                'numeric_columns': list(dataset.data.select_dtypes(include=[np.number]).columns),
                'categorical_columns': list(dataset.data.select_dtypes(include=['object']).columns),
                'datetime_columns': list(dataset.data.select_dtypes(include=['datetime']).columns),
                'column_dtypes': dataset.data.dtypes.astype(str).to_dict(),
                'null_counts': dataset.data.isnull().sum().to_dict()
            },
            'statistical_summary': dataset.data.describe().to_dict() if len(dataset.data.select_dtypes(include=[np.number]).columns) > 0 else {}
        }
        
        metadata_path = self.export_dir / f"{filename}_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=self._json_serializer)
        
        return metadata_path
    
    def _export_quality_report(self, quality_report: DataQualityReport, filename: str) -> Path:
        """Export detailed data quality report"""
        
        # Convert quality report to serializable format
        report_dict = {
            'overall_score': quality_report.overall_score,
            'generated_at': quality_report.generated_at,
            'quality_metrics': [
                {
                    'name': metric.name,
                    'value': metric.value,
                    'threshold': metric.threshold,
                    'status': metric.status,
                    'description': metric.description,
                    'recommendations': metric.recommendations
                }
                for metric in quality_report.quality_metrics
            ],
            'completeness_analysis': quality_report.completeness_analysis,
            'consistency_analysis': quality_report.consistency_analysis,
            'outlier_analysis': [
                {
                    'column': outlier.column,
                    'outlier_count': outlier.outlier_count,
                    'outlier_percentage': outlier.outlier_percentage,
                    'method': outlier.method,
                    'threshold_used': outlier.threshold_used
                }
                for outlier in quality_report.outlier_analysis
            ],
            'temporal_analysis': quality_report.temporal_analysis,
            'recommendations': quality_report.recommendations
        }
        
        quality_path = self.export_dir / f"{filename}_quality_report.json"
        with open(quality_path, 'w') as f:
            json.dump(report_dict, f, indent=2, default=self._json_serializer)
        
        return quality_path
    
    def _create_version_entry(self, dataset: FeatureEngineeredDataset, 
                            file_path: Path, description: str) -> str:
        """Create new dataset version entry"""
        
        version_id = f"v{len(self.versions) + 1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        version_entry = DatasetVersion(
            version_id=version_id,
            created_at=datetime.now().isoformat(),
            description=description,
            feature_count=len(dataset.data.columns),
            quality_score=dataset.data_quality_report.overall_score,
            file_path=str(file_path)
        )
        
        self.versions.append(version_entry)
        self._save_versions()
        
        return version_id
    
    def get_dataset_versions(self) -> List[DatasetVersion]:
        """Get all dataset versions"""
        return self.versions
    
    def get_latest_version(self) -> Optional[DatasetVersion]:
        """Get the most recent dataset version"""
        return self.versions[-1] if self.versions else None
    
    def load_dataset_version(self, version_id: str) -> Optional[pd.DataFrame]:
        """Load specific dataset version"""
        
        version = next((v for v in self.versions if v.version_id == version_id), None)
        if not version:
            return None
        
        file_path = Path(version.file_path)
        if not file_path.exists():
            self.logger.warning(f"Dataset file not found: {file_path}")
            return None
        
        # Determine format from file extension
        if file_path.suffix == '.csv':
            return pd.read_csv(file_path)
        elif file_path.suffix == '.json':
            return pd.read_json(file_path)
        elif file_path.suffix == '.parquet':
            return pd.read_parquet(file_path)
        elif file_path.suffix == '.xlsx':
            return pd.read_excel(file_path, sheet_name='Financial_Data')
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    def _load_versions(self) -> List[DatasetVersion]:
        """Load version history from file"""
        
        if not self.versions_file.exists():
            return []
        
        try:
            with open(self.versions_file, 'r') as f:
                versions_data = json.load(f)
            return [DatasetVersion(**v) for v in versions_data]
        except Exception as e:
            self.logger.error(f"Failed to load version history: {e}")
            return []
    
    def _save_versions(self):
        """Save version history to file"""
        
        try:
            versions_data = [asdict(v) for v in self.versions]
            with open(self.versions_file, 'w') as f:
                json.dump(versions_data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save version history: {e}")
    
    def _json_serializer(self, obj):
        """JSON serializer for numpy types"""
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class FinancialDataAPI:
    """
    Simple REST API for accessing financial datasets
    Provides endpoints for dataset access, quality reports, and version management
    """
    
    def __init__(self, export_manager: DataExportManager, host: str = "127.0.0.1", port: int = 5000):
        if not FLASK_AVAILABLE:
            raise ImportError("Flask not available. Install with: conda install flask flask-cors")
        
        self.export_manager = export_manager
        self.host = host
        self.port = port
        self.logger = logging.getLogger(__name__)
        
        # Initialize Flask app
        self.app = Flask(__name__)
        CORS(self.app)  # Enable CORS for web access
        
        # Register routes
        self._register_routes()
    
    def _register_routes(self):
        """Register API endpoints"""
        
        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'version': '1.0.0'
            })
        
        @self.app.route('/api/datasets', methods=['GET'])
        def list_datasets():
            """List all available dataset versions"""
            versions = self.export_manager.get_dataset_versions()
            return jsonify({
                'versions': [asdict(v) for v in versions],
                'total_count': len(versions)
            })
        
        @self.app.route('/api/datasets/latest', methods=['GET'])
        def get_latest_dataset():
            """Get latest dataset version info"""
            latest = self.export_manager.get_latest_version()
            if not latest:
                return jsonify({'error': 'No datasets available'}), 404
            
            return jsonify(asdict(latest))
        
        @self.app.route('/api/datasets/<version_id>', methods=['GET'])
        def get_dataset(version_id):
            """Get specific dataset version"""
            dataset = self.export_manager.load_dataset_version(version_id)
            if dataset is None:
                return jsonify({'error': f'Dataset version {version_id} not found'}), 404
            
            # Return dataset info and sample data
            return jsonify({
                'version_id': version_id,
                'shape': dataset.shape,
                'columns': list(dataset.columns),
                'sample_data': dataset.head(5).to_dict('records'),
                'summary_statistics': dataset.describe().to_dict() if len(dataset.select_dtypes(include=[np.number]).columns) > 0 else {}
            })
        
        @self.app.route('/api/datasets/<version_id>/download', methods=['GET'])
        def download_dataset(version_id):
            """Download dataset file"""
            version = next((v for v in self.export_manager.get_dataset_versions() if v.version_id == version_id), None)
            if not version:
                return jsonify({'error': f'Dataset version {version_id} not found'}), 404
            
            file_path = Path(version.file_path)
            if not file_path.exists():
                return jsonify({'error': 'Dataset file not found'}), 404
            
            return send_file(file_path, as_attachment=True)
        
        @self.app.route('/api/datasets/<version_id>/quality', methods=['GET'])
        def get_quality_report(version_id):
            """Get data quality report for dataset version"""
            # Look for quality report file
            quality_file = self.export_manager.export_dir / f"{version_id.replace('v1_', '')}_quality_report.json"
            
            if not quality_file.exists():
                return jsonify({'error': 'Quality report not found'}), 404
            
            try:
                with open(quality_file, 'r') as f:
                    quality_report = json.load(f)
                return jsonify(quality_report)
            except Exception as e:
                return jsonify({'error': f'Failed to load quality report: {e}'}), 500
        
        @self.app.route('/api/search', methods=['GET'])
        def search_datasets():
            """Search datasets by criteria"""
            min_quality = request.args.get('min_quality', type=float)
            min_features = request.args.get('min_features', type=int)
            
            versions = self.export_manager.get_dataset_versions()
            filtered_versions = versions
            
            if min_quality is not None:
                filtered_versions = [v for v in filtered_versions if v.quality_score >= min_quality]
            
            if min_features is not None:
                filtered_versions = [v for v in filtered_versions if v.feature_count >= min_features]
            
            return jsonify({
                'matching_versions': [asdict(v) for v in filtered_versions],
                'total_matches': len(filtered_versions)
            })
    
    def run(self, debug: bool = False):
        """Start the API server"""
        self.logger.info(f"Starting Financial Data API on {self.host}:{self.port}")
        self.app.run(host=self.host, port=self.port, debug=debug)


if __name__ == "__main__":
    # Example usage and testing
    print("Testing Data Export Manager...")
    
    # Create sample feature-engineered dataset
    sample_data = pd.DataFrame({
        'company_cik': ['0000320193'] * 3,
        'company_name': ['Apple Inc.'] * 3,
        'fiscal_year': [2022, 2023, 2024],
        'revenue': [394328000000, 383285000000, 391035000000],
        'revenue_volatility_3y': [15234567890, 12345678901, 13456789012],
        'growth_quality_score': [1.25, 1.18, 1.22],
        'margin_momentum': [0.05, -0.02, 0.08]
    })
    
    # Mock feature metadata
    feature_metadata = {
        'generated_features': {
            'revenue_volatility_3y': {'type': 'transformation', 'description': '3-year revenue volatility'},
            'growth_quality_score': {'type': 'interaction', 'description': 'Growth quality composite'}
        }
    }
    
    # Mock quality report
    from data_quality import DataQualityReport, QualityMetric
    
    mock_quality_report = DataQualityReport(
        overall_score=0.85,
        quality_metrics=[
            QualityMetric("completeness", 0.90, 0.80, "pass", "Data completeness", [])
        ],
        completeness_analysis={'overall_completeness': 0.90},
        consistency_analysis={'overall_consistency': 0.95},
        outlier_analysis=[],
        temporal_analysis={'has_temporal_data': True},
        recommendations=["Maintain current data quality"],
        generated_at=datetime.now().isoformat()
    )
    
    # Create mock dataset
    from feature_engineering import FeatureEngineeredDataset
    
    mock_dataset = FeatureEngineeredDataset(
        data=sample_data,
        feature_metadata=feature_metadata,
        generation_summary={'new_features_added': 2},
        data_quality_report=mock_quality_report
    )
    
    # Test export manager
    export_manager = DataExportManager()
    
    # Test CSV export
    csv_export = export_manager.export_dataset(mock_dataset, format="csv", version_description="Test CSV export")
    print(f"CSV Export: {csv_export['main_file']}")
    
    # Test JSON export
    json_export = export_manager.export_dataset(mock_dataset, format="json", version_description="Test JSON export")
    print(f"JSON Export: {json_export['main_file']}")
    
    # List versions
    versions = export_manager.get_dataset_versions()
    print(f"Total versions: {len(versions)}")
    
    # Test API (if Flask available)
    if FLASK_AVAILABLE:
        print("Flask available - API can be started with api.run()")
        api = FinancialDataAPI(export_manager)
        print(f"API endpoints available at http://127.0.0.1:5000/api/")
    else:
        print("Flask not available - install with: conda install flask flask-cors")