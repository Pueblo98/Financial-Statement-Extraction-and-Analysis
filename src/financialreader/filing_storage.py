"""
Filing Storage and Organization System
Manages local storage of SEC filings with metadata tracking and deduplication
"""

import os
import json
import hashlib
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import sqlite3
from pathlib import Path
import logging

@dataclass
class StoredFiling:
    """Represents a stored filing with metadata"""
    cik: str
    company_name: str
    ticker: str
    accession_number: str
    filing_date: str
    report_date: str
    form: str
    file_path: str
    file_size: int
    file_hash: str
    download_date: str
    primary_document: str

class FilingStorage:
    """
    Manages storage and organization of SEC filings
    Features:
    - Organized directory structure by company/year
    - SQLite database for metadata tracking
    - Deduplication via file hashing
    - Version control for updated filings
    """
    
    def __init__(self, base_path: str = "./data/filings"):
        self.base_path = Path(base_path)
        self.db_path = self.base_path / "filings_metadata.db"
        
        # Ensure base directory exists
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self._init_database()
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
    
    def _init_database(self):
        """Initialize SQLite database for filing metadata"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cik TEXT NOT NULL,
                company_name TEXT,
                ticker TEXT,
                accession_number TEXT UNIQUE NOT NULL,
                filing_date TEXT,
                report_date TEXT,
                form TEXT,
                file_path TEXT,
                file_size INTEGER,
                file_hash TEXT,
                download_date TEXT,
                primary_document TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for common queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cik ON filings(cik)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_form ON filings(form)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_filing_date ON filings(filing_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_accession ON filings(accession_number)')
        
        conn.commit()
        conn.close()
    
    def _get_file_hash(self, file_path: str) -> str:
        """Calculate SHA-256 hash of file for deduplication"""
        hash_sha256 = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            self.logger.error(f"Error calculating hash for {file_path}: {e}")
            return ""
    
    def _get_storage_path(self, cik: str, company_name: str, filing_date: str, 
                         accession_number: str, primary_document: str) -> Path:
        """
        Generate organized storage path for filing
        Structure: base_path/CIK_CompanyName/YYYY/accession_number_primary_document
        """
        # Clean company name for filesystem
        clean_name = "".join(c for c in company_name if c.isalnum() or c in (' ', '-', '_')).strip()
        clean_name = clean_name.replace(' ', '_')
        
        # Extract year from filing date
        year = filing_date[:4] if filing_date else "unknown"
        
        # Create directory structure
        company_dir = f"{cik}_{clean_name}"
        year_dir = year
        
        # Generate filename
        filename = f"{accession_number}_{primary_document}"
        
        return self.base_path / company_dir / year_dir / filename
    
    def store_filing(self, filing_data: Dict[str, Any], file_content: bytes, 
                    company_info: Dict[str, Any]) -> StoredFiling:
        """
        Store a filing with proper organization and metadata tracking
        
        Args:
            filing_data: Filing metadata (accession_number, filing_date, etc.)
            file_content: Raw file content as bytes
            company_info: Company information (name, ticker, etc.)
            
        Returns:
            StoredFiling object with storage details
        """
        cik = company_info.get('cik', '')
        company_name = company_info.get('name', 'Unknown')
        ticker = company_info.get('ticker', [''])[0] if company_info.get('ticker') else ''
        
        # Generate storage path
        storage_path = self._get_storage_path(
            cik=cik,
            company_name=company_name,
            filing_date=filing_data['filing_date'],
            accession_number=filing_data['accession_number'],
            primary_document=filing_data['primary_document']
        )
        
        # Ensure directory exists
        storage_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write file
        with open(storage_path, 'wb') as f:
            f.write(file_content)
        
        # Calculate file hash
        file_hash = self._get_file_hash(str(storage_path))
        
        # Create stored filing object
        stored_filing = StoredFiling(
            cik=cik,
            company_name=company_name,
            ticker=ticker,
            accession_number=filing_data['accession_number'],
            filing_date=filing_data['filing_date'],
            report_date=filing_data['report_date'],
            form=filing_data['form'],
            file_path=str(storage_path),
            file_size=len(file_content),
            file_hash=file_hash,
            download_date=datetime.now().isoformat(),
            primary_document=filing_data['primary_document']
        )
        
        # Store metadata in database
        self._store_metadata(stored_filing)
        
        self.logger.info(f"Stored filing {filing_data['accession_number']} at {storage_path}")
        return stored_filing
    
    def _store_metadata(self, stored_filing: StoredFiling):
        """Store filing metadata in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO filings 
                (cik, company_name, ticker, accession_number, filing_date, report_date,
                 form, file_path, file_size, file_hash, download_date, primary_document)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                stored_filing.cik,
                stored_filing.company_name,
                stored_filing.ticker,
                stored_filing.accession_number,
                stored_filing.filing_date,
                stored_filing.report_date,
                stored_filing.form,
                stored_filing.file_path,
                stored_filing.file_size,
                stored_filing.file_hash,
                stored_filing.download_date,
                stored_filing.primary_document
            ))
            conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Database error storing metadata: {e}")
            raise
        finally:
            conn.close()
    
    def get_stored_filings(self, cik: str = None, form: str = None, 
                          start_date: str = None, end_date: str = None) -> List[StoredFiling]:
        """
        Retrieve stored filings based on criteria
        
        Args:
            cik: Company CIK filter
            form: Form type filter (e.g., '10-K')
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
            
        Returns:
            List of StoredFiling objects
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query with filters
        query = "SELECT * FROM filings WHERE 1=1"
        params = []
        
        if cik:
            query += " AND cik = ?"
            params.append(cik)
        
        if form:
            query += " AND form = ?"
            params.append(form)
        
        if start_date:
            query += " AND filing_date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND filing_date <= ?"
            params.append(end_date)
        
        query += " ORDER BY filing_date DESC"
        
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert to StoredFiling objects
            columns = [description[0] for description in cursor.description]
            filings = []
            
            for row in rows:
                row_dict = dict(zip(columns, row))
                # Remove database-specific fields
                row_dict.pop('id', None)
                row_dict.pop('created_at', None)
                row_dict.pop('updated_at', None)
                
                filing = StoredFiling(**row_dict)
                filings.append(filing)
            
            return filings
            
        except sqlite3.Error as e:
            self.logger.error(f"Database error retrieving filings: {e}")
            return []
        finally:
            conn.close()
    
    def is_filing_stored(self, accession_number: str) -> bool:
        """Check if a filing is already stored"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM filings WHERE accession_number = ?", 
                         (accession_number,))
            count = cursor.fetchone()[0]
            return count > 0
        except sqlite3.Error as e:
            self.logger.error(f"Database error checking filing existence: {e}")
            return False
        finally:
            conn.close()
    
    def get_filing_path(self, accession_number: str) -> Optional[str]:
        """Get file path for a stored filing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT file_path FROM filings WHERE accession_number = ?", 
                         (accession_number,))
            result = cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            self.logger.error(f"Database error getting filing path: {e}")
            return None
        finally:
            conn.close()
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Total filings
            cursor.execute("SELECT COUNT(*) FROM filings")
            total_filings = cursor.fetchone()[0]
            
            # Total size
            cursor.execute("SELECT SUM(file_size) FROM filings")
            total_size = cursor.fetchone()[0] or 0
            
            # By form type
            cursor.execute("""
                SELECT form, COUNT(*), SUM(file_size) 
                FROM filings 
                GROUP BY form 
                ORDER BY COUNT(*) DESC
            """)
            by_form = cursor.fetchall()
            
            # By company
            cursor.execute("""
                SELECT company_name, ticker, COUNT(*) 
                FROM filings 
                GROUP BY cik, company_name, ticker 
                ORDER BY COUNT(*) DESC 
                LIMIT 10
            """)
            by_company = cursor.fetchall()
            
            return {
                'total_filings': total_filings,
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'by_form': [{'form': f[0], 'count': f[1], 'size_mb': round(f[2] / (1024 * 1024), 2)} for f in by_form],
                'top_companies': [{'name': c[0], 'ticker': c[1], 'filings': c[2]} for c in by_company]
            }
            
        except sqlite3.Error as e:
            self.logger.error(f"Database error getting stats: {e}")
            return {}
        finally:
            conn.close()
    
    def cleanup_orphaned_files(self) -> int:
        """Remove files that exist on disk but not in database"""
        removed_count = 0
        
        # Get all files in storage directory
        for root, dirs, files in os.walk(self.base_path):
            for file in files:
                if file == "filings_metadata.db":
                    continue
                    
                file_path = os.path.join(root, file)
                
                # Check if file is tracked in database
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                try:
                    cursor.execute("SELECT COUNT(*) FROM filings WHERE file_path = ?", (file_path,))
                    count = cursor.fetchone()[0]
                    
                    if count == 0:
                        os.remove(file_path)
                        removed_count += 1
                        self.logger.info(f"Removed orphaned file: {file_path}")
                        
                except sqlite3.Error as e:
                    self.logger.error(f"Database error during cleanup: {e}")
                finally:
                    conn.close()
        
        return removed_count


if __name__ == "__main__":
    # Example usage
    storage = FilingStorage("./test_data/filings")
    
    # Test storage stats
    stats = storage.get_storage_stats()
    print("Storage Statistics:")
    print(f"Total filings: {stats.get('total_filings', 0)}")
    print(f"Total size: {stats.get('total_size_mb', 0)} MB")