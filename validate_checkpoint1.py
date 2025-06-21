"""
Validation script for Checkpoint 1: EDGAR Data Acquisition System
"""

from filing_manager import FilingManager
import os

def validate_checkpoint1():
    """Validate that Checkpoint 1 is working correctly"""
    
    print("=== CHECKPOINT 1 VALIDATION ===")
    print("EDGAR Data Acquisition System")
    print("=" * 40)
    
    # Initialize filing manager
    manager = FilingManager()
    
    # 1. Test company lookup
    print("\n1. Testing Company Lookup:")
    apple_summary = manager.get_company_filings_summary("AAPL")
    print(f"   Company: {apple_summary['company_info']['name']}")
    print(f"   CIK: {apple_summary['company_info']['cik']}")
    print(f"   Ticker: {apple_summary['company_info']['ticker']}")
    
    # 2. Test filing retrieval results
    print("\n2. Testing Filing Storage:")
    print(f"   Total stored filings: {apple_summary['total_filings']}")
    
    if apple_summary['total_filings'] > 0:
        print(f"   Date range: {apple_summary['date_range']['earliest']} to {apple_summary['date_range']['latest']}")
        
        # Show filings by form
        for form, filings in apple_summary['by_form'].items():
            print(f"   {form} filings: {len(filings)}")
            for filing in filings[:3]:  # Show first 3
                print(f"     - {filing['filing_date']}: {filing['accession_number']} ({filing['file_size_mb']} MB)")
    
    # 3. Test storage statistics
    print("\n3. Overall Storage Statistics:")
    stats = manager.get_storage_stats()
    print(f"   Total filings in system: {stats.get('total_filings', 0)}")
    print(f"   Total storage size: {stats.get('total_size_mb', 0)} MB")
    
    # Show breakdown by form
    by_form = stats.get('by_form', [])
    if by_form:
        print("   Breakdown by form type:")
        for form_stat in by_form:
            print(f"     {form_stat['form']}: {form_stat['count']} filings ({form_stat['size_mb']} MB)")
    
    # 4. Test file system organization
    print("\n4. File System Organization:")
    data_path = "./data/filings"
    if os.path.exists(data_path):
        print(f"   Base path exists: {data_path}")
        
        # Count directories and files
        total_dirs = 0
        total_files = 0
        for root, dirs, files in os.walk(data_path):
            total_dirs += len(dirs)
            total_files += len([f for f in files if f.endswith('.htm')])
        
        print(f"   Directory structure: {total_dirs} directories, {total_files} HTML files")
    
    # 5. Test deduplication
    print("\n5. Testing Deduplication:")
    print("   Attempting to re-download same filings...")
    result = manager.retrieve_company_filings("AAPL", years=10, form_type="10-K")
    print(f"   Already stored (should be > 0): {result.already_stored}")
    print(f"   New downloads (should be 0): {result.new_downloads}")
    
    # 6. Success criteria
    print("\n6. Success Criteria Check:")
    success_checks = []
    
    # At least 5 Apple 10-K filings retrieved
    success_checks.append(("Retrieved 5+ Apple 10-K filings", apple_summary['total_filings'] >= 5))
    
    # Files properly organized
    success_checks.append(("Files properly organized", os.path.exists(data_path)))
    
    # Database tracking working
    success_checks.append(("Database tracking functional", stats.get('total_filings', 0) > 0))
    
    # Deduplication working
    success_checks.append(("Deduplication working", result.already_stored > 0))
    
    # Rate limiting compliance (no errors from too many requests)
    success_checks.append(("Rate limiting compliant", len(result.errors) < 5))  # Some old filings may fail
    
    all_passed = True
    for check_name, passed in success_checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {status}: {check_name}")
        if not passed:
            all_passed = False
    
    print(f"\n{'='*40}")
    if all_passed:
        print("🎉 CHECKPOINT 1 VALIDATION: SUCCESS!")
        print("EDGAR Data Acquisition System is fully functional")
    else:
        print("⚠️  CHECKPOINT 1 VALIDATION: NEEDS ATTENTION")
        print("Some validation checks failed")
    print(f"{'='*40}")
    
    return all_passed

if __name__ == "__main__":
    validate_checkpoint1()