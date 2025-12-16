#!/usr/bin/env python3
"""
PHMSA Pipeline Safety Advisory Bulletins - Alternative Approach
Uses Federal Register API and known direct PDF links
(PHMSA.gov blocks scrapers with 403 Forbidden)
"""

import requests
from pathlib import Path
import time
import re
import json

OUTPUT_DIR = Path("data/raw/phmsa_advisories")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# Federal Register API - search for PHMSA pipeline advisory bulletins
FR_API_BASE = "https://www.federalregister.gov/api/v1"

# Known PHMSA advisory bulletin PDFs (direct links that work)
KNOWN_PDFS = [
    # Recent advisory bulletins with direct PDF links
    {
        "title": "Pipeline Safety Management System (ADB-2025-01)",
        "url": "https://www.govinfo.gov/content/pkg/FR-2025-03-25/pdf/2025-04960.pdf",
    },
    {
        "title": "Hard Spots In-Line Inspection (ADB-2024)",
        "url": "https://www.govinfo.gov/content/pkg/FR-2024-11-18/pdf/2024-26725.pdf",
    },
    {
        "title": "PHMSA Operations & Maintenance Enforcement Guidance Part 192",
        "url": "https://www.phmsa.dot.gov/sites/phmsa.dot.gov/files/docs/regulatory-compliance/pipeline/enforcement/5776/o-m-enforcement-guidance-part-192-7-21-2017.pdf",
    },
    {
        "title": "PHMSA Operations & Maintenance Enforcement Guidance Part 195",
        "url": "https://www.phmsa.dot.gov/sites/phmsa.dot.gov/files/docs/regulatory-compliance/pipeline/enforcement/5781/o-m-enforcement-guidance-part-195-7-21-2017.pdf",
    },
    {
        "title": "Pipeline Safety Threat Deactivation Guidance",
        "url": "https://www.phmsa.dot.gov/sites/phmsa.dot.gov/files/docs/regulatory-compliance/pipeline/gas-transmission-integrity-management/5731/threat-deactivation-guidance-6-22-17.pdf",
    },
]


def search_federal_register():
    """Search Federal Register for PHMSA advisory bulletins."""
    documents = []
    
    # Search for PHMSA pipeline safety advisory bulletins
    search_url = f"{FR_API_BASE}/documents.json"
    params = {
        'conditions[agencies][]': 'pipeline-and-hazardous-materials-safety-administration',
        'conditions[type][]': 'NOTICE',
        'conditions[term]': 'advisory bulletin pipeline',
        'per_page': 100,
        'order': 'newest',
    }
    
    print("Searching Federal Register for PHMSA advisory bulletins...")
    
    try:
        response = requests.get(search_url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        for doc in data.get('results', []):
            title = doc.get('title', '')
            # Filter for pipeline-related bulletins
            if 'pipeline' in title.lower() or 'advisory' in title.lower():
                pdf_url = doc.get('pdf_url')
                if pdf_url:
                    documents.append({
                        'title': title,
                        'url': pdf_url,
                        'date': doc.get('publication_date', ''),
                        'source': 'federal_register'
                    })
        
        print(f"Found {len(documents)} documents from Federal Register")
        
    except Exception as e:
        print(f"Error searching Federal Register: {e}")
    
    return documents


def download_pdf(url: str, output_path: Path) -> bool:
    """Download PDF file."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        
        # Check if it's a PDF
        content_type = response.headers.get('content-type', '')
        if 'pdf' not in content_type.lower() and response.content[:4] != b'%PDF':
            print(f"  Not a PDF: {content_type}")
            return False
        
        output_path.write_bytes(response.content)
        return True
        
    except requests.RequestException as e:
        print(f"  Error downloading: {e}")
        return False


def sanitize_filename(name: str, max_length: int = 100) -> str:
    """Create a safe filename."""
    safe = re.sub(r'[<>:"/\\|?*]', '_', name)
    safe = re.sub(r'\s+', '_', safe)
    safe = re.sub(r'_+', '_', safe)
    safe = safe.strip('_.')
    
    if len(safe) > max_length:
        safe = safe[:max_length]
    
    return safe


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_file = OUTPUT_DIR / "download_log.txt"
    
    print("=" * 60)
    print("PHMSA Pipeline Safety Documents (Alternative)")
    print("Using Federal Register API + known direct links")
    print("=" * 60)
    
    # Collect documents
    all_docs = []
    
    # 1. Search Federal Register
    fr_docs = search_federal_register()
    all_docs.extend(fr_docs)
    
    # 2. Add known direct PDFs
    print(f"\nAdding {len(KNOWN_PDFS)} known direct PDF links...")
    for pdf in KNOWN_PDFS:
        pdf['source'] = 'known_direct'
        all_docs.append(pdf)
    
    # Dedupe by URL
    seen = set()
    unique_docs = []
    for doc in all_docs:
        if doc['url'] not in seen:
            seen.add(doc['url'])
            unique_docs.append(doc)
    
    print(f"\nTotal unique documents: {len(unique_docs)}")
    
    # Download PDFs
    print("\nDownloading PDFs...")
    downloaded = 0
    skipped = 0
    failed = 0
    
    with open(log_file, 'w') as log:
        log.write("PHMSA Documents Download Log (Alternative Method)\n")
        log.write("=" * 60 + "\n\n")
        
        for i, doc in enumerate(unique_docs):
            # Create filename
            title = doc.get('title', doc['url'].split('/')[-1])
            filename = f"PHMSA_{sanitize_filename(title)}.pdf"
            output_path = OUTPUT_DIR / filename
            
            if output_path.exists():
                print(f"[{i+1}/{len(unique_docs)}] Already exists: {filename[:50]}...")
                skipped += 1
                continue
            
            print(f"[{i+1}/{len(unique_docs)}] Downloading: {title[:50]}...")
            
            if download_pdf(doc['url'], output_path):
                print(f"  -> Saved: {filename[:50]}")
                log.write(f"OK: {filename}\n")
                log.write(f"  URL: {doc['url']}\n")
                log.write(f"  Source: {doc.get('source', 'unknown')}\n\n")
                downloaded += 1
            else:
                log.write(f"FAILED: {title}\n")
                log.write(f"  URL: {doc['url']}\n\n")
                failed += 1
            
            time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"Documents found:    {len(unique_docs)}")
    print(f"Downloaded:         {downloaded}")
    print(f"Already existed:    {skipped}")
    print(f"Failed:             {failed}")
    print(f"\nFiles saved to: {OUTPUT_DIR.absolute()}")
    print("\nNote: For more PHMSA documents, visit:")
    print("  https://www.phmsa.dot.gov/guidance")
    print("  https://www.federalregister.gov (search PHMSA)")


if __name__ == "__main__":
    main()
