#!/usr/bin/env python3
"""
CSB Investigation Reports Scraper - FIXED VERSION
Fixes:
1. Prefixes filenames with investigation name to avoid duplicates
2. Updated URLs for current investigations
"""

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import time
import re
from urllib.parse import urljoin
import hashlib

BASE_URL = "https://www.csb.gov"
OUTPUT_DIR = Path("data/raw/csb_reports")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# Known O&G / Refinery investigation URLs (verified Dec 2025)
KNOWN_INVESTIGATIONS = [
    {"name": "Tesoro_Anacortes", "url": "https://www.csb.gov/tesoro-refinery-fatal-explosion-and-fire/"},
    {"name": "BP_Texas_City", "url": "https://www.csb.gov/bp-america-refinery-explosion/"},
    {"name": "Chevron_Richmond", "url": "https://www.csb.gov/chevron-refinery-fire/"},
    {"name": "ExxonMobil_Torrance", "url": "https://www.csb.gov/exxonmobil-refinery-explosion/"},
    {"name": "PES_Philadelphia", "url": "https://www.csb.gov/philadelphia-energy-solutions-refinery-fire-and-explosions/"},
    {"name": "Husky_Superior", "url": "https://www.csb.gov/husky-energy-refinery-explosion-and-fire/"},
    {"name": "Deepwater_Horizon", "url": "https://www.csb.gov/deepwater-horizon-blowout-and-explosion/"},
    {"name": "Enterprise_Pascagoula", "url": "https://www.csb.gov/enterprise-products-pascagoula-gas-plant-explosion/"},
    {"name": "DuPont_LaPorte", "url": "https://www.csb.gov/dupont-la-porte-facility-toxic-chemical-release/"},
    {"name": "Macondo_Well", "url": "https://www.csb.gov/macondo-blowout-and-explosion/"},
    {"name": "TEPPCO_Galena_Park", "url": "https://www.csb.gov/teppco-partners-explosion-and-fire/"},
    {"name": "Giant_Ciniza", "url": "https://www.csb.gov/giant-industries-refinery-explosion-and-fire/"},
    {"name": "Motiva_Delaware", "url": "https://www.csb.gov/motiva-enterprises-sulfuric-acid-tank-explosion/"},
    {"name": "Formosa_Plastics", "url": "https://www.csb.gov/formosa-plastics-explosion/"},
    {"name": "Silver_Eagle_Woods_Cross", "url": "https://www.csb.gov/silver-eagle-refinery-flash-fire-and-explosion/"},
    {"name": "Praxair_St_Louis", "url": "https://www.csb.gov/praxair-st-louis-cold-box-rupture/"},
    {"name": "PEMEX_Deer_Park", "url": "https://www.csb.gov/pemex-deer-park-refinery-incident/"},
]


def get_documents_from_page(detail_url: str, investigation_name: str) -> list[dict]:
    """Get all PDF documents from an investigation page."""
    try:
        response = requests.get(detail_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching {detail_url}: {e}")
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    documents = []
    
    for link in soup.find_all('a', href=True):
        href = link['href']
        text = link.get_text(strip=True)
        
        if '.pdf' in href.lower():
            full_url = urljoin(BASE_URL, href)
            
            # Skip video links
            if 'video' in text.lower():
                continue
            
            # Determine document type
            doc_type = "document"
            text_lower = text.lower()
            if 'report' in text_lower and 'investigation' in text_lower:
                doc_type = "final_report"
            elif 'interim' in text_lower:
                doc_type = "interim_report"
            elif 'case study' in text_lower:
                doc_type = "case_study"
            elif 'factual' in text_lower:
                doc_type = "factual_update"
            elif 'recommendation' in text_lower:
                doc_type = "recommendation"
            elif 'appendix' in text_lower:
                doc_type = "appendix"
            elif 'letter' in text_lower:
                doc_type = "letter"
            elif 'transcript' in text_lower:
                doc_type = "transcript"
            
            documents.append({
                'title': text or href.split('/')[-1],
                'url': full_url,
                'type': doc_type,
                'investigation': investigation_name
            })
    
    return documents


def download_pdf(pdf_url: str, output_path: Path) -> bool:
    """Download PDF file."""
    try:
        response = requests.get(pdf_url, headers=HEADERS, timeout=120)
        response.raise_for_status()
        
        if response.content[:4] != b'%PDF':
            print(f"  Not a valid PDF")
            return False
        
        output_path.write_bytes(response.content)
        return True
        
    except requests.RequestException as e:
        print(f"  Error downloading PDF: {e}")
        return False


def sanitize_filename(name: str, max_length: int = 80) -> str:
    """Create a safe filename."""
    safe = re.sub(r'[<>:"/\\|?*]', '_', name)
    safe = re.sub(r'\s+', '_', safe)
    safe = re.sub(r'_+', '_', safe)
    safe = safe.strip('_.')
    
    if len(safe) > max_length:
        safe = safe[:max_length]
    
    return safe


def make_unique_filename(investigation: str, title: str, url: str) -> str:
    """Create a unique filename by prefixing with investigation name."""
    # Start with investigation name
    prefix = sanitize_filename(investigation)
    
    # Get original filename from URL
    original = url.split('/')[-1]
    if original.endswith('.pdf'):
        original = original[:-4]
    
    # If it's a generic name, use the title instead
    generic_names = ['recommendation_status_change_summary', 'status_change', 'recommendation']
    if any(g in original.lower() for g in generic_names):
        # Use title but keep it short
        title_part = sanitize_filename(title)[:40]
        # Add hash of URL to ensure uniqueness
        url_hash = hashlib.md5(url.encode()).hexdigest()[:6]
        return f"{prefix}_{title_part}_{url_hash}.pdf"
    
    # Otherwise use investigation prefix + original filename
    return f"{prefix}_{sanitize_filename(original)}.pdf"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_file = OUTPUT_DIR / "download_log.txt"
    
    print("=" * 60)
    print("CSB Investigation Reports Scraper (FIXED)")
    print("Fixes duplicate filename issue")
    print("=" * 60)
    
    # Process each known investigation
    print(f"\nProcessing {len(KNOWN_INVESTIGATIONS)} known O&G investigations...")
    
    all_documents = []
    
    for inv in KNOWN_INVESTIGATIONS:
        name = inv['name']
        url = inv['url']
        print(f"\n[{name}] Fetching documents...")
        
        docs = get_documents_from_page(url, name)
        
        if docs:
            print(f"  Found {len(docs)} documents")
            all_documents.extend(docs)
        else:
            print(f"  No documents found (page may not exist)")
        
        time.sleep(1)
    
    print(f"\nTotal documents found: {len(all_documents)}")
    
    # Download PDFs
    print("\nDownloading PDFs...")
    downloaded = 0
    skipped = 0
    failed = 0
    
    with open(log_file, 'w') as log:
        log.write("CSB Reports Download Log (Fixed)\n")
        log.write("=" * 60 + "\n\n")
        
        for i, doc in enumerate(all_documents):
            # Create unique filename
            filename = make_unique_filename(doc['investigation'], doc['title'], doc['url'])
            output_path = OUTPUT_DIR / filename
            
            if output_path.exists():
                print(f"[{i+1}/{len(all_documents)}] Already exists: {filename[:50]}...")
                skipped += 1
                continue
            
            print(f"[{i+1}/{len(all_documents)}] {doc['investigation']}: {doc['title'][:30]}...")
            
            if download_pdf(doc['url'], output_path):
                print(f"  -> Saved: {filename[:50]}")
                log.write(f"OK: {filename}\n")
                log.write(f"  Investigation: {doc['investigation']}\n")
                log.write(f"  Type: {doc['type']}\n")
                log.write(f"  URL: {doc['url']}\n\n")
                downloaded += 1
            else:
                log.write(f"FAILED: {doc['title']}\n")
                log.write(f"  URL: {doc['url']}\n\n")
                failed += 1
            
            time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"Investigations processed: {len(KNOWN_INVESTIGATIONS)}")
    print(f"Documents found:          {len(all_documents)}")
    print(f"PDFs downloaded:          {downloaded}")
    print(f"Already existed:          {skipped}")
    print(f"Failed:                   {failed}")
    print(f"\nFiles saved to: {OUTPUT_DIR.absolute()}")


if __name__ == "__main__":
    main()
