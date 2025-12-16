#!/usr/bin/env python3
"""
OSHA Oil & Gas Safety Documents Scraper - FIXED VERSION
Correct URLs as of Dec 2025: /oil-and-gas-extraction (not /oil-and-gas)
"""

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import time
import re
from urllib.parse import urljoin

BASE_URL = "https://www.osha.gov"
OUTPUT_DIR = Path("data/raw/osha_og")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# CORRECT URLs as of Dec 2025
KNOWN_PAGES = [
    # Main Oil & Gas section (CORRECT URL)
    f"{BASE_URL}/oil-and-gas-extraction",
    f"{BASE_URL}/oil-and-gas-extraction/hazards",
    f"{BASE_URL}/oil-and-gas-extraction/health-hazards",
    f"{BASE_URL}/oil-and-gas-extraction/standards",
    f"{BASE_URL}/oil-and-gas-extraction/resources",
    # eTool
    f"{BASE_URL}/etools/oil-and-gas",
    f"{BASE_URL}/etools/oil-and-gas/general-safety",
    f"{BASE_URL}/etools/oil-and-gas/general-safety/common-wellsite-incidents",
    f"{BASE_URL}/etools/oil-and-gas/general-safety/h2s-monitoring",
    # Related topics
    f"{BASE_URL}/hydrogen-sulfide",
    f"{BASE_URL}/confined-spaces",
    f"{BASE_URL}/process-safety-management",
    f"{BASE_URL}/storage-tanks",
    f"{BASE_URL}/storage-tanks/standards",
    # Publications page
    f"{BASE_URL}/publications",
]


def get_pdfs_from_page(url: str) -> list[dict]:
    """Get all PDF links from a page."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}")
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    pdfs = []
    
    for link in soup.find_all('a', href=True):
        href = link['href']
        text = link.get_text(strip=True)
        
        if '.pdf' in href.lower():
            full_url = urljoin(BASE_URL, href)
            pdfs.append({
                'title': text or href.split('/')[-1],
                'url': full_url,
                'source_page': url
            })
    
    return pdfs


def download_pdf(pdf_url: str, output_path: Path) -> bool:
    """Download PDF file."""
    try:
        response = requests.get(pdf_url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        
        if response.content[:4] != b'%PDF':
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
    print("OSHA Oil & Gas Documents Scraper (FIXED)")
    print("=" * 60)
    
    # Collect all PDFs from known pages
    all_pdfs = {}  # URL -> info dict to dedupe
    
    print("\nStep 1: Scanning OSHA pages for PDFs...")
    
    for page_url in KNOWN_PAGES:
        print(f"\nScanning: {page_url}")
        
        pdfs = get_pdfs_from_page(page_url)
        for pdf in pdfs:
            if pdf['url'] not in all_pdfs:
                all_pdfs[pdf['url']] = pdf
                print(f"  Found: {pdf['title'][:50]}...")
        
        time.sleep(0.5)
    
    print(f"\nTotal unique PDFs found: {len(all_pdfs)}")
    
    if not all_pdfs:
        print("\nNo PDFs found. Trying direct publication search...")
        # Try searching for specific known O&G publications
        pub_numbers = ['3622', '4204', '3755', '3816', '3767', '3790']
        for pub in pub_numbers:
            url = f"{BASE_URL}/sites/default/files/publications/OSHA{pub}.pdf"
            all_pdfs[url] = {'title': f'OSHA{pub}', 'url': url, 'source_page': 'direct'}
    
    # Download PDFs
    print("\nStep 2: Downloading PDFs...")
    downloaded = 0
    skipped = 0
    failed = 0
    
    with open(log_file, 'w') as log:
        log.write("OSHA O&G Download Log (Fixed)\n")
        log.write("=" * 60 + "\n\n")
        
        for i, (url, info) in enumerate(all_pdfs.items()):
            filename = url.split('/')[-1]
            if not filename.endswith('.pdf'):
                filename = sanitize_filename(info['title']) + '.pdf'
            
            # Prefix with OSHA for clarity
            if not filename.upper().startswith('OSHA'):
                filename = f"OSHA_{filename}"
            
            output_path = OUTPUT_DIR / filename
            
            if output_path.exists():
                print(f"[{i+1}/{len(all_pdfs)}] Already exists: {filename[:50]}...")
                skipped += 1
                continue
            
            print(f"[{i+1}/{len(all_pdfs)}] Downloading: {filename[:50]}...")
            
            if download_pdf(url, output_path):
                print(f"  -> Saved")
                log.write(f"OK: {filename}\n")
                log.write(f"  URL: {url}\n\n")
                downloaded += 1
            else:
                log.write(f"FAILED: {filename}\n")
                log.write(f"  URL: {url}\n\n")
                failed += 1
            
            time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"PDFs found:         {len(all_pdfs)}")
    print(f"Downloaded:         {downloaded}")
    print(f"Already existed:    {skipped}")
    print(f"Failed:             {failed}")
    print(f"\nFiles saved to: {OUTPUT_DIR.absolute()}")


if __name__ == "__main__":
    main()
