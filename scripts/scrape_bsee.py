#!/usr/bin/env python3
"""
BSEE Safety Alerts Scraper
Scrapes PDF safety alerts from bsee.gov

Tested structure as of Dec 2025:
- Main listing: https://www.bsee.gov/guidance-and-regulations/guidance/safety-alerts-program
- Paginated with ?page=0, ?page=1, etc. (12 pages total)
- Each row links to detail page
- PDFs are on detail pages
"""

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import time
import re
from urllib.parse import urljoin

BASE_URL = "https://www.bsee.gov"
LISTING_URL = f"{BASE_URL}/guidance-and-regulations/guidance/safety-alerts-program"
OUTPUT_DIR = Path("data/raw/bsee_safety_alerts")

# Be a good citizen
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def get_alert_pages(listing_url: str, max_pages: int = 15) -> list[dict]:
    """Get all alert detail page URLs from the paginated listing."""
    all_alerts = []
    
    for page_num in range(max_pages):
        page_url = f"{listing_url}?page={page_num}"
        print(f"Fetching listing page {page_num}...")
        
        try:
            response = requests.get(page_url, headers=HEADERS, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching {page_url}: {e}")
            break
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the table with safety alerts
        table = soup.find('table')
        if not table:
            print(f"No table found on page {page_num}, stopping.")
            break
        
        rows = table.find_all('tr')[1:]  # Skip header row
        
        if not rows:
            print(f"No more alerts on page {page_num}, stopping.")
            break
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 4:
                number_cell = cells[0]
                title_cell = cells[1]
                date_cell = cells[2]
                category_cell = cells[3]
                
                # Get link from title cell
                link = title_cell.find('a')
                if link and link.get('href'):
                    alert_info = {
                        'number': number_cell.get_text(strip=True),
                        'title': title_cell.get_text(strip=True),
                        'date': date_cell.get_text(strip=True),
                        'category': category_cell.get_text(strip=True),
                        'detail_url': urljoin(BASE_URL, link['href'])
                    }
                    all_alerts.append(alert_info)
        
        print(f"  Found {len(rows)} alerts on page {page_num}")
        time.sleep(1)  # Rate limiting
    
    return all_alerts


def get_pdf_from_detail_page(detail_url: str) -> str | None:
    """Visit detail page and extract PDF URL."""
    try:
        response = requests.get(detail_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching detail page: {e}")
        return None
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Look for PDF links
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '.pdf' in href.lower():
            return urljoin(BASE_URL, href)
    
    return None


def download_pdf(pdf_url: str, output_path: Path) -> bool:
    """Download PDF file."""
    try:
        response = requests.get(pdf_url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        
        # Verify it's actually a PDF
        if response.content[:4] != b'%PDF':
            print(f"  Not a valid PDF: {pdf_url}")
            return False
        
        output_path.write_bytes(response.content)
        return True
        
    except requests.RequestException as e:
        print(f"  Error downloading PDF: {e}")
        return False


def sanitize_filename(name: str, max_length: int = 100) -> str:
    """Create a safe filename."""
    # Remove/replace problematic characters
    safe = re.sub(r'[<>:"/\\|?*]', '_', name)
    safe = re.sub(r'\s+', '_', safe)
    safe = re.sub(r'_+', '_', safe)
    safe = safe.strip('_.')
    
    # Truncate if needed
    if len(safe) > max_length:
        safe = safe[:max_length]
    
    return safe


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create a log file
    log_file = OUTPUT_DIR / "download_log.txt"
    
    print("=" * 60)
    print("BSEE Safety Alerts Scraper")
    print("=" * 60)
    
    # Step 1: Get all alert listings
    print("\nStep 1: Fetching alert listings...")
    alerts = get_alert_pages(LISTING_URL)
    print(f"\nFound {len(alerts)} total alerts")
    
    if not alerts:
        print("No alerts found. Check if website structure has changed.")
        return
    
    # Step 2: Download PDFs
    print("\nStep 2: Downloading PDFs...")
    downloaded = 0
    skipped = 0
    failed = 0
    
    with open(log_file, 'w') as log:
        log.write("BSEE Safety Alerts Download Log\n")
        log.write("=" * 60 + "\n\n")
        
        for i, alert in enumerate(alerts):
            number = alert['number']
            title = alert['title']
            
            # Create filename
            filename = f"BSEE_Alert_{number}_{sanitize_filename(title)}.pdf"
            output_path = OUTPUT_DIR / filename
            
            # Skip if already exists
            if output_path.exists():
                print(f"[{i+1}/{len(alerts)}] Already exists: {filename[:50]}...")
                skipped += 1
                continue
            
            print(f"[{i+1}/{len(alerts)}] Processing Alert {number}: {title[:40]}...")
            
            # Get PDF URL from detail page
            pdf_url = get_pdf_from_detail_page(alert['detail_url'])
            
            if not pdf_url:
                print(f"  No PDF found on detail page")
                log.write(f"NO PDF: {number} - {title}\n")
                log.write(f"  Detail URL: {alert['detail_url']}\n\n")
                failed += 1
                time.sleep(0.5)
                continue
            
            # Download PDF
            if download_pdf(pdf_url, output_path):
                print(f"  -> Saved: {filename[:50]}...")
                log.write(f"OK: {number} - {title}\n")
                log.write(f"  PDF: {pdf_url}\n")
                log.write(f"  File: {filename}\n\n")
                downloaded += 1
            else:
                log.write(f"FAILED: {number} - {title}\n")
                log.write(f"  PDF URL: {pdf_url}\n\n")
                failed += 1
            
            time.sleep(1)  # Rate limiting
    
    # Summary
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"Total alerts found: {len(alerts)}")
    print(f"Downloaded:         {downloaded}")
    print(f"Already existed:    {skipped}")
    print(f"Failed/No PDF:      {failed}")
    print(f"\nFiles saved to: {OUTPUT_DIR.absolute()}")
    print(f"Log file: {log_file.absolute()}")


if __name__ == "__main__":
    main()
