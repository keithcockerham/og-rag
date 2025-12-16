#!/usr/bin/env python3
"""
PDF & Text Extraction for O&G RAG Project - Phase 2 Update
Handles:
- PDFs (regulatory docs, technical manuals)
- Text files (PetroWiki articles)
- JSONL files (glossary terms)
"""

import pymupdf  # PyMuPDF (fitz)
from pathlib import Path
import json
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

RAW_DIR = Path("data/raw")
OUTPUT_DIR = Path("data/processed/extracted_text")
MANIFEST_FILE = Path("data/processed/manifest.json")


def classify_document(text: str, filename: str, source_dir: str) -> dict:
    """Classify document type and extract metadata."""
    text_lower = text.lower()[:5000]
    filename_lower = filename.lower()
    source_lower = source_dir.lower()
    
    # Source classification
    source = "unknown"
    if "bsee" in source_lower:
        source = "bsee"
    elif "phmsa" in source_lower:
        source = "phmsa"
    elif "osha" in source_lower:
        source = "osha"
    elif "csb" in source_lower:
        source = "csb"
    elif "petrowiki" in source_lower:
        source = "petrowiki"
    elif "slb" in source_lower or "glossary" in source_lower:
        source = "slb"
    elif "iadc" in source_lower:
        source = "iadc"
    elif "esp" in source_lower or "pump" in filename_lower:
        source = "equipment_manual"
    
    # Document type classification
    doc_type = "general"
    if "safety alert" in text_lower or "alert" in filename_lower:
        doc_type = "safety_alert"
    elif "investigation" in text_lower and "report" in text_lower:
        doc_type = "investigation_report"
    elif "advisory" in text_lower or "bulletin" in text_lower:
        doc_type = "advisory_bulletin"
    elif "guidance" in text_lower or "guide" in filename_lower:
        doc_type = "guidance"
    elif "regulation" in text_lower or "cfr" in text_lower:
        doc_type = "regulation"
    elif "glossary" in source_lower or "definition" in text_lower[:500]:
        doc_type = "glossary"
    elif "petrowiki" in source_lower:
        doc_type = "technical_article"
    elif "manual" in filename_lower or "catalog" in filename_lower:
        doc_type = "equipment_manual"
    elif "troubleshoot" in text_lower:
        doc_type = "troubleshooting"
    elif "well control" in text_lower:
        doc_type = "well_control"
    
    # Equipment/topic extraction
    equipment = []
    equipment_patterns = [
        (r'\b(BOP|blowout preventer)s?\b', 'BOP'),
        (r'\b(ESP|electric submersible pump)s?\b', 'ESP'),
        (r'\bcompressor\b', 'compressor'),
        (r'\bwellhead\b', 'wellhead'),
        (r'\bpipeline\b', 'pipeline'),
        (r'\bvalve\b', 'valve'),
        (r'\bpump\b', 'pump'),
        (r'\bseparator\b', 'separator'),
        (r'\bheater.treater\b', 'heater_treater'),
        (r'\btank\b', 'tank'),
        (r'\bcrane\b', 'crane'),
        (r'\bscaffold\b', 'scaffold'),
        (r'\bgenerator\b', 'generator'),
        (r'\bturbine\b', 'turbine'),
        (r'\bheat exchanger\b', 'heat_exchanger'),
        (r'\bboiler\b', 'boiler'),
        (r'\bflare\b', 'flare'),
        (r'\bdrill string\b', 'drill_string'),
        (r'\bcasing\b', 'casing'),
        (r'\btubing\b', 'tubing'),
        (r'\bpacker\b', 'packer'),
        (r'\bperforat', 'perforation'),
        (r'\bchristmas tree\b', 'christmas_tree'),
        (r'\bchoke\b', 'choke'),
        (r'\bmotor\b', 'motor'),
        (r'\bseal\b', 'seal'),
        (r'\bcable\b', 'cable'),
    ]
    for pattern, name in equipment_patterns:
        if re.search(pattern, text_lower):
            equipment.append(name)
    
    # Hazard classification
    hazards = []
    hazard_patterns = [
        (r'\bh2s\b|hydrogen sulfide', 'H2S'),
        (r'\bfire\b', 'fire'),
        (r'\bexplosion\b', 'explosion'),
        (r'\bfall\b', 'fall'),
        (r'\bstruck.by\b', 'struck_by'),
        (r'\bcaught.in\b', 'caught_in'),
        (r'\belectr', 'electrical'),
        (r'\bconfined space\b', 'confined_space'),
        (r'\bcorrosion\b', 'corrosion'),
        (r'\bpressure\b', 'pressure'),
        (r'\brelease\b|leak\b', 'release'),
        (r'\bfatigue\b', 'fatigue'),
        (r'\bfracture\b', 'fracture'),
        (r'\bgas lock', 'gas_lock'),
        (r'\bscale\b', 'scale'),
        (r'\bwax\b', 'wax'),
        (r'\bhydrate\b', 'hydrate'),
        (r'\berosion\b', 'erosion'),
    ]
    for pattern, name in hazard_patterns:
        if re.search(pattern, text_lower):
            hazards.append(name)
    
    # Operation type
    operations = []
    if re.search(r'\bdrilling\b', text_lower):
        operations.append("drilling")
    if re.search(r'\bproduction\b', text_lower):
        operations.append("production")
    if re.search(r'\bcompletion\b', text_lower):
        operations.append("completions")
    if re.search(r'\bworkover\b', text_lower):
        operations.append("workover")
    if re.search(r'\bpipeline\b|midstream\b', text_lower):
        operations.append("midstream")
    if re.search(r'\brefin', text_lower):
        operations.append("downstream")
    if re.search(r'\boffshore\b', text_lower):
        operations.append("offshore")
    if re.search(r'\bonshore\b', text_lower):
        operations.append("onshore")
    if re.search(r'\bartificial lift\b', text_lower):
        operations.append("artificial_lift")
    if re.search(r'\bwell control\b', text_lower):
        operations.append("well_control")
    if re.search(r'\bstimulation\b|fracturing\b|acidizing\b', text_lower):
        operations.append("stimulation")
    
    return {
        "source": source,
        "doc_type": doc_type,
        "equipment": list(set(equipment)),
        "hazards": list(set(hazards)),
        "operations": list(set(operations)),
    }


def extract_text_from_pdf(pdf_path: Path) -> tuple[str, dict]:
    """Extract text and metadata from a PDF."""
    try:
        doc = pymupdf.open(pdf_path)
        
        text_parts = []
        for page_num, page in enumerate(doc):
            page_text = page.get_text()
            if page_text.strip():
                text_parts.append(f"[Page {page_num + 1}]\n{page_text}")
        
        full_text = "\n\n".join(text_parts)
        
        pdf_metadata = doc.metadata
        
        metadata = {
            "filename": pdf_path.name,
            "source_path": str(pdf_path),
            "file_type": "pdf",
            "page_count": len(doc),
            "char_count": len(full_text),
            "word_count": len(full_text.split()),
            "pdf_title": pdf_metadata.get("title", ""),
            "pdf_author": pdf_metadata.get("author", ""),
            "extracted_at": datetime.now().isoformat(),
        }
        
        source_dir = pdf_path.parent.name
        classification = classify_document(full_text, pdf_path.name, source_dir)
        metadata.update(classification)
        
        doc.close()
        return full_text, metadata
        
    except Exception as e:
        return "", {"error": str(e), "filename": pdf_path.name}


def extract_text_from_txt(txt_path: Path) -> tuple[str, dict]:
    """Extract text from a plain text file."""
    try:
        text = txt_path.read_text(encoding='utf-8')
        
        metadata = {
            "filename": txt_path.name,
            "source_path": str(txt_path),
            "file_type": "text",
            "char_count": len(text),
            "word_count": len(text.split()),
            "extracted_at": datetime.now().isoformat(),
        }
        
        source_dir = txt_path.parent.name
        classification = classify_document(text, txt_path.name, source_dir)
        metadata.update(classification)
        
        return text, metadata
        
    except Exception as e:
        return "", {"error": str(e), "filename": txt_path.name}


def extract_glossary_from_jsonl(jsonl_path: Path) -> list[tuple[str, dict]]:
    """Extract glossary terms from JSONL file."""
    results = []
    
    try:
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                
                entry = json.loads(line)
                term = entry.get('term', 'Unknown')
                definition = entry.get('definition', '')
                related = entry.get('related_terms', [])
                
                if not definition or len(definition) < 20:
                    continue
                
                text = f"# {term}\n\n{definition}"
                if related:
                    text += f"\n\nRelated terms: {', '.join(related)}"
                
                metadata = {
                    "filename": f"glossary_{term}",
                    "source_path": str(jsonl_path),
                    "file_type": "glossary_entry",
                    "char_count": len(text),
                    "word_count": len(text.split()),
                    "source": "slb",
                    "doc_type": "glossary",
                    "equipment": [],
                    "hazards": [],
                    "operations": [],
                    "extracted_at": datetime.now().isoformat(),
                }
                
                results.append((term, text, metadata))
        
    except Exception as e:
        print(f"Error processing {jsonl_path}: {e}")
    
    return results


def process_file(file_path: Path, output_dir: Path) -> dict:
    """Process a single file and save results."""
    
    if file_path.suffix.lower() == '.pdf':
        text, metadata = extract_text_from_pdf(file_path)
    elif file_path.suffix.lower() in ['.txt', '.md']:
        text, metadata = extract_text_from_txt(file_path)
    else:
        return {"status": "skipped", "file": file_path.name, "reason": "unsupported format"}
    
    if not text:
        return {"status": "error", "file": file_path.name, "error": metadata.get("error", "Empty")}
    
    output_name = file_path.stem
    
    text_file = output_dir / f"{output_name}.txt"
    text_file.write_text(text, encoding='utf-8')
    
    meta_file = output_dir / f"{output_name}.json"
    meta_file.write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    
    return {
        "status": "success",
        "file": file_path.name,
        "chars": metadata["char_count"],
        "doc_type": metadata.get("doc_type", "unknown"),
    }


def sanitize_filename(name: str, max_length: int = 80) -> str:
    """Create safe filename."""
    safe = re.sub(r'[<>:"/\\|?*]', '_', name)
    safe = re.sub(r'\s+', '_', safe)
    safe = re.sub(r'_+', '_', safe)
    safe = safe.strip('_.')
    return safe[:max_length]


def main():
    parser = argparse.ArgumentParser(description="Extract text from O&G documents")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--source", type=str, default=None, help="Only process specific source directory")
    args = parser.parse_args()
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("Text Extraction for O&G RAG (Phase 2 Update)")
    print("=" * 60)
    
    # Find all files to process
    all_files = []
    
    # PDFs
    if args.source:
        pdf_files = list(RAW_DIR.glob(f"{args.source}*/**/*.pdf"))
        txt_files = list(RAW_DIR.glob(f"{args.source}*/**/*.txt"))
    else:
        pdf_files = list(RAW_DIR.glob("**/*.pdf"))
        txt_files = [f for f in RAW_DIR.glob("**/*.txt") if 'extracted_text' not in str(f)]
    
    all_files = pdf_files + txt_files
    
    print(f"\nFound {len(pdf_files)} PDFs and {len(txt_files)} text files")
    
    # Find JSONL glossary files
    jsonl_files = list(RAW_DIR.glob("**/*.jsonl"))
    print(f"Found {len(jsonl_files)} JSONL files (glossary)")
    
    # Check existing
    existing = set(f.stem for f in OUTPUT_DIR.glob("*.txt"))
    to_process = [f for f in all_files if f.stem not in existing]
    
    print(f"Already processed: {len(existing)}")
    print(f"To process: {len(to_process)}")
    
    # Process regular files
    if to_process:
        print(f"\nProcessing files with {args.workers} workers...")
        
        success = 0
        errors = 0
        total_chars = 0
        
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(process_file, f, OUTPUT_DIR): f for f in to_process}
            
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                
                if result["status"] == "success":
                    success += 1
                    total_chars += result["chars"]
                    if (i + 1) % 50 == 0:
                        print(f"[{i+1}/{len(to_process)}] Processed {success} files...")
                elif result["status"] == "error":
                    errors += 1
        
        print(f"\nFiles processed: {success}, Errors: {errors}")
        print(f"Total characters: {total_chars:,}")
    
    # Process glossary JSONL files
    if jsonl_files:
        print("\nProcessing glossary files...")
        glossary_count = 0
        
        for jsonl_file in jsonl_files:
            entries = extract_glossary_from_jsonl(jsonl_file)
            
            for term, text, metadata in entries:
                safe_name = f"glossary_{sanitize_filename(term)}"
                
                if safe_name in existing:
                    continue
                
                text_file = OUTPUT_DIR / f"{safe_name}.txt"
                meta_file = OUTPUT_DIR / f"{safe_name}.json"
                
                text_file.write_text(text, encoding='utf-8')
                meta_file.write_text(json.dumps(metadata, indent=2), encoding='utf-8')
                
                glossary_count += 1
        
        print(f"Glossary terms extracted: {glossary_count}")
    
    # Build manifest
    print("\nBuilding manifest...")
    
    all_processed = list(OUTPUT_DIR.glob("*.txt"))
    
    manifest = {
        "extraction_date": datetime.now().isoformat(),
        "total_files": len(all_processed),
        "sources": {},
    }
    
    for txt_file in all_processed:
        meta_file = txt_file.with_suffix('.json')
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text())
                source = meta.get("source", "unknown")
                manifest["sources"][source] = manifest["sources"].get(source, 0) + 1
            except:
                pass
    
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_FILE.write_text(json.dumps(manifest, indent=2))
    
    # Summary
    print("\n" + "=" * 60)
    print("EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"Total documents: {len(all_processed)}")
    print(f"\nBy source:")
    for source, count in sorted(manifest["sources"].items()):
        print(f"  {source}: {count}")
    print(f"\nOutput directory: {OUTPUT_DIR.absolute()}")


if __name__ == "__main__":
    main()
