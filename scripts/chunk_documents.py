#!/usr/bin/env python3
"""
Document Chunking for O&G RAG Project
Creates overlapping chunks suitable for embedding and retrieval
"""

from pathlib import Path
import json
import re
import argparse
from dataclasses import dataclass, asdict
from typing import Iterator
import hashlib

INPUT_DIR = Path("data/processed/extracted_text")
OUTPUT_DIR = Path("data/processed/chunks")
CHUNKS_FILE = OUTPUT_DIR / "all_chunks.jsonl"


@dataclass
class Chunk:
    """Represents a text chunk for RAG."""
    chunk_id: str
    text: str
    source_file: str
    source: str  # bsee, phmsa, osha, csb
    doc_type: str
    chunk_index: int
    total_chunks: int
    char_start: int
    char_end: int
    # Metadata for filtering
    equipment: list
    hazards: list
    operations: list


def create_chunk_id(source_file: str, chunk_index: int, text: str) -> str:
    """Create a unique chunk ID."""
    content_hash = hashlib.md5(text.encode()).hexdigest()[:8]
    return f"{Path(source_file).stem}_{chunk_index}_{content_hash}"


def split_into_sentences(text: str) -> list[str]:
    """Split text into sentences."""
    # Handle common abbreviations to avoid false splits
    text = re.sub(r'\b(Mr|Mrs|Ms|Dr|Prof|Inc|Ltd|Corp|vs|etc|e\.g|i\.e)\.\s', r'\1<PERIOD> ', text)
    text = re.sub(r'(\d)\.\s', r'\1<PERIOD> ', text)  # Numbers followed by period
    
    # Split on sentence boundaries
    sentences = re.split(r'(?<=[.!?])\s+', text)
    
    # Restore periods
    sentences = [s.replace('<PERIOD>', '.') for s in sentences]
    
    return [s.strip() for s in sentences if s.strip()]


def chunk_text(
    text: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    min_chunk_size: int = 100,
) -> Iterator[tuple[str, int, int]]:
    """
    Split text into overlapping chunks.
    
    Yields: (chunk_text, char_start, char_end)
    """
    if len(text) < min_chunk_size:
        yield text, 0, len(text)
        return
    
    sentences = split_into_sentences(text)
    
    current_chunk = []
    current_length = 0
    char_position = 0
    chunk_start = 0
    
    for sentence in sentences:
        sentence_length = len(sentence)
        
        # If adding this sentence exceeds chunk size
        if current_length + sentence_length > chunk_size and current_chunk:
            # Yield current chunk
            chunk_text = ' '.join(current_chunk)
            yield chunk_text, chunk_start, char_position
            
            # Start new chunk with overlap
            overlap_length = 0
            overlap_sentences = []
            
            for s in reversed(current_chunk):
                if overlap_length + len(s) < chunk_overlap:
                    overlap_sentences.insert(0, s)
                    overlap_length += len(s) + 1
                else:
                    break
            
            current_chunk = overlap_sentences
            current_length = sum(len(s) for s in current_chunk) + len(current_chunk)
            chunk_start = char_position - current_length
        
        current_chunk.append(sentence)
        current_length += sentence_length + 1
        char_position += sentence_length + 1
    
    # Yield final chunk
    if current_chunk:
        chunk_text = ' '.join(current_chunk)
        if len(chunk_text) >= min_chunk_size:
            yield chunk_text, chunk_start, char_position


def clean_text(text: str) -> str:
    """Clean extracted text for better chunking."""
    # Remove page markers we added
    text = re.sub(r'\[Page \d+\]\n*', '\n\n', text)
    
    # Normalize whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    
    # Remove headers/footers (common patterns)
    text = re.sub(r'^\s*Page \d+ of \d+\s*$', '', text, flags=re.MULTILINE)
    
    return text.strip()


def process_document(
    text_file: Path,
    meta_file: Path,
    chunk_size: int,
    chunk_overlap: int,
) -> list[Chunk]:
    """Process a single document into chunks."""
    
    # Load text and metadata
    text = text_file.read_text(encoding='utf-8')
    
    try:
        metadata = json.loads(meta_file.read_text(encoding='utf-8'))
    except:
        metadata = {}
    
    # Clean text
    text = clean_text(text)
    
    # Skip very short documents
    if len(text) < 200:
        return []
    
    # Create chunks
    chunks = []
    chunk_list = list(chunk_text(text, chunk_size, chunk_overlap))
    
    for i, (chunk_text_content, char_start, char_end) in enumerate(chunk_list):
        chunk = Chunk(
            chunk_id=create_chunk_id(text_file.name, i, chunk_text_content),
            text=chunk_text_content,
            source_file=text_file.stem,
            source=metadata.get("source", "unknown"),
            doc_type=metadata.get("doc_type", "unknown"),
            chunk_index=i,
            total_chunks=len(chunk_list),
            char_start=char_start,
            char_end=char_end,
            equipment=metadata.get("equipment", []),
            hazards=metadata.get("hazards", []),
            operations=metadata.get("operations", []),
        )
        chunks.append(chunk)
    
    return chunks


def main():
    parser = argparse.ArgumentParser(description="Chunk O&G documents for RAG")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Target chunk size in characters")
    parser.add_argument("--overlap", type=int, default=200, help="Overlap between chunks")
    parser.add_argument("--source", type=str, default=None, help="Only process specific source")
    args = parser.parse_args()
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("Document Chunking for O&G RAG")
    print("=" * 60)
    print(f"Chunk size: {args.chunk_size} chars")
    print(f"Overlap: {args.overlap} chars")
    
    # Find all text files
    text_files = list(INPUT_DIR.glob("*.txt"))
    
    if args.source:
        text_files = [f for f in text_files if args.source in f.stem.lower()]
    
    print(f"\nFound {len(text_files)} text files")
    
    if not text_files:
        print("No text files found. Run extract_text.py first.")
        return
    
    # Process all documents
    all_chunks = []
    doc_count = 0
    
    for i, text_file in enumerate(text_files):
        meta_file = text_file.with_suffix('.json')
        
        if not meta_file.exists():
            meta_file = None
        
        chunks = process_document(text_file, meta_file, args.chunk_size, args.overlap)
        
        if chunks:
            all_chunks.extend(chunks)
            doc_count += 1
            
            if (i + 1) % 100 == 0:
                print(f"Processed {i + 1}/{len(text_files)} documents, {len(all_chunks)} chunks so far...")
    
    print(f"\nProcessed {doc_count} documents")
    print(f"Total chunks: {len(all_chunks)}")
    
    # Save chunks as JSONL
    print(f"\nSaving to {CHUNKS_FILE}...")
    
    with open(CHUNKS_FILE, 'w', encoding='utf-8') as f:
        for chunk in all_chunks:
            f.write(json.dumps(asdict(chunk)) + '\n')
    
    # Also save summary stats
    stats = {
        "total_documents": doc_count,
        "total_chunks": len(all_chunks),
        "chunk_size": args.chunk_size,
        "overlap": args.overlap,
        "avg_chunk_length": sum(len(c.text) for c in all_chunks) // len(all_chunks) if all_chunks else 0,
        "by_source": {},
        "by_doc_type": {},
    }
    
    for chunk in all_chunks:
        stats["by_source"][chunk.source] = stats["by_source"].get(chunk.source, 0) + 1
        stats["by_doc_type"][chunk.doc_type] = stats["by_doc_type"].get(chunk.doc_type, 0) + 1
    
    stats_file = OUTPUT_DIR / "chunking_stats.json"
    stats_file.write_text(json.dumps(stats, indent=2))
    
    # Summary
    print("\n" + "=" * 60)
    print("CHUNKING COMPLETE")
    print("=" * 60)
    print(f"Documents processed: {doc_count}")
    print(f"Total chunks: {len(all_chunks)}")
    print(f"Average chunk length: {stats['avg_chunk_length']} chars")
    print(f"\nChunks file: {CHUNKS_FILE.absolute()}")
    
    print("\nBy source:")
    for source, count in sorted(stats["by_source"].items()):
        print(f"  {source}: {count} chunks")
    
    print("\nBy document type:")
    for doc_type, count in sorted(stats["by_doc_type"].items(), key=lambda x: -x[1]):
        print(f"  {doc_type}: {count} chunks")


if __name__ == "__main__":
    main()
