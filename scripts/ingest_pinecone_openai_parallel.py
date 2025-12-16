#!/usr/bin/env python3
"""
Pinecone Ingestion Script - OpenAI Embeddings (Parallel Version)
Embeds document chunks using OpenAI with concurrent requests.

Requires:
  pip install pinecone openai tqdm

Usage:
  python ingest_pinecone_openai_parallel.py [--delete-existing]
"""

import json
import argparse
from pathlib import Path
from tqdm import tqdm
from pinecone import Pinecone, ServerlessSpec
from openai import OpenAI
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
CHUNKS_FILE = PROJECT_ROOT / "scripts" / "data" / "processed" / "chunks" / "all_chunks.jsonl"
PINECONE_KEY_FILE = PROJECT_ROOT / ".pinecone_api_key"
OPENAI_KEY_FILE = PROJECT_ROOT / ".openai_api_key"

# Settings
INDEX_NAME = "og-rag"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 1536

# Batch sizes
OPENAI_BATCH_SIZE = 1000  # OpenAI allows up to 2048 per request
PINECONE_BATCH_SIZE = 100  # Pinecone upsert batch size
MAX_WORKERS = 5  # Concurrent OpenAI requests

# Thread-safe counter
upload_lock = threading.Lock()
total_uploaded = 0


def load_chunks(filepath: Path) -> list[dict]:
    """Load chunks from JSONL file."""
    chunks = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            chunks.append(json.loads(line))
    return chunks


def get_embeddings_batch(texts: list[str], client: OpenAI) -> list[list[float]]:
    """Get embeddings for a batch of texts from OpenAI."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = client.embeddings.create(
                model=EMBEDDING_MODEL,
                input=texts
            )
            return [item.embedding for item in response.data]
        except Exception as e:
            if "rate_limit" in str(e).lower() or "429" in str(e):
                wait_time = 2 ** attempt
                print(f"\nRate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
            elif attempt < max_retries - 1:
                print(f"\nRetrying due to: {e}")
                time.sleep(1)
            else:
                raise
    return []


def process_batch(batch_data: tuple, openai_client: OpenAI, index) -> int:
    """Process a single batch: embed and upload to Pinecone."""
    global total_uploaded
    batch_idx, batch_chunks = batch_data
    
    # Get texts and embeddings
    texts = [c["text"] for c in batch_chunks]
    embeddings = get_embeddings_batch(texts, openai_client)
    
    if not embeddings:
        return 0
    
    # Prepare vectors
    vectors = []
    for chunk, embedding in zip(batch_chunks, embeddings):
        vectors.append({
            "id": chunk["chunk_id"],
            "values": embedding,
            "metadata": {
                "text": chunk["text"][:8000],
                "source": chunk.get("source", "unknown"),
                "doc_type": chunk.get("doc_type", "unknown"),
                "source_file": chunk.get("source_file", ""),
                "doc_id": chunk.get("doc_id", ""),
            }
        })
    
    # Upload to Pinecone in smaller batches
    for i in range(0, len(vectors), PINECONE_BATCH_SIZE):
        pinecone_batch = vectors[i:i + PINECONE_BATCH_SIZE]
        index.upsert(vectors=pinecone_batch)
    
    with upload_lock:
        total_uploaded += len(vectors)
    
    return len(vectors)


def main():
    global total_uploaded
    
    parser = argparse.ArgumentParser(description="Ingest chunks into Pinecone with OpenAI embeddings (parallel)")
    parser.add_argument("--delete-existing", action="store_true", help="Delete existing index first")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help=f"Number of concurrent workers (default: {MAX_WORKERS})")
    parser.add_argument("--batch-size", type=int, default=OPENAI_BATCH_SIZE, help=f"OpenAI batch size (default: {OPENAI_BATCH_SIZE})")
    args = parser.parse_args()

    print("=" * 60)
    print("Pinecone Ingestion - OpenAI Embeddings (PARALLEL)")
    print("=" * 60)

    # Load API keys
    if not PINECONE_KEY_FILE.exists():
        print(f"ERROR: Pinecone API key file not found: {PINECONE_KEY_FILE}")
        return
    if not OPENAI_KEY_FILE.exists():
        print(f"ERROR: OpenAI API key file not found: {OPENAI_KEY_FILE}")
        return

    pinecone_key = PINECONE_KEY_FILE.read_text().strip()
    openai_key = OPENAI_KEY_FILE.read_text().strip()
    print("✓ API keys loaded")

    # Initialize clients
    pc = Pinecone(api_key=pinecone_key)
    openai_client = OpenAI(api_key=openai_key)
    print("✓ Clients initialized")

    # Handle index
    existing_indexes = [idx.name for idx in pc.list_indexes()]

    if args.delete_existing and INDEX_NAME in existing_indexes:
        print(f"Deleting existing index '{INDEX_NAME}'...")
        pc.delete_index(INDEX_NAME)
        time.sleep(5)
        existing_indexes = []

    if INDEX_NAME not in existing_indexes:
        print(f"Creating index '{INDEX_NAME}'...")
        pc.create_index(
            name=INDEX_NAME,
            dimension=EMBEDDING_DIMENSIONS,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1")
        )
        print("Waiting for index to be ready...")
        while not pc.describe_index(INDEX_NAME).status.ready:
            time.sleep(2)
        print("Index ready!")

    index = pc.Index(INDEX_NAME)
    stats = index.describe_index_stats()
    print(f"Current vectors in index: {stats.total_vector_count}")

    # Load chunks
    if not CHUNKS_FILE.exists():
        print(f"ERROR: Chunks file not found: {CHUNKS_FILE}")
        return

    print(f"Loading chunks from {CHUNKS_FILE}...")
    chunks = load_chunks(CHUNKS_FILE)
    print(f"Loaded {len(chunks):,} chunks")

    # Create batches
    batches = []
    for i in range(0, len(chunks), args.batch_size):
        batches.append((i // args.batch_size, chunks[i:i + args.batch_size]))
    
    print(f"\nProcessing {len(batches)} batches with {args.workers} workers...")
    print(f"OpenAI batch size: {args.batch_size}")
    print(f"Embedding model: {EMBEDDING_MODEL}")

    # Process in parallel
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_batch, batch, openai_client, index): batch[0] 
            for batch in batches
        }
        
        with tqdm(total=len(chunks), desc="Embedding & uploading") as pbar:
            for future in as_completed(futures):
                try:
                    count = future.result()
                    pbar.update(count)
                except Exception as e:
                    print(f"\nBatch failed: {e}")

    elapsed = time.time() - start_time
    
    # Final stats
    stats = index.describe_index_stats()
    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    print(f"Total vectors uploaded: {total_uploaded:,}")
    print(f"Total vectors in index: {stats.total_vector_count:,}")
    print(f"Time elapsed: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"Rate: {total_uploaded/elapsed:.1f} vectors/sec")
    print(f"Index name: {INDEX_NAME}")
    print(f"Embedding model: {EMBEDDING_MODEL}")
    print("✓ Ready for RAG queries!")


if __name__ == "__main__":
    main()
