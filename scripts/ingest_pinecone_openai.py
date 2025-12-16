#!/usr/bin/env python3
"""
Pinecone Ingestion Script - OpenAI Embeddings Version
Embeds document chunks using OpenAI and uploads to Pinecone.

Requires:
  pip install pinecone-client openai tqdm

Usage:
  python ingest_pinecone_openai.py [--delete-existing]
"""

import json
import argparse
from pathlib import Path
from tqdm import tqdm
from pinecone import Pinecone, ServerlessSpec
from openai import OpenAI
import time

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
CHUNKS_FILE = PROJECT_ROOT / "scripts" / "data" / "processed" / "chunks" / "all_chunks.jsonl"
PINECONE_KEY_FILE = PROJECT_ROOT / ".pinecone_api_key"
OPENAI_KEY_FILE = PROJECT_ROOT / ".openai_api_key"

# Settings
INDEX_NAME = "og-rag"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 1536
BATCH_SIZE = 100  # Pinecone upsert batch size
EMBED_BATCH_SIZE = 100  # OpenAI embedding batch size


def load_chunks(filepath: Path) -> list[dict]:
    """Load chunks from JSONL file."""
    chunks = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            chunks.append(json.loads(line))
    return chunks


def get_embeddings_batch(texts: list[str], client: OpenAI) -> list[list[float]]:
    """Get embeddings for a batch of texts from OpenAI."""
    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts
    )
    return [item.embedding for item in response.data]


def main():
    parser = argparse.ArgumentParser(description="Ingest chunks into Pinecone with OpenAI embeddings")
    parser.add_argument("--delete-existing", action="store_true", help="Delete existing index first")
    args = parser.parse_args()

    print("=" * 60)
    print("Pinecone Ingestion for O&G RAG (OpenAI Embeddings)")
    print("=" * 60)

    # Load API keys
    if not PINECONE_KEY_FILE.exists():
        print(f"ERROR: Pinecone API key file not found: {PINECONE_KEY_FILE}")
        return
    if not OPENAI_KEY_FILE.exists():
        print(f"ERROR: OpenAI API key file not found: {OPENAI_KEY_FILE}")
        print("Create it with: echo 'your-key' > .openai_api_key")
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
        time.sleep(5)  # Wait for deletion
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
        print("Run chunk_documents.py first.")
        return

    print(f"Loading chunks from {CHUNKS_FILE}...")
    chunks = load_chunks(CHUNKS_FILE)
    print(f"Loaded {len(chunks):,} chunks")

    # Process in batches
    print(f"\nEmbedding and uploading in batches of {BATCH_SIZE}...")
    print(f"Using OpenAI model: {EMBEDDING_MODEL}")
    
    total_uploaded = 0
    
    for i in tqdm(range(0, len(chunks), BATCH_SIZE), desc="Processing batches"):
        batch_chunks = chunks[i:i + BATCH_SIZE]
        
        # Get texts for embedding
        texts = [c["text"] for c in batch_chunks]
        
        # Get embeddings from OpenAI (with retry logic)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                embeddings = get_embeddings_batch(texts, openai_client)
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"\nRetrying batch due to: {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
        
        # Prepare vectors for Pinecone
        vectors = []
        for chunk, embedding in zip(batch_chunks, embeddings):
            vectors.append({
                "id": chunk["chunk_id"],
                "values": embedding,
                "metadata": {
                    "text": chunk["text"][:8000],  # Pinecone metadata limit
                    "source": chunk.get("source", "unknown"),
                    "doc_type": chunk.get("doc_type", "unknown"),
                    "source_file": chunk.get("source_file", ""),
                    "doc_id": chunk.get("doc_id", ""),
                }
            })
        
        # Upsert to Pinecone
        index.upsert(vectors=vectors)
        total_uploaded += len(vectors)
        
        # Small delay to avoid rate limits
        time.sleep(0.1)

    # Final stats
    stats = index.describe_index_stats()
    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    print(f"Total vectors uploaded: {total_uploaded:,}")
    print(f"Total vectors in index: {stats.total_vector_count:,}")
    print(f"Index name: {INDEX_NAME}")
    print(f"Embedding model: {EMBEDDING_MODEL}")
    print(f"Dimensions: {EMBEDDING_DIMENSIONS}")
    print("✓ Ready for RAG queries!")


if __name__ == "__main__":
    main()
