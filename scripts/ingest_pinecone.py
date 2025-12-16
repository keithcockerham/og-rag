#!/usr/bin/env python3
"""
Pinecone Vector Store Ingestion for O&G RAG
Embeds chunks and uploads to Pinecone for retrieval
"""

import json
from pathlib import Path
from pinecone import Pinecone, ServerlessSpec
from sentence_transformers import SentenceTransformer
import argparse
from tqdm import tqdm
import time
import os

os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
# Paths
PROJECT_ROOT = Path(__file__).parent.parent
CHUNKS_FILE = PROJECT_ROOT / "/Projects/energy-rag/scripts/data/processed/chunks/all_chunks.jsonl"
API_KEY_FILE = PROJECT_ROOT / ".pinecone_api_key"

# Pinecone settings
INDEX_NAME = "og-rag"
EMBEDDING_MODEL = "BAAI/bge-base-en-v1.5"  # 768 dims, excellent quality
EMBEDDING_DIM = 768
BATCH_SIZE = 100  # Pinecone upsert batch size

# Add this function and use it on chunk_id before upsert

def sanitize_id(chunk_id: str) -> str:
    """Make chunk ID ASCII-safe for Pinecone."""
    # Replace non-ASCII with underscore, keep alphanumeric and basic punctuation
    return ''.join(c if c.isascii() and (c.isalnum() or c in '-_') else '_' for c in chunk_id)

def load_api_key() -> str:
    """Load Pinecone API key from file."""
    if not API_KEY_FILE.exists():
        raise FileNotFoundError(f"API key file not found: {API_KEY_FILE}")
    return API_KEY_FILE.read_text().strip()


def load_chunks(limit: int = None) -> list[dict]:
    """Load chunks from JSONL file."""
    chunks = []
    with open(CHUNKS_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if limit and i >= limit:
                break
            chunks.append(json.loads(line))
    return chunks


def create_index(pc: Pinecone, index_name: str, dimension: int):
    """Create Pinecone index if it doesn't exist."""
    existing = [idx.name for idx in pc.list_indexes()]
    
    if index_name in existing:
        print(f"Index '{index_name}' already exists")
        return pc.Index(index_name)
    
    print(f"Creating index '{index_name}'...")
    pc.create_index(
        name=index_name,
        dimension=dimension,
        metric="cosine",
        spec=ServerlessSpec(
            cloud="aws",
            region="us-east-1"  # Free tier region
        )
    )
    
    # Wait for index to be ready
    print("Waiting for index to be ready...")
    while not pc.describe_index(index_name).status['ready']:
        time.sleep(1)
    
    print("Index ready!")
    return pc.Index(index_name)


def prepare_metadata(chunk: dict) -> dict:
    """Prepare metadata for Pinecone (must be flat, no nested objects)."""
    return {
        "source": chunk.get("source", "unknown"),
        "doc_type": chunk.get("doc_type", "unknown"),
        "source_file": chunk.get("source_file", ""),
        "chunk_index": chunk.get("chunk_index", 0),
        "total_chunks": chunk.get("total_chunks", 1),
        # Join lists as comma-separated strings (Pinecone metadata limitation)
        "equipment": ",".join(chunk.get("equipment", [])),
        "hazards": ",".join(chunk.get("hazards", [])),
        "operations": ",".join(chunk.get("operations", [])),
        # Store text for retrieval (Pinecone allows up to 40KB metadata)
        "text": chunk.get("text", "")[:8000],  # Truncate if very long
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest O&G chunks into Pinecone")
    parser.add_argument("--limit", type=int, default=None, help="Limit chunks for testing")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Upsert batch size")
    parser.add_argument("--delete-existing", action="store_true", help="Delete and recreate index")
    args = parser.parse_args()
    
    print("=" * 60)
    print("Pinecone Ingestion for O&G RAG")
    print("=" * 60)
    
    # Load API key
    api_key = load_api_key()
    print("✓ API key loaded")
    
    # Initialize Pinecone
    pc = Pinecone(api_key=api_key)
    print("✓ Pinecone client initialized")
    
    # Delete existing index if requested
    if args.delete_existing:
        existing = [idx.name for idx in pc.list_indexes()]
        if INDEX_NAME in existing:
            print(f"Deleting existing index '{INDEX_NAME}'...")
            pc.delete_index(INDEX_NAME)
            time.sleep(5)  # Wait for deletion
    
    # Create/get index
    index = create_index(pc, INDEX_NAME, EMBEDDING_DIM)
    
    # Check current vector count
    stats = index.describe_index_stats()
    existing_vectors = stats.total_vector_count
    print(f"Current vectors in index: {existing_vectors}")
    
    if existing_vectors > 0 and not args.delete_existing:
        response = input("Index has existing vectors. Continue adding? (y/n): ")
        if response.lower() != 'y':
            print("Aborted.")
            return
    
    # Load chunks
    print(f"\nLoading chunks from {CHUNKS_FILE}...")
    chunks = load_chunks(args.limit)
    print(f"Loaded {len(chunks)} chunks")
    
    # Load embedding model
    print(f"\nLoading embedding model: {EMBEDDING_MODEL}")
    print("(This may download ~400MB on first run)")
    model = SentenceTransformer(EMBEDDING_MODEL)
    print("✓ Model loaded")
    
    # Process in batches
    print(f"\nEmbedding and uploading in batches of {args.batch_size}...")
    
    total_uploaded = 0
    
    for i in tqdm(range(0, len(chunks), args.batch_size), desc="Processing batches"):
        batch = chunks[i:i + args.batch_size]
        
        # Get texts for embedding
        texts = [chunk["text"] for chunk in batch]
        
        # Generate embeddings
        embeddings = model.encode(texts, show_progress_bar=False)
        
        # Prepare vectors for upsert
        vectors = []
        for chunk, embedding in zip(batch, embeddings):
            vectors.append({
                "id": sanitize_id(chunk["chunk_id"]),  # <-- wrap with sanitize_id()
                "values": embedding.tolist(),
                "metadata": prepare_metadata(chunk)
            })
        
        # Upsert to Pinecone
        index.upsert(vectors=vectors)
        total_uploaded += len(vectors)
    
    # Final stats
    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    
    # Wait a moment for stats to update
    time.sleep(2)
    final_stats = index.describe_index_stats()
    
    print(f"Total vectors uploaded: {total_uploaded}")
    print(f"Total vectors in index: {final_stats.total_vector_count}")
    print(f"\nIndex name: {INDEX_NAME}")
    print(f"Embedding model: {EMBEDDING_MODEL}")
    print(f"Dimensions: {EMBEDDING_DIM}")
    
    print("\n✓ Ready for RAG queries!")


if __name__ == "__main__":
    main()
