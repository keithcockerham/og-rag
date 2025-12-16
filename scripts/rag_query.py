#!/usr/bin/env python3
"""
O&G RAG Assistant - Query Interface
Retrieves relevant chunks from Pinecone and generates answers with Claude
"""
import os
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

import tensorflow as tf
tf.get_logger().setLevel("ERROR")
from pathlib import Path
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer
import anthropic
import argparse
import textwrap


os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
# Paths
PROJECT_ROOT = Path(__file__).parent.parent
PINECONE_KEY_FILE = PROJECT_ROOT / ".pinecone_api_key"
ANTHROPIC_KEY_FILE = PROJECT_ROOT / ".anthropic_api_key"

# Settings
INDEX_NAME = "og-rag"
EMBEDDING_MODEL = "BAAI/bge-base-en-v1.5"
CLAUDE_MODEL = "claude-sonnet-4-20250514"  # Best quality/cost ratio
MAX_TOKENS = 1024


SYSTEM_PROMPT = """You are an Oil & Gas domain expert assistant with deep knowledge of:
- Offshore and onshore safety regulations (BSEE, PHMSA, OSHA)
- Equipment operations (BOPs, ESPs, compressors, pipelines, separators)
- Incident investigation and root cause analysis
- HSE compliance and best practices

You have access to a knowledge base of regulatory documents, safety alerts, investigation reports, and technical guidance.

When answering questions:
1. Base your answers on the provided context from the knowledge base
2. Cite specific sources when possible (e.g., "According to BSEE Safety Alert...")
3. If the context doesn't contain enough information, say so clearly
4. Provide actionable, practical guidance when appropriate
5. Use industry-standard terminology

If asked about something outside the O&G domain or not covered in the context, acknowledge the limitation."""


def load_keys() -> tuple[str, str]:
    """Load API keys from files."""
    pinecone_key = PINECONE_KEY_FILE.read_text().strip()
    anthropic_key = ANTHROPIC_KEY_FILE.read_text().strip()
    return pinecone_key, anthropic_key


def retrieve_context(index, model, query: str, top_k: int = 5, filter_dict: dict = None, 
                     min_score: float = None) -> list[dict]:
    """Retrieve relevant chunks from Pinecone."""
    query_embedding = model.encode(query).tolist()
    
    results = index.query(
        vector=query_embedding,
        top_k=top_k,
        include_metadata=True,
        filter=filter_dict
    )
    
    contexts = []
    for match in results.matches:
        # Skip if below score threshold
        if min_score is not None and match.score < min_score:
            continue
        contexts.append({
            "text": match.metadata.get("text", ""),
            "source": match.metadata.get("source", "unknown").upper(),
            "doc_type": match.metadata.get("doc_type", "unknown"),
            "source_file": match.metadata.get("source_file", ""),
            "score": match.score
        })
    
    return contexts


def format_context_for_prompt(contexts: list[dict]) -> str:
    """Format retrieved contexts for the LLM prompt."""
    formatted = []
    for i, ctx in enumerate(contexts, 1):
        formatted.append(f"""<source id="{i}" origin="{ctx['source']}" type="{ctx['doc_type']}" file="{ctx['source_file']}">
{ctx['text']}
</source>""")
    
    return "\n\n".join(formatted)


def generate_answer(client: anthropic.Anthropic, query: str, contexts: list[dict]) -> str:
    """Generate answer using Claude."""
    context_text = format_context_for_prompt(contexts)
    
    user_message = f"""Based on the following sources from the O&G knowledge base, please answer the question.

<knowledge_base>
{context_text}
</knowledge_base>

<question>
{query}
</question>

Provide a clear, accurate answer based on the sources above. Reference specific sources when making claims."""

    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[
            {"role": "user", "content": user_message}
        ]
    )
    
    return response.content[0].text


def print_sources(contexts: list[dict]):
    """Print retrieved sources for transparency."""
    print("\n" + "─" * 60)
    print("SOURCES RETRIEVED:")
    print("─" * 60)
    for i, ctx in enumerate(contexts, 1):
        print(f"[{i}] {ctx['source']} | {ctx['doc_type']} | Score: {ctx['score']:.3f}")
        print(f"    File: {ctx['source_file']}")
    print("─" * 60)


def interactive_mode(index, embed_model, claude_client):
    """Run interactive query session."""
    print("\n" + "=" * 60)
    print("O&G RAG Assistant")
    print("=" * 60)
    print("Ask questions about oil & gas safety, regulations, and operations.")
    print("Commands: /quit, /sources on|off, /topk N, /minscore N, /filter source=X")
    print("=" * 60)
    
    show_sources = True
    top_k = 5
    min_score = None
    current_filter = None
    
    while True:
        try:
            query = input("\n🛢️  Question> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break
        
        if not query:
            continue
        
        if query == "/quit":
            print("Goodbye!")
            break
        
        if query.startswith("/sources"):
            parts = query.split()
            if len(parts) > 1:
                show_sources = parts[1].lower() == "on"
            print(f"Show sources: {show_sources}")
            continue
        
        if query.startswith("/topk"):
            parts = query.split()
            if len(parts) > 1:
                top_k = int(parts[1])
            print(f"Top-K set to: {top_k}")
            continue
        
        if query.startswith("/minscore"):
            parts = query.split()
            if len(parts) > 1:
                val = parts[1].lower()
                if val == "off" or val == "none":
                    min_score = None
                    print("Min score threshold disabled")
                else:
                    min_score = float(val)
                    print(f"Min score set to: {min_score}")
            else:
                print(f"Current min score: {min_score}")
            continue
        
        if query.startswith("/filter"):
            parts = query.split(" ", 1)
            if len(parts) > 1 and "=" in parts[1]:
                field, value = parts[1].split("=")
                current_filter = {field.strip(): value.strip()}
                print(f"Filter set: {current_filter}")
            else:
                current_filter = None
                print("Filter cleared")
            continue
        
        # Retrieve and generate
        print("\n🔍 Searching knowledge base...")
        contexts = retrieve_context(index, embed_model, query, top_k, current_filter, min_score)
        
        if not contexts:
            print("No relevant documents found.")
            continue
        
        if show_sources:
            print_sources(contexts)
        
        print("\n💭 Generating answer...\n")
        answer = generate_answer(claude_client, query, contexts)
        
        # Print with word wrap
        print("─" * 60)
        for line in answer.split('\n'):
            if line.strip():
                wrapped = textwrap.fill(line, width=80)
                print(wrapped)
            else:
                print()
        print("─" * 60)


def single_query(index, embed_model, claude_client, query: str, top_k: int = 5, 
                 filter_dict: dict = None, show_sources: bool = True, min_score: float = None):
    """Run a single query and print results."""
    print(f"\n🔍 Query: {query}\n")
    
    contexts = retrieve_context(index, embed_model, query, top_k, filter_dict, min_score)
    
    if not contexts:
        print("No relevant documents found.")
        return
    
    if show_sources:
        print_sources(contexts)
    
    print("\n💭 Generating answer...\n")
    answer = generate_answer(claude_client, query, contexts)
    
    print("=" * 60)
    print("ANSWER:")
    print("=" * 60)
    for line in answer.split('\n'):
        if line.strip():
            wrapped = textwrap.fill(line, width=80)
            print(wrapped)
        else:
            print()
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="O&G RAG Assistant")
    parser.add_argument("--query", "-q", type=str, help="Single query (non-interactive)")
    parser.add_argument("--top-k", "-k", type=int, default=5, help="Number of chunks to retrieve")
    parser.add_argument("--min-score", "-s", type=float, default=None, help="Minimum similarity score threshold")
    parser.add_argument("--source", type=str, help="Filter by source (bsee, phmsa, osha, csb)")
    parser.add_argument("--no-sources", action="store_true", help="Hide source citations")
    parser.add_argument("--model", type=str, default=CLAUDE_MODEL, help="Claude model to use")
    args = parser.parse_args()
    
    print("Loading API keys and models...")
    
    # Initialize
    pinecone_key, anthropic_key = load_keys()
    
    pc = Pinecone(api_key=pinecone_key)
    index = pc.Index(INDEX_NAME)
    
    print(f"Loading embedding model: {EMBEDDING_MODEL}")
    embed_model = SentenceTransformer(EMBEDDING_MODEL)
    
    claude_client = anthropic.Anthropic(api_key=anthropic_key)
    
    # Check index
    stats = index.describe_index_stats()
    print(f"Connected to Pinecone index with {stats.total_vector_count:,} vectors")
    print(f"Using Claude model: {args.model}")
    
    # Build filter
    filter_dict = None
    if args.source:
        filter_dict = {"source": args.source}
    
    # Run query or interactive mode
    if args.query:
        single_query(index, embed_model, claude_client, args.query, 
                    args.top_k, filter_dict, not args.no_sources, args.min_score)
    else:
        interactive_mode(index, embed_model, claude_client)


if __name__ == "__main__":
    main()
