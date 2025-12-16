import azure.functions as func
import logging
import json
import os

app = func.FunctionApp()


# =============================================================================
# CORS Headers Helper
# =============================================================================
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type"
}


# =============================================================================
# Health Check
# =============================================================================
@app.function_name(name="health_check")
@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Diagnostic endpoint to check imports and API keys."""
    results = {}
    
    try:
        from pinecone import Pinecone
        results["pinecone"] = "OK"
    except Exception as e:
        results["pinecone"] = f"FAIL - {e}"
    
    try:
        import anthropic
        results["anthropic"] = f"OK - {anthropic.__version__}"
    except Exception as e:
        results["anthropic"] = f"FAIL - {e}"
    
    try:
        from sentence_transformers import SentenceTransformer
        results["sentence_transformers"] = "OK"
    except Exception as e:
        results["sentence_transformers"] = f"FAIL - {e}"
    
    # Check API keys are configured (don't expose values)
    results["PINECONE_API_KEY"] = "SET" if os.environ.get("PINECONE_API_KEY") else "MISSING"
    results["ANTHROPIC_API_KEY"] = "SET" if os.environ.get("ANTHROPIC_API_KEY") else "MISSING"
    
    return func.HttpResponse(
        json.dumps(results, indent=2),
        mimetype="application/json",
        headers=CORS_HEADERS
    )


# =============================================================================
# RAG Query Endpoint
# =============================================================================
@app.function_name(name="rag_query")
@app.route(route="query", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def rag_query(req: func.HttpRequest) -> func.HttpResponse:
    """
    Main RAG query endpoint.
    
    Request body:
    {
        "query": "What causes high motor temperature in ESP systems?",
        "top_k": 10,
        "min_score": 0.7
    }
    
    Response:
    {
        "answer": "...",
        "sources": [
            {
                "text": "...",
                "source": "BSEE",
                "doc_type": "equipment_manual",
                "source_file": "...",
                "score": 0.85
            },
            ...
        ]
    }
    """
    # Handle CORS preflight
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=200, headers=CORS_HEADERS)
    
    # Parse request
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON payload"}),
            status_code=400,
            mimetype="application/json",
            headers=CORS_HEADERS
        )
    
    query = body.get("query", "").strip()
    if not query:
        return func.HttpResponse(
            json.dumps({"error": "Query is required"}),
            status_code=400,
            mimetype="application/json",
            headers=CORS_HEADERS
        )
    
    top_k = int(body.get("top_k", 10))
    min_score = float(body.get("min_score", 0.7))
    
    logging.info(f"RAG query: '{query[:50]}...' top_k={top_k} min_score={min_score}")
    
    try:
        # Initialize clients
        from pinecone import Pinecone
        from sentence_transformers import SentenceTransformer
        import anthropic
        
        pinecone_key = os.environ.get("PINECONE_API_KEY")
        anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
        
        if not pinecone_key or not anthropic_key:
            return func.HttpResponse(
                json.dumps({"error": "API keys not configured"}),
                status_code=500,
                mimetype="application/json",
                headers=CORS_HEADERS
            )
        
        # Connect to Pinecone
        pc = Pinecone(api_key=pinecone_key)
        index = pc.Index("og-rag")
        
        # Load embedding model (cached after first call in warm instance)
        embed_model = SentenceTransformer("BAAI/bge-base-en-v1.5")
        
        # Embed query
        query_embedding = embed_model.encode(query).tolist()
        
        # Search Pinecone
        results = index.query(
            vector=query_embedding,
            top_k=top_k,
            include_metadata=True
        )
        
        # Filter by min_score and extract contexts
        contexts = []
        for match in results.matches:
            if match.score >= min_score:
                contexts.append({
                    "text": match.metadata.get("text", ""),
                    "source": match.metadata.get("source", "unknown").upper(),
                    "doc_type": match.metadata.get("doc_type", "unknown"),
                    "source_file": match.metadata.get("source_file", ""),
                    "score": match.score
                })
        
        if not contexts:
            return func.HttpResponse(
                json.dumps({
                    "answer": "I couldn't find any relevant information in the knowledge base for this query. Try rephrasing your question or lowering the similarity threshold.",
                    "sources": []
                }),
                mimetype="application/json",
                headers=CORS_HEADERS
            )
        
        # Build prompt for Claude
        context_text = format_context_for_prompt(contexts)
        
        system_prompt = """You are an Oil & Gas domain expert assistant with deep knowledge of:
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

        user_message = f"""Based on the following sources from the O&G knowledge base, please answer the question.

<knowledge_base>
{context_text}
</knowledge_base>

<question>
{query}
</question>

Provide a clear, accurate answer based on the sources above. Reference specific sources when making claims."""

        # Call Claude
        client = anthropic.Anthropic(api_key=anthropic_key)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}]
        )
        
        answer = response.content[0].text
        
        return func.HttpResponse(
            json.dumps({
                "answer": answer,
                "sources": contexts
            }),
            mimetype="application/json",
            headers=CORS_HEADERS
        )
        
    except Exception as e:
        logging.error(f"RAG query error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json",
            headers=CORS_HEADERS
        )


def format_context_for_prompt(contexts: list[dict]) -> str:
    """Format retrieved contexts for the LLM prompt."""
    formatted = []
    for i, ctx in enumerate(contexts, 1):
        formatted.append(f"""<source id="{i}" origin="{ctx['source']}" type="{ctx['doc_type']}" file="{ctx['source_file']}">
{ctx['text']}
</source>""")
    
    return "\n\n".join(formatted)


# =============================================================================
# Corpus Stats Endpoint
# =============================================================================
@app.function_name(name="corpus_stats")
@app.route(route="stats", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def corpus_stats(req: func.HttpRequest) -> func.HttpResponse:
    """Return corpus statistics from Pinecone index."""
    try:
        from pinecone import Pinecone
        
        pinecone_key = os.environ.get("PINECONE_API_KEY")
        if not pinecone_key:
            return func.HttpResponse(
                json.dumps({"error": "Pinecone API key not configured"}),
                status_code=500,
                mimetype="application/json",
                headers=CORS_HEADERS
            )
        
        pc = Pinecone(api_key=pinecone_key)
        index = pc.Index("og-rag")
        stats = index.describe_index_stats()
        
        return func.HttpResponse(
            json.dumps({
                "total_vectors": stats.total_vector_count,
                "dimension": stats.dimension,
                "index_name": "og-rag"
            }),
            mimetype="application/json",
            headers=CORS_HEADERS
        )
        
    except Exception as e:
        logging.error(f"Stats error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json",
            headers=CORS_HEADERS
        )
