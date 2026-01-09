# 🛢️ O&G RAG Assistant
**Currently Down Because I'm not Paying for APIs for a non-production project**
**AI-powered domain expert for Oil & Gas operations, safety, and regulatory compliance**

A Retrieval-Augmented Generation system trained on 66,000+ document chunks from regulatory agencies, equipment manuals, and technical literature. Combines semantic search with LLM capabilities to provide accurate, source-cited answers to complex O&G drilling questions.

[![Live Demo](https://img.shields.io/badge/Live-Demo-brightgreen)](https://keithcockerham.github.io/og-rag)
[![Azure Functions](https://img.shields.io/badge/Azure-Functions-blue)](https://azure.microsoft.com/en-us/products/functions)
[![Pinecone](https://img.shields.io/badge/Pinecone-Vector_DB-purple)](https://www.pinecone.io/)

---

## 🎯 Project Overview

This project demonstrates end-to-end RAG implementation for a specialized technical domain:

- **Problem**: General LLMs lack deep knowledge of O&G regulations, equipment specifications, and operational procedures
- **Solution**: Domain-specific RAG system grounded in authoritative sources with full citation support
- **Result**: Expert-level answers with verifiable provenance—critical for regulatory compliance and safety applications

### Sample Queries

```
What causes high motor temperature in ESP systems?
What is the difference between Driller's Method and Wait and Weight?
What are BOP testing requirements?
What is gas lock and how do you prevent it?
Define SIDPP and explain how it's used in well control.
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        GitHub Pages                             │
│                    (Static Frontend)                            │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  Query Input  │  Settings  │  Results + Sources         │   │
│   └─────────────────────────────────────────────────────────┘   │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTPS
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Azure Functions                             │
│                    (Serverless API)                             │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│   │ /api/query   │  │ /api/health  │  │ /api/stats           │  │
│   └──────┬───────┘  └──────────────┘  └──────────────────────┘  │
└──────────┼──────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────┐      ┌──────────────────────┐
│   Pinecone           │      │   Anthropic API      │
│   Vector Database    │      │   (Claude Sonnet 4)  │
│                      │      │                      │
│   66,497 vectors     │      │   Answer Generation  │
│   768 dimensions     │      │   with Citations     │
│   BGE-base embeddings│      │                      │
└──────────────────────┘      └──────────────────────┘
```

### Query Flow

1. **User submits question** via web interface
2. **Query embedded** using BGE-base-en-v1.5 (same model used for corpus)
3. **Semantic search** retrieves top-k relevant chunks from Pinecone
4. **Context assembled** with source metadata
5. **Claude generates answer** grounded in retrieved sources
6. **Response returned** with answer + expandable source citations

---

## 📊 Corpus Statistics

| Metric | Value |
|--------|-------|
| Total Vectors | 66,497 |
| Source Documents | 1,000+ |
| Embedding Dimensions | 768 |
| Embedding Model | BAAI/bge-base-en-v1.5 |
| Avg Chunk Size | ~900 characters |

### Document Distribution

| Category | Chunks | Description |
|----------|--------|-------------|
| Investigation Reports | 19,496 | CSB incident analyses, root cause findings |
| General | 16,895 | Mixed technical content |
| Equipment Manuals | 10,685 | ESP, BOP, artificial lift systems |
| Troubleshooting | 8,159 | Diagnostic procedures, failure analysis |
| Guidance | 5,426 | Best practices, operational procedures |
| Safety Alerts | 2,191 | BSEE safety notifications |
| Advisory Bulletins | 2,050 | PHMSA pipeline safety advisories |
| Regulations | 906 | Regulatory requirements |
| Well Control | 633 | IADC procedures, kill methods |
| Glossary | 56 | Curated O&G terminology |

### Source Agencies

- **BSEE** - Bureau of Safety and Environmental Enforcement (offshore safety)
- **PHMSA** - Pipeline and Hazardous Materials Safety Administration
- **OSHA** - Occupational Safety and Health Administration
- **CSB** - Chemical Safety Board (incident investigations)
- **IADC** - International Association of Drilling Contractors

---

## 🛠️ Technology Stack

### Data Pipeline
- **Python** - Scraping, extraction, chunking
- **PyMuPDF** - PDF text extraction
- **BeautifulSoup** - Web scraping
- **Sentence Transformers** - BGE embeddings

### Vector Search
- **Pinecone** - Managed vector database
- **BGE-base-en-v1.5** - Dense passage embeddings

### LLM Integration
- **Anthropic Claude Sonnet 4** - Answer generation
- **Structured prompting** - Source-grounded responses

### Deployment
- **Azure Functions** - Serverless API backend
- **GitHub Pages** - Static frontend hosting
- **GitHub Actions** - CI/CD (optional)

---

## 🚀 Getting Started

### Prerequisites

- Python 3.10+
- Pinecone account (free tier works)
- Anthropic API key
- Azure account (for deployment)

### Local Development

```bash
# Clone repository
git clone https://github.com/keithcockerham/og-rag.git
cd og-rag

# Install dependencies
pip install -r requirements.txt

# Set up API keys
echo "your-pinecone-key" > .pinecone_api_key
echo "your-anthropic-key" > .anthropic_api_key

# Run interactive query interface
python scripts/rag_query.py
```

### Data Pipeline (if rebuilding corpus)

```bash
cd scripts

# 1. Collect documents (scrapers for each source)
python scrape_bsee.py
python scrape_phmsa.py
python scrape_csb.py
python scrape_osha.py
python scrape_technical_docs.py
python generate_og_glossary.py

# 2. Extract text from PDFs
python extract_text_v2.py --workers 20

# 3. Chunk documents
python chunk_documents.py --chunk-size 1000 --overlap 200

# 4. Build Pinecone index
python ingest_pinecone.py --delete-existing
```

### Azure Deployment

```bash
# Create Function App
az functionapp create \
  --resource-group your-rg \
  --consumption-plan-location centralus \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4 \
  --name og-rag-api \
  --storage-account your-storage

# Configure API keys
az functionapp config appsettings set \
  --name og-rag-api \
  --resource-group your-rg \
  --settings \
    PINECONE_API_KEY="xxx" \
    ANTHROPIC_API_KEY="xxx"

# Deploy
cd backend
func azure functionapp publish og-rag-api
```

---

## 📁 Project Structure

```
og-rag/
├── frontend/                 # GitHub Pages static site
│   ├── index.html           # Main query interface
│   ├── style.css            # Styling (shared with other projects)
│   ├── rag.js               # API calls and UI logic
│   ├── architecture.html    # Architecture documentation
│   └── corpus.html          # Corpus details page
│
├── backend/                  # Azure Functions API
│   ├── function_app.py      # API endpoints
│   ├── requirements.txt     # Python dependencies
│   └── host.json            # Function configuration
│
├── scripts/                  # Data pipeline
│   ├── scrape_bsee.py       # BSEE safety alerts scraper
│   ├── scrape_phmsa.py      # PHMSA advisories scraper
│   ├── scrape_csb.py        # CSB reports scraper
│   ├── scrape_osha.py       # OSHA documents scraper
│   ├── scrape_technical_docs.py  # Technical PDFs
│   ├── generate_og_glossary.py   # Curated terminology
│   ├── extract_text_v2.py   # PDF/text extraction
│   ├── chunk_documents.py   # Document chunking
│   ├── ingest_pinecone.py   # Vector index building
│   └── rag_query.py         # Local query interface
│
├── data/                     # Data directory (not in repo)
│   ├── raw/                 # Downloaded PDFs
│   └── processed/           # Extracted text and chunks
│
├── requirements.txt          # Project dependencies
└── README.md
```
---

## 🎓 Key Learnings

### RAG Implementation
- **Chunk size matters**: 1000 chars with 200 overlap balances context and precision
- **Score thresholds**: 0.7 filters noise while preserving relevant matches
- **Source diversity**: Multiple authoritative sources improve coverage and credibility

### Enterprise Considerations
- **Data governance**: Real enterprise corpora have version conflicts, outdated docs, regional variations
- **Provenance**: Source citations are essential for regulated industries
- **Evaluation**: Domain experts needed to validate answer quality

### Technical Insights
- BGE embeddings outperform ada-002 for technical content
- Curated glossaries more valuable than scraped definitions
- Many technical sources now paywalled (SPE, SLB)—requires creative sourcing

---

## 🔮 Future Enhancements

- [ ] Hybrid search (dense + sparse/BM25)
- [ ] Metadata filtering in UI (by source, doc_type, date)
- [ ] Conversation memory for follow-up questions
- [ ] Feedback collection for answer quality
- [ ] Fine-tuned reranker for improved retrieval
- [ ] Streaming responses for better UX

---

## Author

**Keith Cockerham**

- LinkedIn: [linkedin.com/in/kcockerham](https://linkedin.com/in/kcockerham)

---

## 📄 License

This project is for demonstration purposes. Source documents are from public government agencies and publicly available technical literature.

---

## 🙏 Acknowledgments

- [Anthropic](https://www.anthropic.com/) - Claude API
- [Pinecone](https://www.pinecone.io/) - Vector database
- [Hugging Face](https://huggingface.co/) - BGE embeddings
- BSEE, PHMSA, OSHA, CSB - Public safety documentation
