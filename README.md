# AI Knowledge Graph Builder for Enterprise Intelligence
Infosys Springboard 6.0 Internship Project

## Milestones

### Milestone 1 — Data Ingestion & Preprocessing
- Raw job postings dataset (644 records)
- Data cleaning, entity extraction, skill normalization

### Milestone 2 — Knowledge Graph Construction
- Neo4j graph with 5 node types: Job, Location, Category, Department, Skill
- 4 relationships: LOCATED_IN, BELONGS_TO, IN_DEPARTMENT, REQUIRES
- 62 countries · 232 cities · 6 job categories

### Milestone 3 — RAG-Based Semantic Search
Two RAG pipelines built and evaluated head-to-head:

| Approach | Vector Store | Avg Retrieval | Result |
|---|---|---|---|
| LangChain + FAISS | Local (RAM) | 36ms | ✅ Winner |
| LangChain + Pinecone | Cloud (AWS) | 674ms | — |

FAISS wins at current scale (644 docs).
Pinecone is the right choice at enterprise scale (50k+ docs).

## Tech Stack
Neo4j · LangChain · FAISS · Pinecone · HuggingFace · Groq Llama-3.3-70B · Python

## Note
Replace API credentials in notebooks with your own keys before running.
