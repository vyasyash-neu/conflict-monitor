import os
from fastapi import APIRouter
from pydantic import BaseModel
from groq import Groq
from services.vectorizer import embed_query, get_qdrant

router = APIRouter()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
COLLECTION = "conflict_events"

class RAGQuery(BaseModel):
    question: str

class RAGResponse(BaseModel):
    answer: str
    sources: list
    events_used: int

@router.post("/query", response_model=RAGResponse)
def rag_query(body: RAGQuery):
    """Answer natural language questions about the conflict using RAG."""
    # Step 1: Embed the question
    q_embedding = embed_query(body.question)

    # Step 2: Search Qdrant for relevant events
    qdrant = get_qdrant()
    results = qdrant.search(
        collection_name=COLLECTION,
        query_vector=q_embedding,
        limit=10,
    )

    # Step 3: Build context from retrieved events
    context_parts = []
    sources = []
    for r in results:
        p = r.payload
        entry = f"- [{p.get('category', 'unknown')}] {p.get('summary', 'No summary')} (Location: {p.get('location_name', 'Unknown')}, Country: {p.get('country', 'Unknown')}, Severity: {p.get('severity', '?')}/10, Confidence: {p.get('confidence_score', 0):.0%})"
        context_parts.append(entry)
        sources.append({
            "event_id": p.get("event_id", ""),
            "summary": p.get("summary", ""),
            "category": p.get("category", ""),
            "confidence": p.get("confidence_score", 0),
            "relevance_score": r.score,
        })

    context = "\n".join(context_parts)

    # Step 4: Generate answer with Groq
    try:
        client = Groq(api_key=GROQ_API_KEY)
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": """You are an analyst for a real-time conflict monitoring platform tracking the 2026 US-Iran war.
Answer questions based ONLY on the provided event data. Be factual, cite specific events when possible.
If the data doesn't contain enough information, say so honestly.
Include confidence levels when referencing events. Be concise but thorough."""
                },
                {
                    "role": "user",
                    "content": f"""Based on these monitored conflict events:\n\n{context}\n\nQuestion: {body.question}\n\nProvide a factual analysis based on the event data above."""
                },
            ],
            temperature=0.2,
            max_tokens=800,
        )
        answer = resp.choices[0].message.content
    except Exception as e:
        answer = f"AI analysis temporarily unavailable (rate limit). Here are the {len(sources)} most relevant events found via vector search:\n\n"
        for s in sources[:5]:
            answer += f"• [{s['category']}] {s['summary']} (confidence: {s['confidence']:.0%})\n"

    return RAGResponse(
        answer=answer,
        sources=sources,
        events_used=len(results),
    )