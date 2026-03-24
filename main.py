import os
import re
import time
import logging
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from neo4j import GraphDatabase
from google import genai
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv()

# Configuration
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL_ID = "gemini-flash-latest"

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(title="SAP O2C Graph AI Copilot")

# Enable CORS for React Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "https://graph-flow-ai.vercel.app",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Neo4j Driver
try:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    # Test connection
    with driver.session() as session:
        session.run("RETURN 1")
    logger.info("Successfully connected to Neo4j.")
except Exception as e:
    logger.error(f"Failed to connect to Neo4j: {e}")
    driver = None

# Initialize Gemini Client
if GEMINI_API_KEY:
    client = genai.Client(api_key=GEMINI_API_KEY)
else:
    logger.warning("GEMINI_API_KEY not found. LLM features will be disabled.")
    client = None

# --- MODELS ---

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    question: str
    answer: str
    generated_cypher: str
    data: List[dict]
    message: str
    retries_used: int

# --- UTILS & LLM LOGIC ---

def is_question_in_scope(question: str) -> bool:
    """ Classifies if the question is related to SAP O2C data. """
    if not client: return True
    
    prompt = f"""Classify if the following question is related to SAP Order-to-Cash (O2C) business data (customers, orders, invoices, payments, deliveries, products).
Question: "{question}"
Answer ONLY 'YES' or 'NO'."""
    try:
        response = client.models.generate_content(model=MODEL_ID, contents=prompt)
        return "YES" in response.text.upper()
    except:
        return True

def generate_cypher_from_nl(question: str, error_feedback: str = None) -> str:
    """ Translates NL to Cypher using few-shot prompting and error feedback. """
    schema_context = """
    Nodes: Customer(id, name, country), Order(id, status, total_amount, date), Product(id, name, category, price), 
           Delivery(id, status, ship_date), Invoice(id, amount, status), Payment(id, method, date, amount).
    Relationships: (Customer)-[PLACED]->(Order), (Order)-[CONTAINS]->(Product), (Order)-[DELIVERED_AS]->(Delivery),
                   (Delivery)-[BILLED_FOR]->(Invoice), (Invoice)-[PAID_BY]->(Payment).
    """
    
    prompt = f"""You are a Neo4j Cypher expert. Convert the question into a valid Cypher query using this schema:
{schema_context}

Rules:
1. Always use 'LIMIT 100' or similar for safety.
2. IMPORTANT: Always return nodes or paths (e.g., 'RETURN path' or 'RETURN c'), even for counting questions. The NL summarizer will handle the math, but the UI needs the nodes to render the graph. 
   - Good: MATCH (c:Customer) RETURN c
   - Bad: MATCH (c:Customer) RETURN count(c)
3. Output ONLY raw Cypher. No markdown.

Question: "{question}"
"""
    if error_feedback:
        prompt += f"\nPrevious attempt failed with error: {error_feedback}. Please fix the syntax."

    try:
        response = client.models.generate_content(model=MODEL_ID, contents=prompt)
        cypher = response.text.strip().replace("```cypher", "").replace("```", "")
        return cypher
    except Exception as e:
        error_str = str(e).upper()
        if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
            raise HTTPException(status_code=429, detail="Gemini API limit reached. Pls wait.")
        raise HTTPException(status_code=500, detail=f"Gemini error: {e}")

def build_direct_answer(question: str, data: list) -> Optional[str]:
    """
    Tries to build a direct NL answer from simple/tabular Neo4j results
    WITHOUT calling Gemini. Returns None if data is too complex.
    """
    if not data or not isinstance(data[0], dict):
        return None
    
    first = data[0]
    keys = list(first.keys())
    vals = list(first.values())
    
    # Case 1: Single numeric result (e.g., {"Count": 8})
    if len(keys) == 1 and isinstance(vals[0], (int, float)):
        return f"The answer is **{vals[0]}**."
    
    # Case 2: Multiple rows of simple key-value pairs (e.g., [{"Entity": "Customer", "Count": 8}, ...])
    if all(isinstance(r, dict) for r in data) and len(data) <= 20:
        # Check if it's a simple tabular result
        if all(not isinstance(v, (dict, list)) for v in vals):
            lines = []
            for row in data:
                parts = [f"{k}: {v}" for k, v in row.items()]
                lines.append(", ".join(parts))
            return "Here are the results:\n" + "\n".join(f"- {l}" for l in lines)
    
    return None

def summarize_results_to_nl(question: str, data: list) -> str:
    """ Summarizes database results into a user-friendly sentence. """
    if not data:
        return "I couldn't find any data matching your request in the O2C graph."
    
    # Step 1: Try to build a direct answer without Gemini
    direct = build_direct_answer(question, data)
    if direct:
        return direct

    # Step 2: Use Gemini for complex results
    prompt = f"Summarize this O2C data for the question '{question}': {str(data)[:1000]}"
    try:
        time.sleep(2)  # Safety for Free Tier
        response = client.models.generate_content(model=MODEL_ID, contents=prompt)
        return response.text.strip()
    except Exception as e:
        if "429" in str(e):
            return "Query successful, but I've reached my AI summary limit. Raw data is available below."
        return f"Query ran successfully. Here is the raw result: {str(data[0])}"

def execute_read_query(cypher: str) -> list:
    """ Runs Cypher and formats paths for the frontend. """
    if not driver: return []
    
    with driver.session() as session:
        try:
            result = session.run(cypher)
            records = []
            for record in result:
                data = record.data()
                # Handle path objects
                if 'path' in data:
                    path = record['path']
                    records.append({
                        "nodes": [{"id": n.get("id"), "labels": list(n.labels), "properties": dict(n)} for n in path.nodes],
                        "relationships": [{"id": r.id, "type": r.type, "start": r.start_node.get("id"), "end": r.end_node.get("id"), "properties": dict(r)} for r in path.relationships]
                    })
                else:
                    records.append(data)
            return records
        except Exception as e:
            raise RuntimeError(str(e))

def get_fallback_cypher(question: str) -> Optional[str]:
    """
    Generates fallback Cypher for flow/trace queries.
    Extracts valid Order, Invoice, Delivery, or Payment IDs only.
    Supports multiple IDs.
    """
    flow_keywords = ["trace", "flow", "identify", "show", "journey"]
    if not any(word in question.lower() for word in flow_keywords):
        return None

    ids = []

    # Step 1: Keyword-proximity extraction
    # Look for "order 740517", "invoice 90504235", etc.
    keyword_pattern = re.compile(
        r'\b(order|invoice|delivery|payment)\s+([A-Z0-9]+)\b',
        flags=re.IGNORECASE
    )
    for match in keyword_pattern.findall(question):
        ids.append(match[1].upper())  # capture the ID part

    # Step 2: Fallback regex - only alphanumeric strings with digits
    if not ids:
        candidates = re.findall(r'\b[A-Z0-9]+\b', question.upper())
        ignore_words = {
            "TRACE", "FLOW", "IDENTIFY", "SHOW", "LIST", "COUNT", "ORDER",
            "SALESORDER", "CUSTOMER", "INVOICE", "PAYMENT", "THE", "FROM",
            "FOR", "WITH", "THIS", "THAT", "JOURNEY", "OF", "SALES",
            "ORDERS", "CUSTOMERS", "INVOICES", "DELIVERY", "GIVE"
        }
        # only keep strings with at least one digit and not in ignore list
        ids = [c for c in candidates if any(ch.isdigit() for ch in c) and c not in ignore_words]

    if ids:
        logger.info(f"Fallback matched IDs: {ids}")
        ids_list_str = ",".join(f"'{i}'" for i in ids)
        return f"MATCH path = (n)-[*1..2]-(m) WHERE n.id IN [{ids_list_str}] RETURN path LIMIT 25"

    logger.warning(f"No valid O2C entity ID found in question: '{question}'")
    return None

def detect_entity_label(cypher: str) -> Optional[str]:
    """Detects the primary node label from a Cypher query."""
    for label in ["Customer", "Order", "Product", "Delivery", "Invoice", "Payment"]:
        if label.lower() in cypher.lower():
            return label
    return None

# --- API ROUTES ---

@app.post("/query", response_model=QueryResponse)
async def query_graph(request: QueryRequest):
    logger.info(f"Question: {request.question}")
    
    if not is_question_in_scope(request.question):
        return QueryResponse(question=request.question, answer="Out of scope.", generated_cypher="", data=[], message="Blocked", retries_used=0)

    max_retries = 3
    last_error = None
    
    for attempt in range(max_retries):
        try:
            if attempt > 0: time.sleep(attempt * 2)
            
            cypher = generate_cypher_from_nl(request.question, last_error)
            logger.info(f"Generated Cypher: {cypher}")
            results = execute_read_query(cypher)
            logger.info(f"Neo4j returned {len(results)} records. Sample: {str(results[:1])[:200]}")
            
            # --- Determine if results are graph-ready or tabular ---
            is_graph_data = (results and isinstance(results[0], dict) and "nodes" in results[0])
            
            # Build the NL answer from the raw results
            answer = summarize_results_to_nl(request.question, results)
            
            if is_graph_data:
                # Results are already path objects - send directly
                return QueryResponse(question=request.question, answer=answer, generated_cypher=cypher, data=results, message="Success", retries_used=attempt)
            else:
                # Results are tabular (e.g., counts, lists). 
                # Try to enrich with actual nodes for graph visualization.
                graph_data = []
                label = detect_entity_label(cypher)
                if label:
                    try:
                        enrich_cypher = f"MATCH (n:{label}) RETURN n LIMIT 25"
                        enrich_results = execute_read_query(enrich_cypher)
                        # Convert node records to graph-compatible format
                        for rec in enrich_results:
                            for key, val in rec.items():
                                if isinstance(val, dict) and "id" in val:
                                    # Neo4j node returned as dict
                                    pass
                            # Build a simple node entry
                            node_data = list(rec.values())[0] if rec else {}
                            if isinstance(node_data, dict):
                                node_id = node_data.get("id", "unknown")
                                graph_data.append({
                                    "nodes": [{"id": node_id, "labels": [label], "properties": node_data}],
                                    "relationships": []
                                })
                        logger.info(f"Enriched graph with {len(graph_data)} {label} nodes")
                    except Exception as enrich_err:
                        logger.warning(f"Enrichment failed: {enrich_err}")
                
                final_data = graph_data if graph_data else results
                return QueryResponse(question=request.question, answer=answer, generated_cypher=cypher, data=final_data, message="Success", retries_used=attempt)

        except (RuntimeError, ValueError) as e:
            last_error = str(e)
            logger.warning(f"Query attempt {attempt+1} failed: {last_error}")
            fallback = get_fallback_cypher(request.question)
            if fallback:
                res = execute_read_query(fallback)
                ans = summarize_results_to_nl(request.question, res)
                return QueryResponse(question=request.question, answer=ans, generated_cypher=fallback, data=res, message="Fallback used", retries_used=attempt)
            if attempt == max_retries - 1:
                raise HTTPException(status_code=422, detail=f"Failed: {last_error}")

    raise HTTPException(status_code=422, detail="Exceeded retries")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
