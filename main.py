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
MODEL_ID = "gemini-3.1-flash-lite-preview"

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(title="SAP O2C Graph AI Copilot")

# CORS Middleware Setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "https://graphflow-ai.vercel.app",  # Add your Vercel URL here
        "https://*.vercel.app"              # Allows all your vercel preview deployments
    ],
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
    except Exception as e:
        if "503" in str(e) or "UNAVAILABLE" in str(e).upper():
            raise RuntimeError("Gemini AI is currently overloaded. Please try again later.")
        return True

def generate_cypher_from_nl(question: str, error_feedback: str = None) -> str:
    """ Translates NL to Cypher using few-shot prompting and error feedback. """
    schema_context = """
    Nodes: Customer(id, name), Order(id, status, total_amount, order_date), Product(id, unit, category), 
           Delivery(id, status, shipping_point), Invoice(id, total_amount, issue_date, accounting_document, is_cancelled), Plant(id, name).
    Relationships: 
      (Customer)-[PLACED]->(Order), 
      (Order)-[CONTAINS]->(Product), 
      (Delivery)-[FULFILLS]->(Order),
      (Invoice)-[BILLED_TO]->(Customer), 
      (Invoice)-[BILLED_FOR]->(Product), 
      (Invoice)-[REFERENCES]->(Delivery),
      (Product)-[MANUFACTURED_AT]->(Plant).
    IMPORTANT: There is NO Payment node. There is NO DELIVERED_AS relationship.
    Note: Delivery FULFILLS Order (direction: Delivery -> Order), so to find deliveries for an order, use: MATCH (d:Delivery)-[:FULFILLS]->(o:Order {id: 'xxx'})
    """
    
    prompt = f"""You are a Neo4j Cypher expert. Convert the question into a valid Cypher query using this schema:
{schema_context}

Rules:
1. Always use 'LIMIT 100' or similar for safety.
2. IMPORTANT: Always return nodes or paths (e.g., 'RETURN path' or 'RETURN c'), even for counting questions. The NL summarizer will handle the math, but the UI needs the nodes to render the graph. 
   - Good: MATCH (c:Customer) RETURN c
   - Bad: MATCH (c:Customer) RETURN count(c)
3. Output ONLY raw Cypher. No markdown.
4. Use the exact relationship names and directions from the schema above.

Question: "{question}"
"""
    if error_feedback:
        prompt += f"\nPrevious attempt failed with error: {error_feedback}. Please fix the syntax."

    try:
        time.sleep(2)  # Safety for Free Tier
        response = client.models.generate_content(model=MODEL_ID, contents=prompt)
        cypher = response.text.replace('```cypher', '').replace('```', '').strip()
        return cypher
    except Exception as e:
        error_str = str(e).upper()
        logger.error(f"Gemini error in generate_cypher_from_nl: {error_str}")
        if "503" in error_str or "UNAVAILABLE" in error_str:
            raise RuntimeError("Gemini AI is currently overloaded. Please try again later")
        raise RuntimeError(f"Gemini API error: {e}")

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
    prompt = f"Summarize this O2C data for the question '{question}': {str(data)[:8000]}"
    try:
        time.sleep(2)  # Safety for Free Tier
        response = client.models.generate_content(model=MODEL_ID, contents=prompt)
        return response.text.strip()
    except Exception as e:
        error_str = str(e).upper()
        if "503" in error_str or "UNAVAILABLE" in error_str:
            return "Query successful, but the GraphFlow AI model is currently overloaded and cannot generate a summary. Please try again later."
        if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
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
    for label in ["Customer", "Order", "Product", "Delivery", "Invoice", "Payment", "Plant"]:
        if label.lower() in cypher.lower():
            return label
    return None

# --- API ROUTES ---

@app.post("/query", response_model=QueryResponse)
async def query_graph(request: QueryRequest):
    logger.info(f"Question: {request.question}")
    
    try:
        if not is_question_in_scope(request.question):
            return QueryResponse(question=request.question, answer="That request is outside the scope of the SAP Order-to-Cash data. Please ask something related to orders, deliveries, or customers.", generated_cypher="", data=[], message="Out of scope", retries_used=0)

        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                if attempt > 0: time.sleep(attempt * 2)
                
                cypher = generate_cypher_from_nl(request.question, last_error)
                logger.info(f"Generated Cypher: {cypher}")
                results = execute_read_query(cypher)
                logger.info(f"Neo4j returned {len(results)} records. Sample: {str(results[:1])[:200]}")
                
                # Improved detection: check if ANY value in the first row is a node/relationship list or dict
                is_graph_data = False
                if results and isinstance(results[0], dict):
                    for val in results[0].values():
                        if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict) and ("id" in val[0] or "start" in val[0]):
                            is_graph_data = True
                            break
                        if isinstance(val, dict) and ("id" in val or "identity" in val):
                            is_graph_data = True
                            break
                
                answer = summarize_results_to_nl(request.question, results)
                
                if is_graph_data:
                    return QueryResponse(question=request.question, answer=answer, generated_cypher=cypher, data=results, message="Success", retries_used=attempt)
                else:
                    graph_data = []
                    label = detect_entity_label(cypher)
                    significant_ids = []
                    for row in results:
                        if isinstance(row, dict):
                            for k, v in row.items():
                                if (isinstance(v, str) and v.isalnum() and len(v) >= 5) or ('id' in k.lower() and isinstance(v, str)):
                                    significant_ids.append(v)
                    
                    if label and significant_ids:
                        try:
                            ids_str = str(significant_ids[:10])
                            enrich_cypher = f"MATCH path = (n:{label})-[r]-(m) WHERE n.id IN {ids_str} RETURN path LIMIT 25"
                            enrich_results = execute_read_query(enrich_cypher)
                            
                            if enrich_results:
                                graph_data = enrich_results
                            else:
                                ids_str = str(significant_ids[:25])
                                node_enrich_cypher = f"MATCH (n:{label}) WHERE n.id IN {ids_str} RETURN n LIMIT 25"
                                node_results = execute_read_query(node_enrich_cypher)
                                for rec in node_results:
                                    node_data = list(rec.values())[0] if rec else {}
                                    if isinstance(node_data, dict):
                                        graph_data.append({
                                            "nodes": [{"id": node_data.get("id"), "labels": [label], "properties": node_data}],
                                            "relationships": []
                                        })
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
                    return QueryResponse(question=request.question, answer=f"I couldn't process this query. Error: {last_error}", generated_cypher="", data=[], message="Failed after retries", retries_used=attempt)

    except Exception as global_err:
        error_str = str(global_err).lower()
        logger.error(f"FATAL ERROR in /query: {global_err}", exc_info=True)
        
        friendly_msg = "I encountered an internal error. Please ensure the Neo4j database is running and try again."
        if "overloaded" in error_str or "503" in error_str:
            friendly_msg = "The GraphFlow AI model is currently experiencing high demand. Please try your request again in about 30 seconds."
            
        return QueryResponse(
            question=request.question,
            answer=friendly_msg,
            generated_cypher="",
            data=[],
            message=f"Error: {str(global_err)}",
            retries_used=0
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
