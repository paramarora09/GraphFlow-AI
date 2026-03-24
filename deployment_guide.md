# Deployment Guide: GraphFlow AI Live 🚀☁️

Follow these steps to host your application using **Vercel**, **Render**, and **Neo4j AuraDB**.

---

### 1. Database: Neo4j AuraDB (Free Tier)
1.  Go to [Neo4j Aura](https://neo4j.com/cloud/platform/aura-graph-database/).
2.  Create a **Free Instance**.
3.  **Download the Credentials (JSON)**: You will get a `NEO4J_URI`, `NEO4J_USER`, and `NEO4J_PASSWORD`.
4.  **Import your data**:
    - Open your Aura Console.
    - Go to **Query** (Neo4j Browser).
    - Run your Cypher load scripts (or use `export_to_graph.py` pointing to the new URI).

---

### 2. Backend: FastAPI on Render.com
1.  Create an account on [Render.com](https://render.com).
2.  Click **New +** → **Web Service**.
3.  Connect your GitHub repository.
4.  **Settings**:
    - **Language**: `Python`
    - **Build Command**: `pip install -r requirements.txt`
    - **Start Command**: `python main.py` or `uvicorn main:app --host 0.0.0.0 --port 10000`
5.  **Environment Variables**:
    - Add `GEMINI_API_KEY`
    - Add `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`
    - Add `MODEL_ID="gemini-2.0-flash-lite"`
6.  **CORS**: Note the URL Render gives you (e.g., `https://graphflow-backend.onrender.com`).

---

### 3. Frontend: React on Vercel
1.  Go to [Vercel.com](https://vercel.com).
2.  Import your GitHub repository.
3.  **Framework Preset**: Select `Vite`.
4.  **Root Directory**: Set to `frontend`.
5.  **Environment Variables**: (Optional, but recommended)
    - If you want the frontend to dynamic, we'll update the API URL in the code to point to your Render URL.
6.  **Deploy**: Hit Deploy.

---

### 4. Final Code Synchronization
I will now update your local code with these final deployment-ready changes:
1.  **CORS**: Allow your Vercel domain in the backend.
2.  **API URL**: Update the frontend to point to your live Render API.
3.  **Requirements**: Ensure all production dependencies are listed.

---
🚀 *Your application is now going live!*
