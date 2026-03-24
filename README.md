# GraphFlow AI: SAP O2C Graph Explorer 🕸️🚀

GraphFlow AI is an intelligent Graph Explorer designed to visualize and analyze SAP Order-to-Cash (O2C) business data. It uses a Knowledge Graph (Neo4j) and LLMs (Gemini) to translate natural language questions into insightful visualizations.

## 🌟 Features
- **Natural Language to Cypher**: Ask complex business questions in plain English.
- **Dynamic Graph Visualization**: Explore relationships between Customers, Orders, Deliveries, and Invoices.
- **Intelligent Trace**: Visualize full fulfillment flows from a single order ID.
- **Rich Chat UX**: Formatted summaries with Markdown, tables, and bold highlights.

## 🛠️ Tech Stack
- **Backend**: FastAPI, Neo4j, Google Gemini API
- **Frontend**: React, Vite, Tailwind CSS, React Force Graph, React Markdown

## 🚀 Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/graphflow-ai.git
cd graphflow-ai
```

### 2. Backend Setup
1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
2. **Configure Environment**:
   - Copy `.env.example` to `.env`.
   - Add your **Neo4j** credentials and **Gemini API Key**.
3. **Run the Server**:
   ```bash
   python main.py
   ```

### 3. Frontend Setup
1. **Navigate to Frontend**:
   ```bash
   cd frontend
   ```
2. **Install Packages**:
   ```bash
   npm install
   ```
3. **Start Development Server**:
   ```bash
   npm run dev
   ```

## 🔒 Security & Secrets
- **DO NOT** commit your `.env` file. It is already included in `.gitignore`.
- Always use the `.env.example` template for sharing configuration structures.
- For production hosting (e.g., Vercel, Render), set these variables in the platform's Environment Variables settings.

## 📊 Data Schema
- **Nodes**: `Customer`, `Order`, `Product`, `Delivery`, `Invoice`, `Plant`.
- **Key Flows**: `(Customer)-[:PLACED]->(Order)-[:CONTAINS]->(Product)`.

---
Built with ❤️ by GraphFlow AI
