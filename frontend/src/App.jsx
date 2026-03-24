import React, { useState, useEffect } from 'react';
import axios from 'axios';
import ChatBox from './components/ChatBox';
import GraphPanel from './components/GraphPanel';
import NodeDetails from './components/NodeDetails';

const API_URL = "https://graphflow-ai.onrender.com/query";

function App() {
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      content: 'Hello! I am your SAP Order-to-Cash AI Copilot. Ask me about orders, customers, deliveries, or trace specific fulfillment flows.',
    }
  ]);
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [selectedNode, setSelectedNode] = useState(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    document.title = "GraphFlow AI";
  }, []);

  // Parse path arrays into ForceGraph structure
  const parseGraphData = (dataArray) => {
    const nodesMap = new Map();
    const linksMap = new Map();

    dataArray.forEach(record => {
      // Check if this record is a path structure
      if (record.nodes && record.relationships) {
        record.nodes.forEach(n => {
          if (!nodesMap.has(n.id)) {
            nodesMap.set(n.id, { ...n.properties, id: n.id, labels: n.labels });
          }
        });
        record.relationships.forEach(rel => {
          const linkId = `${rel.start}-${rel.type}-${rel.end}`;
          if (!linksMap.has(linkId)) {
            linksMap.set(linkId, {
              id: rel.id,
              source: rel.start,
              target: rel.end,
              type: rel.type,
              ...rel.properties
            });
          }
        });
      }
    });

    return {
      nodes: Array.from(nodesMap.values()),
      links: Array.from(linksMap.values())
    };
  };

  const handleSendMessage = async (text) => {
    const userMsg = { role: 'user', content: text, timestamp: new Date().toISOString() };
    setMessages(prev => [...prev, userMsg]);
    setIsLoading(true);

    try {
      const response = await axios.post(API_URL, { question: text });
      const { answer, generated_cypher, data } = response.data;

      const systemMsg = {
        role: 'assistant',
        content: answer,
        cypher: generated_cypher,
        timestamp: new Date().toISOString(),
      };

      setMessages(prev => [...prev, systemMsg]);

      // If data looks like graph paths, update the visualization
      if (data && data.length > 0 && data[0].nodes && data[0].relationships) {
        const newGraphData = parseGraphData(data);
        setGraphData(newGraphData);

        // Auto-select node if mentioned in the answer
        const idMatch = answer.match(/\b([A-Z0-9]{5,})\b/);
        if (idMatch) {
          const mentionedId = idMatch[1];
          const nodeToSelect = newGraphData.nodes.find(n => n.id === mentionedId);
          if (nodeToSelect) {
            setSelectedNode(nodeToSelect);
          } else {
            setSelectedNode(null); // Clear if no match
          }
        } else {
          setSelectedNode(null); // Clear if no ID mentioned
        }
      }

    } catch (error) {
      console.error("API Error (Hidden from User):", error);

      let userFriendlyMessage = "I encountered an unexpected issue while processing your request. Please try again in a moment.";

      if (error.response) {
        const status = error.response.status;
        const detail = error.response.data?.detail || "";

        if (status === 429 || detail.includes("RESOURCE_EXHAUSTED")) {
          userFriendlyMessage = "I'm currently receiving too many requests. Please wait a few seconds and try again.";
        } else if (status === 404) {
          userFriendlyMessage = "I couldn't find the data you're looking for. Try a broader search.";
        } else if (status === 422) {
          userFriendlyMessage = "I'm having trouble interpreting that specific query. Could you rephrase it?";
        }
      } else if (error.request) {
        userFriendlyMessage = "I'm having trouble connecting to the backend. Please check if the server is running.";
      }

      setMessages(prev => [...prev, {
        role: 'assistant',
        content: userFriendlyMessage,
        isError: true,
      }]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="flex h-screen w-full bg-white font-sans text-gray-900 overflow-hidden">
      <ChatBox
        messages={messages}
        onSendMessage={handleSendMessage}
        isLoading={isLoading}
      />

      <div className="flex-1 relative flex flex-col">
        <GraphPanel
          graphData={graphData}
          onNodeClick={(node) => setSelectedNode(node)}
        />

        {selectedNode && (
          <NodeDetails
            node={selectedNode}
            onClose={() => setSelectedNode(null)}
          />
        )}
      </div>
    </div>
  );
}

export default App;
