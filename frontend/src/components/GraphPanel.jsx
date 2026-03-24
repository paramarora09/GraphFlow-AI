import React, { useRef, useEffect, useState, useCallback } from 'react';
import ForceGraph2D from 'react-force-graph-2d';

// Color palette for different node labels
const nodeColors = {
  Customer: '#3b82f6', // blue-500
  Order: '#8b5cf6', // violet-500
  Payment: '#10b981', // emerald-500
  Invoice: '#f59e0b', // amber-500
  Delivery: '#ec4899', // pink-500
  Product: '#06b6d4', // cyan-500
  Material: '#6366f1', // indigo-500
  default: '#94a3b8' // slate-400
};

const GraphPanel = ({ graphData, onNodeClick }) => {
  const fgRef = useRef();
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });
  const containerRef = useRef();

  // Resize graph to fill container
  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      setDimensions({ width, height });
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  // Zoom to fit when data changes
  useEffect(() => {
    if (graphData && graphData.nodes.length > 0 && fgRef.current) {
      setTimeout(() => {
        fgRef.current.zoomToFit(400, 50); // ms transition, padding
      }, 100);
    }
  }, [graphData]);

  const getNodeColor = (node) => {
    const label = node.labels && node.labels.length > 0 ? node.labels[0] : 'default';
    return nodeColors[label] || nodeColors.default;
  };

  const drawNode = useCallback((node, ctx, globalScale) => {
    const label = node.id || '';
    const fontSize = 12 / globalScale;
    ctx.font = `${fontSize}px Sans-Serif`;
    
    // Draw circle
    const r = 4;
    ctx.beginPath();
    ctx.arc(node.x, node.y, r, 0, 2 * Math.PI, false);
    ctx.fillStyle = getNodeColor(node);
    ctx.fill();

    // Draw text
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    ctx.fillStyle = '#1e293b'; // slate-800
    ctx.fillText(label, node.x, node.y + r + 2);
  }, []);

  if (!graphData || (graphData.nodes.length === 0 && graphData.links.length === 0)) {
    return (
      <div className="flex-1 flex items-center justify-center bg-gray-50 bg-[radial-gradient(#e5e7eb_1px,transparent_1px)] [background-size:20px_20px]">
        <div className="text-center">
          <div className="text-gray-400 mb-2">
            <svg className="w-16 h-16 mx-auto opacity-50" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z" />
            </svg>
          </div>
          <h2 className="text-xl font-medium text-gray-600">No Graph Data</h2>
          <p className="text-sm text-gray-500 mt-1 max-w-sm mx-auto">Ask a question involving paths or traces to visualize the Order-to-Cash data flow.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-hidden relative bg-gray-50 bg-[radial-gradient(#e5e7eb_1px,transparent_1px)] [background-size:20px_20px]" ref={containerRef}>
      <ForceGraph2D
        ref={fgRef}
        width={dimensions.width}
        height={dimensions.height}
        graphData={graphData}
        nodeLabel="id"
        nodeColor={getNodeColor}
        nodeCanvasObject={drawNode}
        onNodeClick={onNodeClick}
        linkLabel={link => link.type || ''}
        linkDirectionalArrowLength={3.5}
        linkDirectionalArrowRelPos={1}
        linkColor={() => '#cbd5e1'}
        linkWidth={1.5}
        d3VelocityDecay={0.3}
      />
      
      {/* Legend overlay */}
      <div className="absolute bottom-4 left-4 bg-white/90 backdrop-blur p-3 rounded-lg border border-gray-100 shadow-sm pointer-events-none text-xs">
        <div className="font-semibold text-gray-700 mb-2">Entity Legend</div>
        <div className="grid grid-cols-2 gap-x-4 gap-y-1.5">
          {Object.entries(nodeColors).filter(([k]) => k !== 'default').map(([label, color]) => (
            <div key={label} className="flex items-center gap-2">
              <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: color }}></div>
              <span className="text-gray-600 font-medium">{label}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default GraphPanel;
