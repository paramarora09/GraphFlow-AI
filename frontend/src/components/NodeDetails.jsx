import React from 'react';
import { X, Info } from 'lucide-react';

const NodeDetails = ({ node, onClose }) => {
  if (!node) return null;

  // Extract properties to display (excluding internal graph properties like x, y, ix, iy, vx, vy, index)
  const displayProps = Object.keys(node).reduce((acc, key) => {
    if (!['x', 'y', 'z', 'vx', 'vy', 'vz', 'index', 'id', 'labels', '__indexColor', 'fx', 'fy'].includes(key)) {
      acc[key] = node[key];
    }
    return acc;
  }, {});

  const labels = node.labels || [];

  return (
    <div className="absolute top-4 right-4 w-80 bg-white/90 backdrop-blur shadow-2xl rounded-xl border border-gray-100 overflow-hidden flex flex-col z-50 transition-all">
      <div className="flex justify-between items-center p-4 border-b border-gray-100 bg-gray-50/50">
        <div className="flex items-center gap-2">
          <Info size={16} className="text-blue-500" />
          <h3 className="font-semibold text-gray-800 text-sm">Node Details</h3>
        </div>
        <button onClick={onClose} className="text-gray-400 hover:text-gray-700 transition-colors">
          <X size={16} />
        </button>
      </div>
      
      <div className="p-4 overflow-y-auto max-h-[60vh] flex-1">
        <div className="mb-4">
          <span className="text-xs font-semibold uppercase tracking-wider text-gray-400 block mb-1">ID</span>
          <div className="text-gray-800 font-medium font-mono text-sm break-all bg-gray-50 px-2 py-1 rounded">
            {node.id}
          </div>
        </div>

        {labels.length > 0 && (
          <div className="mb-4">
            <span className="text-xs font-semibold uppercase tracking-wider text-gray-400 block mb-2">Labels</span>
            <div className="flex flex-wrap gap-1">
              {labels.map(label => (
                <span key={label} className="px-2 py-1 bg-blue-50 text-blue-600 rounded-md text-xs font-medium border border-blue-100">
                  {label}
                </span>
              ))}
            </div>
          </div>
        )}

        {Object.keys(displayProps).length > 0 && (
          <div>
            <span className="text-xs font-semibold uppercase tracking-wider text-gray-400 block mb-2">Properties</span>
            <div className="space-y-2">
              {Object.entries(displayProps).map(([key, value]) => (
                <div key={key} className="bg-gray-50 rounded-lg p-2 border border-gray-100">
                  <span className="text-xs text-gray-500 block mb-0.5">{key}</span>
                  <span className="text-sm text-gray-800 font-medium break-words">
                    {typeof value === 'object' ? JSON.stringify(value) : String(value)}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default NodeDetails;
