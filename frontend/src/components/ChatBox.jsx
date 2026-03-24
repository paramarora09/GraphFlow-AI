import React, { useRef, useEffect, useState } from 'react';
import { Send, Loader2, Bot, User } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

const ChatBox = ({ messages, onSendMessage, isLoading }) => {
  const [input, setInput] = React.useState('');
  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, isLoading]);

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;
    onSendMessage(input);
    setInput('');
  };

  return (
    <div className="w-96 bg-white border-r border-gray-200 shadow-lg flex flex-col h-screen overflow-hidden z-40 flex-shrink-0">
      <div className="p-5 border-b border-gray-100 bg-gradient-to-r from-blue-600 to-indigo-600">
        <h2 className="text-lg font-bold text-white flex items-center gap-2">
          <Bot className="text-blue-100" /> GraphFlow AI
        </h2>
        <p className="text-xs text-blue-100 mt-1 opacity-90">Your Intelligent Graph Explorer</p>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4 bg-gray-50/50">
        {messages.map((msg, idx) => (
          <div key={idx} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'} animate-in fade-in slide-in-from-bottom-2 duration-300`}>
            <div className={`max-w-[85%] rounded-2xl p-3 shadow-sm ${msg.role === 'user'
                ? 'bg-blue-600 text-white rounded-tr-sm'
                : msg.isError ? 'bg-red-50 text-red-700 border border-red-100 rounded-tl-sm' : 'bg-white text-gray-800 border border-gray-100 rounded-tl-sm'
              }`}>
              {msg.role === 'assistant' && !msg.isError && (
                <div className="flex items-center gap-1.5 mb-1.5">
                  <Bot size={14} className="text-blue-500" />
                  <span className="text-[10px] uppercase tracking-wider font-semibold text-gray-400">GraphFlow Agent</span>
                </div>
              )}
              <div className="text-sm leading-relaxed markdown-content">
                {msg.role === 'assistant' ? (
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>
                    {msg.content}
                  </ReactMarkdown>
                ) : (
                  msg.content
                )}
              </div>
            </div>
          </div>
        ))}
        {isLoading && (
          <div className="flex justify-start">
            <div className="bg-white border border-gray-100 rounded-2xl rounded-tl-sm p-4 shadow-sm flex items-center gap-3">
              <Loader2 className="animate-spin text-blue-500" size={16} />
              <span className="text-sm text-gray-500 font-medium">Analyzing graph data...</span>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      <div className="p-4 bg-white border-t border-gray-100">
        <form onSubmit={handleSubmit} className="relative flex items-center">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask GraphFlow AI about an order or customer..."
            disabled={isLoading}
            className="w-full pl-4 pr-12 py-3 bg-gray-50 border border-gray-200 rounded-full focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all text-sm disabled:opacity-50"
          />
          <button
            type="submit"
            disabled={isLoading || !input.trim()}
            className="absolute right-1.5 p-2 bg-blue-600 text-white rounded-full hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:hover:bg-blue-600 flex items-center justify-center shadow-sm"
          >
            <Send size={16} className={input.trim() && !isLoading ? "translate-x-0.5 -translate-y-0.5 transition-transform" : ""} />
          </button>
        </form>
      </div>
    </div>
  );
};

export default ChatBox;
