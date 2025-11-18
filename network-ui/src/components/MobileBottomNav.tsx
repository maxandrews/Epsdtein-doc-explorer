import { useState } from 'react';
import { searchActors } from '../api';
import type { Actor, Stats, TagCluster, Relationship } from '../types';
import DocumentModal from './DocumentModal';

interface MobileBottomNavProps {
  stats: Stats | null;
  selectedActor: string | null;
  onActorSelect: (actor: string | null) => void;
  limit: number;
  onLimitChange: (limit: number) => void;
  tagClusters: TagCluster[];
  enabledClusterIds: Set<number>;
  onToggleCluster: (clusterId: number) => void;
  enabledCategories: Set<string>;
  onToggleCategory: (category: string) => void;
  relationships: Relationship[];
}

type Tab = 'search' | 'timeline' | 'filters';

export default function MobileBottomNav({
  stats,
  selectedActor,
  onActorSelect,
  limit,
  onLimitChange,
  tagClusters,
  enabledClusterIds,
  onToggleCluster,
  enabledCategories,
  onToggleCategory,
  relationships
}: MobileBottomNavProps) {
  const [activeTab, setActiveTab] = useState<Tab | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<Actor[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [documentToView, setDocumentToView] = useState<string | null>(null);

  const handleSearch = async (query: string) => {
    setSearchQuery(query);

    if (query.trim().length < 2) {
      setSearchResults([]);
      return;
    }

    setIsSearching(true);
    try {
      const results = await searchActors(query);
      setSearchResults(results);
    } catch (error) {
      console.error('Search error:', error);
      setSearchResults([]);
    } finally {
      setIsSearching(false);
    }
  };

  const handleActorClick = (actorName: string) => {
    onActorSelect(actorName);
    setSearchQuery('');
    setSearchResults([]);
    setActiveTab(null);
  };

  return (
    <>
      {/* Expanded Panel */}
      {activeTab && (
        <div className="fixed inset-x-0 bottom-16 bg-gray-800 border-t border-gray-700 max-h-[70vh] overflow-y-auto z-40">
          {activeTab === 'timeline' && (
            <div className="p-4">
              {/* Close button in upper right */}
              <button
                onClick={() => setActiveTab(null)}
                className="absolute top-4 right-4 p-2 text-gray-400 hover:text-white hover:bg-gray-700 rounded-lg transition-colors"
                aria-label="Close"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>

              <h3 className="text-lg font-semibold mb-4">Timeline View</h3>
              {relationships.length > 0 ? (
                <div className="space-y-2 max-h-[60vh] overflow-y-auto">
                  {relationships.slice(0, 50).map((rel, idx) => (
                    <button
                      key={idx}
                      onClick={() => setDocumentToView(rel.doc_id)}
                      className="w-full bg-gray-700 hover:bg-gray-600 rounded-lg p-3 text-left transition-colors"
                    >
                      <div className="flex justify-between items-start mb-2">
                        <div className="font-medium text-sm">{rel.actor}</div>
                        {rel.timestamp && (
                          <div className="text-xs text-gray-400">{rel.timestamp}</div>
                        )}
                      </div>
                      <div className="text-xs text-gray-300 mb-1">{rel.action}</div>
                      <div className="text-sm text-blue-400">{rel.target}</div>
                      {rel.location && (
                        <div className="text-xs text-purple-400 mt-1">{rel.location}</div>
                      )}
                      <div className="text-xs text-gray-500 mt-2">{rel.doc_id}</div>
                    </button>
                  ))}
                </div>
              ) : (
                <div className="text-center py-8 text-gray-400">No relationships to display</div>
              )}
            </div>
          )}

          {activeTab === 'search' && (
            <div className="p-4">
              <div className="mb-4">
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => handleSearch(e.target.value)}
                  placeholder="Search actors..."
                  className="w-full px-4 py-3 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:border-blue-500"
                  autoFocus
                />
              </div>

              {selectedActor && (
                <div className="mb-4 bg-blue-900/30 border border-blue-700/50 rounded-lg p-3">
                  <div className="flex items-center justify-between">
                    <div>
                      <div className="text-xs text-gray-400 mb-1">Selected:</div>
                      <div className="font-medium text-blue-300">{selectedActor}</div>
                    </div>
                    <button
                      onClick={() => onActorSelect(null)}
                      className="px-3 py-1 bg-red-600 hover:bg-red-700 rounded text-sm"
                    >
                      Clear
                    </button>
                  </div>
                </div>
              )}

              {searchQuery.trim().length >= 2 && (
                <div className="space-y-2">
                  {isSearching ? (
                    <div className="text-center py-4 text-gray-400">Searching...</div>
                  ) : searchResults.length > 0 ? (
                    searchResults.map((actor) => (
                      <button
                        key={actor.name}
                        onClick={() => handleActorClick(actor.name)}
                        className="w-full p-3 bg-gray-700 hover:bg-gray-600 rounded-lg text-left"
                      >
                        <div className="font-medium">{actor.name}</div>
                        <div className="text-xs text-gray-400">
                          {actor.connection_count} relationships
                        </div>
                      </button>
                    ))
                  ) : (
                    <div className="text-center py-4 text-gray-400">No actors found</div>
                  )}
                </div>
              )}
            </div>
          )}


          {activeTab === 'filters' && (
            <div className="p-4">
              {/* Close button in upper right */}
              <button
                onClick={() => setActiveTab(null)}
                className="absolute top-4 right-4 p-2 text-gray-400 hover:text-white hover:bg-gray-700 rounded-lg transition-colors"
                aria-label="Close"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>

              <h3 className="text-lg font-semibold mb-4">Filters</h3>

              <div className="mb-6">
                <label className="block text-sm text-gray-400 mb-2">
                  Relationships: {limit.toLocaleString()}
                </label>
                <input
                  type="range"
                  min="100"
                  max="5000"
                  step="100"
                  value={limit}
                  onChange={(e) => onLimitChange(parseInt(e.target.value))}
                  className="w-full h-2 bg-gray-700 rounded-lg appearance-none cursor-pointer"
                />
              </div>

              <div className="mb-4">
                <div className="flex items-center justify-between mb-3">
                  <h4 className="font-semibold">Content Filters</h4>
                  <div className="flex gap-2">
                    <button
                      onClick={() => {
                        tagClusters.forEach(cluster => {
                          if (!enabledClusterIds.has(cluster.id)) {
                            onToggleCluster(cluster.id);
                          }
                        });
                      }}
                      className="px-2 py-1 bg-gray-700 hover:bg-gray-600 text-xs rounded"
                    >
                      All
                    </button>
                    <button
                      onClick={() => {
                        tagClusters.forEach(cluster => {
                          if (enabledClusterIds.has(cluster.id)) {
                            onToggleCluster(cluster.id);
                          }
                        });
                      }}
                      className="px-2 py-1 bg-gray-700 hover:bg-gray-600 text-xs rounded"
                    >
                      None
                    </button>
                  </div>
                </div>
                <div className="flex flex-wrap gap-2">
                  {tagClusters.map((cluster) => {
                    const isEnabled = enabledClusterIds.has(cluster.id);
                    return (
                      <button
                        key={cluster.id}
                        onClick={() => onToggleCluster(cluster.id)}
                        className={`px-3 py-1.5 rounded-full text-xs font-medium ${
                          isEnabled
                            ? 'bg-blue-600 text-white'
                            : 'bg-gray-700 text-gray-400'
                        }`}
                      >
                        {cluster.name}
                      </button>
                    );
                  })}
                </div>
              </div>

              {stats && (
                <div className="mb-6">
                  <div className="flex items-center justify-between mb-3">
                    <h4 className="font-semibold">Document Categories</h4>
                    <div className="flex gap-2">
                      <button
                        onClick={() => {
                          stats.categories.forEach(cat => {
                            if (!enabledCategories.has(cat.category)) {
                              onToggleCategory(cat.category);
                            }
                          });
                        }}
                        className="px-2 py-1 bg-gray-700 hover:bg-gray-600 text-xs rounded"
                      >
                        All
                      </button>
                      <button
                        onClick={() => {
                          stats.categories.forEach(cat => {
                            if (enabledCategories.has(cat.category)) {
                              onToggleCategory(cat.category);
                            }
                          });
                        }}
                        className="px-2 py-1 bg-gray-700 hover:bg-gray-600 text-xs rounded"
                      >
                        None
                      </button>
                    </div>
                  </div>
                  <div className="space-y-2">
                    {stats.categories.slice(0, 10).map((cat) => {
                      const isEnabled = enabledCategories.has(cat.category);
                      return (
                        <button
                          key={cat.category}
                          onClick={() => onToggleCategory(cat.category)}
                          className={`w-full flex justify-between items-center rounded px-3 py-2 text-sm transition-colors ${
                            isEnabled
                              ? 'bg-blue-600 text-white'
                              : 'bg-gray-700 text-gray-400'
                          }`}
                        >
                          <span className="capitalize">
                            {cat.category.replace(/_/g, ' ')}
                          </span>
                          <span className="font-mono text-xs">
                            {cat.count}
                          </span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              )}

              {stats && (
                <div className="mt-6 pt-4 border-t border-gray-700">
                  <h4 className="font-semibold mb-3">Stats</h4>
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between">
                      <span className="text-gray-400">Documents:</span>
                      <span className="font-mono text-green-400">
                        {stats.totalDocuments.count.toLocaleString()}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-400">Relationships:</span>
                      <span className="font-mono text-blue-400">
                        {stats.totalTriples.count.toLocaleString()}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-400">Actors:</span>
                      <span className="font-mono text-purple-400">
                        {stats.totalActors.count.toLocaleString()}
                      </span>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Bottom Navigation Bar */}
      <div className="fixed inset-x-0 bottom-0 bg-gray-800 border-t border-gray-700 z-50">
        <div className="flex justify-around">
          <button
            onClick={() => setActiveTab(activeTab === 'search' ? null : 'search')}
            className={`flex-1 py-4 flex flex-col items-center ${
              activeTab === 'search' ? 'text-blue-400' : 'text-gray-400'
            }`}
          >
            <svg className="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <span className="text-xs">Search</span>
          </button>

          <button
            onClick={() => setActiveTab(activeTab === 'timeline' ? null : 'timeline')}
            className={`flex-1 py-4 flex flex-col items-center ${
              activeTab === 'timeline' ? 'text-blue-400' : 'text-gray-400'
            }`}
          >
            <svg className="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
            </svg>
            <span className="text-xs">Timeline</span>
          </button>

          <button
            onClick={() => setActiveTab(activeTab === 'filters' ? null : 'filters')}
            className={`flex-1 py-4 flex flex-col items-center ${
              activeTab === 'filters' ? 'text-blue-400' : 'text-gray-400'
            }`}
          >
            <svg className="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4" />
            </svg>
            <span className="text-xs">Filters</span>
          </button>
        </div>
      </div>

      {/* Document Modal */}
      {documentToView && (() => {
        const rel = relationships.find(r => r.doc_id === documentToView);
        return rel ? (
          <DocumentModal
            docId={documentToView}
            highlightTerm={selectedActor || rel.actor}
            secondaryHighlightTerm={
              selectedActor
                ? (rel.actor === selectedActor ? rel.target : rel.actor)
                : rel.target
            }
            onClose={() => setDocumentToView(null)}
          />
        ) : null;
      })()}
    </>
  );
}
