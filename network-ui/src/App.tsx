import { useState, useEffect, useCallback } from 'react';
import NetworkGraph from './components/NetworkGraph';
import Sidebar from './components/Sidebar';
import RightSidebar from './components/RightSidebar';
import MobileBottomNav from './components/MobileBottomNav';
import { WelcomeModal } from './components/WelcomeModal';
import { fetchStats, fetchRelationships, fetchActorRelationships, fetchTagClusters, fetchActorCounts } from './api';
import type { Stats, Relationship, TagCluster } from './types';

function App() {
  // Detect if mobile on initial load (lg breakpoint is 1024px in Tailwind)
  const isMobile = window.innerWidth < 1024;

  const [stats, setStats] = useState<Stats | null>(null);
  const [tagClusters, setTagClusters] = useState<TagCluster[]>([]);
  const [relationships, setRelationships] = useState<Relationship[]>([]);
  const [totalBeforeLimit, setTotalBeforeLimit] = useState<number>(0);
  const [loading, setLoading] = useState(true);
  const [selectedActor, setSelectedActor] = useState<string | null>(null);
  const [actorRelationships, setActorRelationships] = useState<Relationship[]>([]);
  const [actorTotalBeforeFilter, setActorTotalBeforeFilter] = useState<number>(0);
  const [limit, setLimit] = useState(isMobile ? 5000 : 9600);
  const [maxHops, setMaxHops] = useState<number | null>(3); // Default 3 hops
  const [minDensity, setMinDensity] = useState(50); // Default 50% density threshold
  const [enabledClusterIds, setEnabledClusterIds] = useState<Set<number>>(new Set());
  const [enabledCategories, setEnabledCategories] = useState<Set<string>>(new Set());
  const [yearRange, setYearRange] = useState<[number, number]>([1980, 2025]);
  const [includeUndated, setIncludeUndated] = useState(false);
  const [keywords, setKeywords] = useState('');
  const [actorTotalCounts, setActorTotalCounts] = useState<Record<string, number>>({});
  const [showWelcome, setShowWelcome] = useState(() => {
    // Check if user has seen the welcome message before
    return !localStorage.getItem('hasSeenWelcome');
  });
  const [isInitialized, setIsInitialized] = useState(false);

  // Load tag clusters and stats on mount, then trigger initial data load
  useEffect(() => {
    const initializeApp = async () => {
      try {
        // Load tag clusters and stats in parallel
        const [clusters, statsData] = await Promise.all([
          fetchTagClusters(),
          fetchStats()
        ]);

        // Batch all state updates together with a single render using a microtask
        // This ensures enabledClusterIds and enabledCategories are set before the data loading effect runs
        queueMicrotask(() => {
          setTagClusters(clusters);
          setEnabledClusterIds(new Set(clusters.map(c => c.id)));
          setStats(statsData);
          setEnabledCategories(new Set(statsData.categories.map(c => c.category)));
          setIsInitialized(true);
        });
      } catch (error) {
        console.error('Error initializing app:', error);
      }
    };
    initializeApp();
  }, []);

  // Load data when limit, enabled clusters, enabled categories, year range, includeUndated, keywords, or maxHops change (but only after initialization)
  useEffect(() => {
    if (isInitialized) {
      loadData();
    }
  }, [isInitialized, limit, enabledClusterIds, enabledCategories, yearRange, includeUndated, keywords, maxHops]);

  const loadData = async () => {
    try {
      setLoading(true);
      const clusterIds = Array.from(enabledClusterIds);
      const categories = Array.from(enabledCategories);
      const [relationshipsResponse, actorCounts] = await Promise.all([
        fetchRelationships(limit, clusterIds, categories, yearRange, includeUndated, keywords, maxHops),
        fetchActorCounts(300)
      ]);
      setRelationships(relationshipsResponse.relationships);
      setTotalBeforeLimit(relationshipsResponse.totalBeforeLimit);
      setActorTotalCounts(actorCounts);
    } catch (error) {
      console.error('Error loading data:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleActorClick = useCallback((actorName: string) => {
    setSelectedActor(prev => prev === actorName ? null : actorName);
  }, []);

  // Toggle tag cluster
  const toggleCluster = useCallback((clusterId: number) => {
    setEnabledClusterIds(prev => {
      const next = new Set(prev);
      if (next.has(clusterId)) {
        next.delete(clusterId);
      } else {
        next.add(clusterId);
      }
      return next;
    });
  }, []);

  // Toggle category
  const toggleCategory = useCallback((category: string) => {
    setEnabledCategories(prev => {
      const next = new Set(prev);
      if (next.has(category)) {
        next.delete(category);
      } else {
        next.add(category);
      }
      return next;
    });
  }, []);

  // Handle closing welcome modal
  const handleCloseWelcome = useCallback(() => {
    localStorage.setItem('hasSeenWelcome', 'true');
    setShowWelcome(false);
  }, []);

  // Fetch actor-specific relationships when an actor is selected or clusters/categories/year range/includeUndated/keywords/maxHops change
  useEffect(() => {
    if (!selectedActor) {
      setActorRelationships([]);
      setActorTotalBeforeFilter(0);
      return;
    }

    const loadActorRelationships = async () => {
      try {
        const clusterIds = Array.from(enabledClusterIds);
        const categories = Array.from(enabledCategories);
        const response = await fetchActorRelationships(selectedActor, clusterIds, categories, yearRange, includeUndated, keywords, maxHops);
        setActorRelationships(response.relationships);
        setActorTotalBeforeFilter(response.totalBeforeFilter);
      } catch (error) {
        console.error('Error loading actor relationships:', error);
        setActorRelationships([]);
        setActorTotalBeforeFilter(0);
      }
    };

    loadActorRelationships();
  }, [selectedActor, enabledClusterIds, enabledCategories, yearRange, includeUndated, keywords, maxHops]);

  return (
    <div className="flex h-screen bg-gray-900 text-white">
      {/* Desktop Sidebar - hidden on mobile */}
      <div className="hidden lg:block">
        <Sidebar
          stats={stats}
          selectedActor={selectedActor}
          onActorSelect={setSelectedActor}
          limit={limit}
          onLimitChange={setLimit}
          maxHops={maxHops}
          onMaxHopsChange={setMaxHops}
          minDensity={minDensity}
          onMinDensityChange={setMinDensity}
          tagClusters={tagClusters}
          enabledClusterIds={enabledClusterIds}
          onToggleCluster={toggleCluster}
          enabledCategories={enabledCategories}
          onToggleCategory={toggleCategory}
          yearRange={yearRange}
          onYearRangeChange={setYearRange}
          includeUndated={includeUndated}
          onIncludeUndatedChange={setIncludeUndated}
          keywords={keywords}
          onKeywordsChange={setKeywords}
        />
      </div>

      {/* Main Graph Area */}
      <div className="flex-1 relative pb-16 lg:pb-0">
        {loading ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <div className="animate-spin rounded-full h-16 w-16 border-t-2 border-b-2 border-blue-500 mx-auto mb-4"></div>
              <p className="text-gray-400">Loading network data...</p>
            </div>
          </div>
        ) : (
          <NetworkGraph
            relationships={relationships}
            selectedActor={selectedActor}
            onActorClick={handleActorClick}
            minDensity={minDensity}
            actorTotalCounts={actorTotalCounts}
          />
        )}
      </div>

      {/* Desktop Right Sidebar - hidden on mobile */}
      {selectedActor && (
        <div className="hidden lg:block">
          <RightSidebar
            selectedActor={selectedActor}
            relationships={actorRelationships}
            totalRelationships={actorTotalBeforeFilter}
            onClose={() => setSelectedActor(null)}
            yearRange={yearRange}
          />
        </div>
      )}

      {/* Mobile Bottom Navigation - shown only on mobile */}
      <div className="lg:hidden">
        <MobileBottomNav
          stats={stats}
          selectedActor={selectedActor}
          onActorSelect={setSelectedActor}
          limit={limit}
          onLimitChange={setLimit}
          tagClusters={tagClusters}
          enabledClusterIds={enabledClusterIds}
          onToggleCluster={toggleCluster}
          enabledCategories={enabledCategories}
          onToggleCategory={toggleCategory}
          relationships={selectedActor ? actorRelationships : relationships}
        />
      </div>

      {/* Welcome Modal */}
      <WelcomeModal isOpen={showWelcome} onClose={handleCloseWelcome} />
    </div>
  );
}

export default App;
