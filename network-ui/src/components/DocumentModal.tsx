import { useState, useEffect, useRef, useMemo } from 'react';
import { fetchDocument, fetchDocumentText } from '../api';
import type { Document } from '../types';
import { ImessageChatView, parseImessageTranscript } from './ImessageChatView';
import { EmailThreadView, parseEmailThread } from './EmailThreadView';

interface DocumentModalProps {
  docId: string;
  highlightTerm: string | null;
  secondaryHighlightTerm?: string | null;
  onClose: () => void;
}

interface MatchPosition {
  index: number;
  term: string;
  type: 'primary' | 'secondary';
  percentage: number;
}

type TranscriptView = 'chat' | 'email' | 'text';

export default function DocumentModal({ docId, highlightTerm, secondaryHighlightTerm, onClose }: DocumentModalProps) {
  const [document, setDocument] = useState<Document | null>(null);
  const [documentText, setDocumentText] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [matchPositions, setMatchPositions] = useState<MatchPosition[]>([]);
  const [activeView, setActiveView] = useState<TranscriptView>('text');
  const contentRef = useRef<HTMLDivElement>(null);
  const matchRefs = useRef<Map<number, HTMLElement>>(new Map());
  const imessageTranscript = useMemo(() => parseImessageTranscript(documentText), [documentText]);
  const emailThread = useMemo(() => parseEmailThread(documentText), [documentText]);
  const hasStructuredView = Boolean(imessageTranscript || emailThread);
  const showChatView = Boolean(imessageTranscript && activeView === 'chat');
  const showEmailView = Boolean(emailThread && activeView === 'email');
  const showPlainTextView = !hasStructuredView || activeView === 'text';

  useEffect(() => {
    if (imessageTranscript) {
      setActiveView('chat');
    } else if (emailThread) {
      setActiveView('email');
    } else {
      setActiveView('text');
    }
  }, [imessageTranscript, emailThread]);

  useEffect(() => {
    const loadDocument = async () => {
      setLoading(true);
      setError(null);

      try {
        const [doc, textData] = await Promise.all([
          fetchDocument(docId),
          fetchDocumentText(docId)
        ]);

        setDocument(doc);
        setDocumentText(textData.text);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load document');
      } finally {
        setLoading(false);
      }
    };

    loadDocument();
  }, [docId]);

  // Calculate match positions when document text loads
  useEffect(() => {
    if (!documentText) return;

    const positions: MatchPosition[] = [];
    const textLength = documentText.length;

    // Build patterns including individual words
    const primaryPatterns: string[] = [];
    const secondaryPatterns: string[] = [];

    if (highlightTerm) {
      // Add full term
      primaryPatterns.push(highlightTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
      // Add individual words (min 3 chars, excluding common words)
      highlightTerm.split(/\s+/).forEach(word => {
        if (word.length >= 3 && word.toLowerCase() !== 'the') {
          primaryPatterns.push(word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
        }
      });
    }

    if (secondaryHighlightTerm) {
      // Add full term
      secondaryPatterns.push(secondaryHighlightTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
      // Add individual words (min 3 chars, excluding common words)
      secondaryHighlightTerm.split(/\s+/).forEach(word => {
        if (word.length >= 3 && word.toLowerCase() !== 'the') {
          secondaryPatterns.push(word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
        }
      });
    }

    // Find all matches for primary term and its words
    if (primaryPatterns.length > 0) {
      const regex = new RegExp(`(${primaryPatterns.join('|')})`, 'gi');
      let match;
      while ((match = regex.exec(documentText)) !== null) {
        positions.push({
          index: match.index,
          term: match[0],
          type: 'primary',
          percentage: (match.index / textLength) * 100
        });
      }
    }

    // Find all matches for secondary term and its words
    if (secondaryPatterns.length > 0) {
      const regex = new RegExp(`(${secondaryPatterns.join('|')})`, 'gi');
      let match;
      while ((match = regex.exec(documentText)) !== null) {
        positions.push({
          index: match.index,
          term: match[0],
          type: 'secondary',
          percentage: (match.index / textLength) * 100
        });
      }
    }

    // Sort by position
    positions.sort((a, b) => a.index - b.index);
    setMatchPositions(positions);
  }, [documentText, highlightTerm, secondaryHighlightTerm]);

  const scrollToMatch = (index: number) => {
    const element = matchRefs.current.get(index);
    if (element && contentRef.current) {
      element.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
  };

  const highlightText = (text: string, term: string | null, secondaryTerm: string | null): JSX.Element[] => {
    if (!term && !secondaryTerm) {
      return [<span key="0">{text}</span>];
    }

    try {
      // Build regex pattern for both terms, including individual words
      const patterns: string[] = [];
      const primaryWords = new Set<string>();
      const secondaryWords = new Set<string>();

      if (term) {
        // Add full term
        patterns.push(term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
        // Add individual words (min 3 chars, excluding common words)
        term.split(/\s+/).forEach(word => {
          if (word.length >= 3 && word.toLowerCase() !== 'the') {
            primaryWords.add(word.toLowerCase());
            patterns.push(word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
          }
        });
      }

      if (secondaryTerm) {
        // Add full term
        patterns.push(secondaryTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
        // Add individual words (min 3 chars, excluding common words)
        secondaryTerm.split(/\s+/).forEach(word => {
          if (word.length >= 3 && word.toLowerCase() !== 'the') {
            secondaryWords.add(word.toLowerCase());
            patterns.push(word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
          }
        });
      }

      const regex = new RegExp(`(${patterns.join('|')})`, 'gi');
      const parts = text.split(regex);

      let currentIndex = 0;
      let matchCount = 0;

      return parts.map((part, index) => {
        const partLower = part.toLowerCase();
        const partStart = currentIndex;
        currentIndex += part.length;

        // Check if it matches the primary term or any of its words (yellow)
        if (term && (partLower === term.toLowerCase() || primaryWords.has(partLower))) {
          const matchIndex = matchCount++;
          return (
            <mark
              key={index}
              ref={(el) => {
                if (el) matchRefs.current.set(partStart, el);
              }}
              className="bg-yellow-400 text-black px-1 rounded"
            >
              {part}
            </mark>
          );
        }

        // Check if it matches the secondary term or any of its words (light orange)
        if (secondaryTerm && (partLower === secondaryTerm.toLowerCase() || secondaryWords.has(partLower))) {
          const matchIndex = matchCount++;
          return (
            <mark
              key={index}
              ref={(el) => {
                if (el) matchRefs.current.set(partStart, el);
              }}
              className="bg-orange-300 text-black px-1 rounded"
            >
              {part}
            </mark>
          );
        }

        return <span key={index}>{part}</span>;
      });
    } catch (err) {
      // If regex fails, just return plain text
      return [<span key="0">{text}</span>];
    }
  };

  return (
    <div
      className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center z-50 p-4"
      onClick={onClose}
    >
      <div
        className="bg-gray-800 rounded-lg shadow-2xl max-w-4xl w-full max-h-[90vh] flex flex-col relative"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="p-6 border-b border-gray-700 flex justify-between items-start">
          <div className="flex-1">
            <h2 className="text-2xl font-semibold text-blue-400 mb-2">
              {document?.doc_id || docId}
            </h2>
            {document && (
              <div className="space-y-1 text-sm">
                <p className="text-gray-300">{document.one_sentence_summary}</p>
                <div className="flex gap-4 text-gray-500">
                  <span className="px-2 py-1 bg-gray-700 rounded">{document.category}</span>
                  {document.date_range_earliest && (
                    <span>
                      {document.date_range_earliest}
                      {document.date_range_latest && document.date_range_latest !== document.date_range_earliest
                        ? ` to ${document.date_range_latest}`
                        : ''}
                    </span>
                  )}
                </div>
              </div>
            )}
          </div>
          <button
            onClick={onClose}
            className="ml-4 text-gray-400 hover:text-white text-2xl leading-none transition-colors"
          >
            ✕
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 pr-12" ref={contentRef}>
          {loading && (
            <div className="flex items-center justify-center py-12">
              <div className="text-gray-400">Loading document...</div>
            </div>
          )}

          {error && (
            <div className="bg-red-900/30 border border-red-700 rounded p-4 text-red-300">
              {error}
            </div>
          )}

          {!loading && !error && documentText && (
            <div className="space-y-6">
              {hasStructuredView && (
                <div>
                  <div className="flex gap-4 border-b border-gray-700 mb-4">
                    {imessageTranscript && (
                      <button
                        onClick={() => setActiveView('chat')}
                        className={`pb-2 text-sm font-medium transition-colors ${
                          showChatView
                            ? 'text-blue-300 border-b-2 border-blue-400'
                            : 'text-gray-500 border-b-2 border-transparent hover:text-gray-300'
                        }`}
                      >
                        Chat View
                      </button>
                    )}
                    {emailThread && (
                      <button
                        onClick={() => setActiveView('email')}
                        className={`pb-2 text-sm font-medium transition-colors ${
                          showEmailView
                            ? 'text-blue-300 border-b-2 border-blue-400'
                            : 'text-gray-500 border-b-2 border-transparent hover:text-gray-300'
                        }`}
                      >
                        Email View
                      </button>
                    )}
                    <button
                      onClick={() => setActiveView('text')}
                      className={`pb-2 text-sm font-medium transition-colors ${
                        activeView === 'text'
                          ? 'text-blue-300 border-b-2 border-blue-400'
                          : 'text-gray-500 border-b-2 border-transparent hover:text-gray-300'
                      }`}
                    >
                      Plain Text
                    </button>
                  </div>

                  {showChatView && imessageTranscript && (
                    <ImessageChatView transcript={imessageTranscript} />
                  )}

                  {showEmailView && emailThread && <EmailThreadView thread={emailThread} />}
                </div>
              )}

              {showPlainTextView && (
                <div className="prose prose-invert max-w-none">
                  <div className="whitespace-pre-wrap text-gray-300 leading-relaxed font-mono text-sm">
                    {highlightText(documentText, highlightTerm, secondaryHighlightTerm || null)}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Scroll Indicator - Fixed to modal container */}
        {!loading && !error && matchPositions.length > 0 && showPlainTextView && (
          <div className="absolute right-4 top-32 bottom-24 w-3 bg-gray-700/50 rounded-full pointer-events-none z-10">
            {matchPositions.map((match, idx) => (
              <button
                key={idx}
                onClick={() => scrollToMatch(match.index)}
                className={`absolute w-3 h-3 rounded-full transform -translate-x-0 transition-all hover:scale-150 pointer-events-auto ${
                  match.type === 'primary'
                    ? 'bg-yellow-400 hover:bg-yellow-300'
                    : 'bg-orange-300 hover:bg-orange-200'
                }`}
                style={{ top: `${match.percentage}%` }}
                title={`${match.term} (${idx + 1}/${matchPositions.length})`}
              />
            ))}
          </div>
        )}

        {/* Footer */}
        <div className="p-4 border-t border-gray-700 flex justify-between items-center">
          <div className="text-sm text-gray-500 flex gap-4">
            {highlightTerm && (
              <span>
                <span className="inline-block bg-yellow-400 text-black px-2 py-0.5 rounded text-xs mr-1">
                  {highlightTerm}
                </span>
              </span>
            )}
            {secondaryHighlightTerm && (
              <span>
                <span className="inline-block bg-orange-300 text-black px-2 py-0.5 rounded text-xs mr-1">
                  {secondaryHighlightTerm}
                </span>
              </span>
            )}
          </div>
          <button
            onClick={onClose}
            className="px-4 py-2 bg-gray-700 hover:bg-gray-600 text-white rounded transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
