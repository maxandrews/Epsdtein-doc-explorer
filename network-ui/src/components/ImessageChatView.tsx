import { useMemo } from 'react';
import { LinkifiedText } from './LinkifiedText';

export interface ImessageMessage {
  sender: string;
  displayName: string;
  time?: string;
  message: string;
  isLocal: boolean;
}

export interface ImessageTranscript {
  participants: string[];
  presentityIds: string[];
  messages: ImessageMessage[];
}

const metadataRegex = /^(Flags|Is Read|Is Invitation|GUID|Last Message ID|Source|Service|Start Time|End Time|Chat Room|Messages\s*-?)/i;
const participantsRegex = /^Participants:/i;
const presentityRegex = /^Presentity IDs:/i;
const senderRegex = /^Sender:/i;
const timeRegex = /^Time:/i;
const messageRegex = /^Message:/i;
const sectionBreakRegex = /^(HOUSE\s+OVERSIGHT|[-A-Z0-9_ ]{5,}\d{3,})/;
const PRIMARY_SENDER_EMAIL = 'jeeitunes@gmail.com';

const parseListField = (value: string) =>
  value
    .split(/[,;]+/)
    .map((entry) => entry.trim())
    .filter(Boolean);

const extractValue = (line: string) => line.slice(line.indexOf(':') + 1).trim();

const cleanIdentifier = (value: string) => value.replace(/^e:/i, '').trim();

interface RawMessageState {
  sender?: string;
  time?: string;
  messageParts: string[];
  capturing: boolean;
}

const finalizeMessage = (
  current: RawMessageState | null,
  fallbackLocalName: string
): ImessageMessage | null => {
  if (!current) return null;
  const combinedMessage = current.messageParts.join('\n').trim();
  if (!combinedMessage) {
    return null;
  }

  const rawSender = current.sender ? cleanIdentifier(current.sender) : '';
  const senderLower = rawSender.toLowerCase();
  const normalizedPrimary = PRIMARY_SENDER_EMAIL.toLowerCase();
  const isLocal = senderLower === normalizedPrimary;
  const displayName = rawSender || (isLocal ? fallbackLocalName : 'Unknown');
  const cleanedTime = current.time ? current.time.split('(')[0]?.trim() : undefined;

  return {
    sender: rawSender || displayName,
    displayName,
    time: cleanedTime,
    message: combinedMessage,
    isLocal
  };
};

export const parseImessageTranscript = (text: string): ImessageTranscript | null => {
  if (!text || !text.includes('Service: iMessage')) {
    return null;
  }

  const lines = text.split(/\r?\n/);
  let participants: string[] = [];
  let presentityIds: string[] = [];
  const messages: ImessageMessage[] = [];

  let current: RawMessageState | null = null;

  const pushCurrentMessage = () => {
    const fallbackLocalName =
      presentityIds.find(Boolean) || participants.find(Boolean) || PRIMARY_SENDER_EMAIL;
    const parsed = finalizeMessage(current, fallbackLocalName);
    if (parsed) {
      messages.push(parsed);
    }
    current = null;
  };

  for (const line of lines) {
    const trimmed = line.trim();

    if (!trimmed) {
      if (current?.capturing) {
        current.messageParts.push('');
      }
      continue;
    }

    if (participantsRegex.test(trimmed)) {
      participants = parseListField(extractValue(line)).map(cleanIdentifier);
      continue;
    }

    if (presentityRegex.test(trimmed)) {
      presentityIds = parseListField(extractValue(line)).map(cleanIdentifier);
      continue;
    }

    if (sectionBreakRegex.test(trimmed)) {
      if (current) {
        current.capturing = false;
      }
      continue;
    }

    if (senderRegex.test(trimmed)) {
      if (current) {
        pushCurrentMessage();
      }
      current = {
        sender: extractValue(line),
        time: undefined,
        messageParts: [],
        capturing: false
      };
      continue;
    }

    if (timeRegex.test(trimmed)) {
      if (!current) {
        current = { messageParts: [], capturing: false };
      }
      current.time = extractValue(line);
      continue;
    }

    if (messageRegex.test(trimmed)) {
      if (!current) {
        current = { messageParts: [], capturing: true };
      }
      const initial = extractValue(line);
      current.messageParts = initial ? [initial] : [];
      current.capturing = true;
      continue;
    }

    if (metadataRegex.test(trimmed)) {
      if (current) {
        current.capturing = false;
      }
      continue;
    }

    if (current?.capturing) {
      current.messageParts.push(line);
    }
  }

  if (current) {
    pushCurrentMessage();
  }

  if (messages.length === 0) {
    return null;
  }

  return {
    participants,
    presentityIds,
    messages
  };
};

interface ImessageChatViewProps {
  transcript: ImessageTranscript;
}

export function ImessageChatView({ transcript }: ImessageChatViewProps) {
  const participantNames = useMemo(() => {
    return Array.from(
      new Set([...transcript.participants, ...transcript.presentityIds])
    ).filter(Boolean);
  }, [transcript]);

  return (
    <div className="space-y-6">
      <div className="text-sm text-gray-400">
        Participants:{' '}
        <span className="text-gray-200">
          {participantNames.length > 0 ? participantNames.join(', ') : 'Unknown'}
        </span>
      </div>

      <div className="space-y-4">
        {transcript.messages.map((message, idx) => (
          <div
            key={`${message.displayName}-${idx}-${message.time ?? ''}`}
            className={`flex ${message.isLocal ? 'justify-end' : 'justify-start'}`}
          >
            <div className="max-w-[80%]">
              <div
                className={`text-xs text-gray-500 mb-1 ${
                  message.isLocal ? 'text-right' : 'text-left'
                }`}
              >
                {message.displayName}
                {message.time ? ` • ${message.time}` : ''}
              </div>
              <div
                className={`px-4 py-2 whitespace-pre-wrap leading-relaxed text-sm shadow-lg ${
                  message.isLocal
                    ? 'bg-blue-500 text-white rounded-2xl rounded-br-sm'
                    : 'bg-gray-700 text-gray-100 rounded-2xl rounded-bl-sm'
                }`}
              >
                <LinkifiedText text={message.message} />
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
