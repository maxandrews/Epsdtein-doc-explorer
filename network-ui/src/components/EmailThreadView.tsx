import { LinkifiedText } from './LinkifiedText';

export interface EmailMessage {
  from?: string;
  to?: string;
  cc?: string;
  sent?: string;
  subject?: string;
  importance?: string;
  headerLine?: string;
  body: string;
  isQuoted: boolean;
}

export interface EmailThread {
  messages: EmailMessage[];
  disclaimer?: string;
}

const headerRegex = /^(From|Sent|To|Subject|Cc|Bcc|Date|Importance):\s*(.*)$/i;
const quotedRegex = /^On .+wrote:?$/i;
const oversightLineRegex = /^HOUSE\s+OVERSIGHT\s+\d+/i;
const disclaimerStartRegex = /^\s*please note\s*$/i;
const disclaimerEndRegex = /attachments\. copyright/i;

const parseHeaderValue = (line: string) => {
  const match = line.match(headerRegex);
  if (!match) return null;
  return {
    key: match[1].toLowerCase(),
    value: match[2].trim()
  };
};

const hasEmailStructure = (text: string) => {
  const headerMatches = text.match(/^(From|Sent|To|Subject|Date):/gim);
  return !!(headerMatches && headerMatches.length >= 2);
};

const extractDisclaimer = (lines: string[]): { disclaimer?: string; cleanedLines: string[] } => {
  const cleaned: string[] = [];
  let capturingDisclaimer = false;
  let disclaimerLines: string[] = [];

  for (const line of lines) {
    if (oversightLineRegex.test(line.trim())) {
      continue;
    }

    if (disclaimerStartRegex.test(line)) {
      capturingDisclaimer = true;
      disclaimerLines.push(line);
      continue;
    }

    if (capturingDisclaimer) {
      disclaimerLines.push(line);
      if (disclaimerEndRegex.test(line)) {
        capturingDisclaimer = false;
      }
      continue;
    }

    cleaned.push(line);
  }

  return {
    disclaimer: disclaimerLines.length > 0 ? disclaimerLines.join('\n').trim() : undefined,
    cleanedLines: cleaned
  };
};

export const parseEmailThread = (rawText: string): EmailThread | null => {
  if (!rawText || !hasEmailStructure(rawText)) {
    return null;
  }

  const { cleanedLines, disclaimer } = extractDisclaimer(rawText.split(/\r?\n/));
  const lines = cleanedLines;

  const messages: EmailMessage[] = [];
  let index = 0;

  const collectBody = () => {
    const bodyLines: string[] = [];
    while (
      index < lines.length &&
      !quotedRegex.test(lines[index].trim()) &&
      !headerRegex.test(lines[index])
    ) {
      bodyLines.push(lines[index]);
      index++;
    }
    return bodyLines.join('\n').trim();
  };

  while (index < lines.length) {
    const line = lines[index];
    const trimmed = line.trim();

    if (!trimmed) {
      index++;
      continue;
    }

    if (quotedRegex.test(trimmed)) {
      const headerLine = trimmed;
      index++;
      const body = collectBody();
      messages.push({
        headerLine,
        body,
        isQuoted: true
      });
      continue;
    }

    if (!headerRegex.test(line)) {
      index++;
      continue;
    }

    const headers: Record<string, string> = {};
    while (index < lines.length) {
      const parsed = parseHeaderValue(lines[index]);
      if (!parsed) break;
      headers[parsed.key] = parsed.value;
      index++;
    }

    while (index < lines.length && lines[index].trim() === '') {
      index++;
    }

    const body = collectBody();

    messages.push({
      from: headers['from'],
      to: headers['to'],
      cc: headers['cc'] || headers['bcc'],
      sent: headers['sent'] || headers['date'],
      subject: headers['subject'],
      importance: headers['importance'],
      body,
      isQuoted: false
    });
  }

  if (messages.length === 0) {
    return null;
  }

  return { messages, disclaimer };
};

interface EmailThreadViewProps {
  thread: EmailThread;
}

export function EmailThreadView({ thread }: EmailThreadViewProps) {
  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-yellow-800/50 bg-yellow-900/10 text-sm text-yellow-100 p-3">
        Rendering of emails is best-effort because the source text is inconsistent. Always double-check against the plain text view before quoting or jumping to conclusions.
      </div>

      {thread.messages.map((message, idx) => (
        <div
          key={`${message.sent ?? idx}-${idx}`}
          className={`rounded-2xl border border-gray-700/60 p-4 ${
            message.isQuoted ? 'bg-gray-800/40' : 'bg-gray-800/70'
          }`}
        >
          {!message.isQuoted ? (
            <div className="space-y-1 text-sm text-gray-300">
              {message.subject && (
                <div className="text-lg font-semibold text-white">{message.subject}</div>
              )}
              {message.from && (
                <div>
                  <span className="text-gray-500 uppercase tracking-wider text-xs">From: </span>
                  {message.from}
                </div>
              )}
              {message.to && (
                <div>
                  <span className="text-gray-500 uppercase tracking-wider text-xs">To: </span>
                  {message.to}
                </div>
              )}
              {message.cc && (
                <div>
                  <span className="text-gray-500 uppercase tracking-wider text-xs">Cc: </span>
                  {message.cc}
                </div>
              )}
              {message.sent && (
                <div>
                  <span className="text-gray-500 uppercase tracking-wider text-xs">Sent: </span>
                  {message.sent}
                </div>
              )}
              {message.importance && (
                <div>
                  <span className="text-gray-500 uppercase tracking-wider text-xs">Importance: </span>
                  {message.importance}
                </div>
              )}
            </div>
          ) : (
            <div className="text-sm text-gray-400 italic">{message.headerLine}</div>
          )}

          {message.body && (
            <div className="mt-3 whitespace-pre-wrap text-sm text-gray-100 leading-relaxed">
              <LinkifiedText text={message.body} />
            </div>
          )}
        </div>
      ))}

      {thread.disclaimer && (
        <details className="rounded-lg border border-gray-700/60 bg-gray-800/50">
          <summary className="px-4 py-2 cursor-pointer text-sm text-gray-200">
            View disclaimer
          </summary>
          <div className="px-4 pb-4 pt-2 whitespace-pre-wrap text-xs text-gray-400 leading-relaxed">
            {thread.disclaimer}
          </div>
        </details>
      )}
    </div>
  );
}
