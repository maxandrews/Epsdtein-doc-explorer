import type { ReactNode } from 'react';

interface LinkifiedTextProps {
  text: string;
}

const isLinkStart = (value: string, index: number) => {
  const lower = value.toLowerCase();
  const httpIndex = lower.indexOf('http://', index);
  const httpsIndex = lower.indexOf('https://', index);

  if (httpIndex === -1) return httpsIndex;
  if (httpsIndex === -1) return httpIndex;
  return Math.min(httpIndex, httpsIndex);
};

const renderLinkifiedContent = (text: string): ReactNode[] => {
  const nodes: ReactNode[] = [];
  let cursor = 0;
  let key = 0;

  const pushText = (value: string) => {
    if (!value) return;
    nodes.push(
      <span key={`text-${key++}`}>
        {value}
      </span>
    );
  };

  while (cursor < text.length) {
    const nextLink = isLinkStart(text, cursor);
    if (nextLink === -1) {
      pushText(text.slice(cursor));
      break;
    }

    if (nextLink > cursor) {
      pushText(text.slice(cursor, nextLink));
    }

    let end = nextLink;
    while (end < text.length) {
      const char = text[end];
      if (char === '\n' || char === '\r') {
        end++;
        continue;
      }
      if (char === ')' || /\s/.test(char)) {
        break;
      }
      end++;
    }

    const displayUrl = text.slice(nextLink, end);
    const href = displayUrl.replace(/\s+/g, '');

    nodes.push(
      <a
        key={`link-${key++}`}
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        className="underline text-blue-200 hover:text-blue-100 break-all"
      >
        {displayUrl}
      </a>
    );

    cursor = end;
  }

  return nodes;
};

export function LinkifiedText({ text }: LinkifiedTextProps) {
  const nodes = renderLinkifiedContent(text);
  if (nodes.length === 0) {
    return <>{text}</>;
  }
  return <>{nodes}</>;
}
