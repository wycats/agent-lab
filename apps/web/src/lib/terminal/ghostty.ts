import { FitAddon, Terminal, init } from 'ghostty-web';
import type { TerminalSurface, TerminalDimensions, Disposable } from './surface';

let initialization: Promise<void> | undefined;

function initializeGhostty(): Promise<void> {
  initialization ??= init().catch((error: unknown) => {
    initialization = undefined;
    throw error;
  });
  return initialization;
}

class GhosttyTerminalSurface implements TerminalSurface {
  readonly #terminal: Terminal;
  readonly #fit: FitAddon;
  readonly #host: HTMLElement;
  readonly #input: HTMLTextAreaElement;
  readonly #forwardHostFocus: (event: FocusEvent) => void;
  readonly #focusInputAfterClick: (event: MouseEvent) => void;

  constructor(
    terminal: Terminal,
    fit: FitAddon,
    host: HTMLElement,
    input: HTMLTextAreaElement
  ) {
    this.#terminal = terminal;
    this.#fit = fit;
    this.#host = host;
    this.#input = input;
    this.#forwardHostFocus = (event) => {
      if (event.target === this.#host) this.#input.focus({ preventScroll: true });
    };
    this.#focusInputAfterClick = (event) => {
      if (event.button === 0) this.#input.focus({ preventScroll: true });
    };
    this.#host.addEventListener('focusin', this.#forwardHostFocus);
    this.#host.addEventListener('click', this.#focusInputAfterClick);
  }

  get dimensions(): TerminalDimensions {
    return { cols: this.#terminal.cols, rows: this.#terminal.rows };
  }

  write(data: string | Uint8Array, afterWrite?: () => void): void {
    this.#terminal.write(data, afterWrite);
  }

  readText(): string {
    const buffer = this.#terminal.buffer.active;
    const lines: string[] = [];
    const viewportOffset = Math.floor(this.#terminal.getViewportY());
    const firstRow = Math.max(
      0,
      buffer.length - this.#terminal.rows - viewportOffset
    );
    const lastRow = Math.min(buffer.length, firstRow + this.#terminal.rows);
    for (let row = firstRow; row < lastRow; row += 1) {
      lines.push(buffer.getLine(row)?.translateToString(true) ?? '');
    }
    return lines.join('\n');
  }

  onData(listener: (data: string) => void): Disposable {
    return this.#terminal.onData(listener);
  }

  onUserInput(listener: () => void): Disposable {
    const key = this.#terminal.onKey(({ domEvent }) => {
      const keyName = domEvent.key.toLowerCase();
      const modifierOnly = ['alt', 'altgraph', 'capslock', 'control', 'meta', 'shift'].includes(
        keyName
      );
      const cancellation = domEvent.ctrlKey && keyName === 'c';
      if (!domEvent.metaKey && !modifierOnly && !cancellation) listener();
    });
    const paste = () => listener();
    const composition = () => listener();
    this.#host.addEventListener('paste', paste, { capture: true });
    this.#host.addEventListener('compositionstart', composition, { capture: true });
    return {
      dispose: () => {
        key.dispose();
        this.#host.removeEventListener('paste', paste, { capture: true });
        this.#host.removeEventListener('compositionstart', composition, { capture: true });
      }
    };
  }

  onResize(listener: (dimensions: TerminalDimensions) => void): Disposable {
    return this.#terminal.onResize(listener);
  }

  onScroll(listener: () => void): Disposable {
    return this.#terminal.onScroll(() => listener());
  }

  focus(): void {
    this.#input.focus({ preventScroll: true });
  }

  dispose(): void {
    this.#host.removeEventListener('focusin', this.#forwardHostFocus);
    this.#host.removeEventListener('click', this.#focusInputAfterClick);
    this.#fit.dispose();
    this.#terminal.dispose();
  }
}

export async function createGhosttySurface(host: HTMLElement): Promise<TerminalSurface> {
  await initializeGhostty();
  await document.fonts.load('400 13px "Geist Mono Variable"');
  const terminal = new Terminal({
    cols: 100,
    rows: 30,
    // Ghostty redraws the full canvas for every blink. A steady cursor keeps
    // focus visible without doing terminal-sized work while the workbench idles.
    cursorBlink: false,
    cursorStyle: 'bar',
    fontFamily: '"Geist Mono Variable", "Geist Mono", monospace',
    fontSize: 13,
    scrollback: 10_000,
    theme: {
      background: '#111715',
      foreground: '#d8e0db',
      cursor: '#e6b450',
      cursorAccent: '#111715',
      selectionBackground: '#345e52',
      black: '#111715',
      red: '#e06c75',
      green: '#8fb573',
      yellow: '#e6b450',
      blue: '#6c99bb',
      magenta: '#d290e4',
      cyan: '#7fb4ca',
      white: '#d8e0db',
      brightBlack: '#59645e',
      brightWhite: '#f4f7f5'
    }
  });
  const fit = new FitAddon();
  terminal.loadAddon(fit);
  terminal.open(host);
  const input = host.querySelector<HTMLTextAreaElement>('textarea[aria-label="Terminal input"]');
  if (!input) {
    terminal.dispose();
    throw new Error('Ghostty did not create its terminal input');
  }
  fit.fit();
  fit.observeResize();
  return new GhosttyTerminalSurface(terminal, fit, host, input);
}
