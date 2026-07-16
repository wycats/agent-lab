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

  constructor(terminal: Terminal, fit: FitAddon) {
    this.#terminal = terminal;
    this.#fit = fit;
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
    const firstRow = Math.max(
      0,
      buffer.length - this.#terminal.rows - this.#terminal.viewportY
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

  onResize(listener: (dimensions: TerminalDimensions) => void): Disposable {
    return this.#terminal.onResize(listener);
  }

  onScroll(listener: () => void): Disposable {
    return this.#terminal.onScroll(() => listener());
  }

  focus(): void {
    this.#terminal.focus();
  }

  dispose(): void {
    this.#fit.dispose();
    this.#terminal.dispose();
  }
}

export async function createGhosttySurface(host: HTMLElement): Promise<TerminalSurface> {
  await initializeGhostty();
  await document.fonts.load('400 14px "Geist Mono Variable"');
  const terminal = new Terminal({
    cols: 100,
    rows: 30,
    cursorBlink: true,
    cursorStyle: 'bar',
    fontFamily: '"Geist Mono Variable", "Geist Mono", monospace',
    fontSize: 14,
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
  fit.fit();
  fit.observeResize();
  return new GhosttyTerminalSurface(terminal, fit);
}
