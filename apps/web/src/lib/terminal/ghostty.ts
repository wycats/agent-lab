import { FitAddon, Terminal, init } from 'ghostty-web';
import type { TerminalSurface, TerminalDimensions, Disposable } from './surface';

let initialization: Promise<void> | undefined;

function initializeGhostty(): Promise<void> {
  initialization ??= init();
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

  write(data: string | Uint8Array): void {
    this.#terminal.write(data);
  }

  readText(): string {
    const buffer = this.#terminal.buffer.active;
    const lines: string[] = [];
    for (let row = 0; row < buffer.length; row += 1) {
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
  const terminal = new Terminal({
    cols: 100,
    rows: 30,
    cursorBlink: true,
    cursorStyle: 'bar',
    fontFamily: '"Berkeley Mono", "SFMono-Regular", Consolas, monospace',
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
