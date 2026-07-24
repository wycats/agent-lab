export interface Disposable {
  dispose(): void;
}

export interface TerminalDimensions {
  cols: number;
  rows: number;
}

export interface TerminalSurface {
  readonly dimensions: TerminalDimensions;
  write(data: string | Uint8Array, afterWrite?: () => void): void;
  readText(): string;
  onData(listener: (data: string) => void): Disposable;
  onUserInput(listener: () => void): Disposable;
  onResize(listener: (dimensions: TerminalDimensions) => void): Disposable;
  onScroll(listener: () => void): Disposable;
  focus(): void;
  dispose(): void;
}

export type TerminalSurfaceFactory = (host: HTMLElement) => Promise<TerminalSurface>;
