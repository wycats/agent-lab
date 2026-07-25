import { rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, sep } from 'node:path';

export default function globalTeardown() {
  const runId = process.env.AGENT_LAB_WEB_E2E_RUN_ID;
  if (!runId || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(runId)) {
    return;
  }

  const dataDir = resolve(join(tmpdir(), `agent-lab-web-e2e-${runId}`));
  const temporaryRoot = `${resolve(tmpdir())}${sep}`;
  if (
    dataDir.startsWith(temporaryRoot) &&
    dataDir.slice(temporaryRoot.length).startsWith('agent-lab-web-e2e-')
  ) {
    rmSync(dataDir, { force: true, recursive: true });
  }
}
