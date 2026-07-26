import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { once } from 'node:events';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const runId = process.env.AGENT_LAB_WEB_E2E_RUN_ID;
if (!runId || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(runId)) {
  throw new Error('AGENT_LAB_WEB_E2E_RUN_ID must be a UUID');
}
const port = Number(process.env.AGENT_LAB_WEB_E2E_PORT);
if (!Number.isSafeInteger(port) || port < 1024 || port > 65_535) {
  throw new Error('AGENT_LAB_WEB_E2E_PORT must be a valid unprivileged TCP port');
}
const dataDir = join(tmpdir(), `agent-lab-web-e2e-${runId}`);
mkdirSync(dataDir);
let child;
let stopping = false;

function cleanup() {
  const resolved = resolve(dataDir);
  const temporaryRoot = `${resolve(tmpdir())}${sep}`;
  if (
    resolved.startsWith(temporaryRoot) &&
    resolved.slice(temporaryRoot.length).startsWith('agent-lab-web-e2e-')
  ) {
    rmSync(resolved, { force: true, recursive: true });
  }
}

function start(command, args) {
  child = spawn(command, args, {
    cwd: repoRoot,
    env: process.env,
    stdio: 'inherit'
  });
  return child;
}

async function stop(signal) {
  if (stopping) return;
  stopping = true;
  if (child && child.exitCode === null && child.signalCode === null) {
    child.kill(signal);
    const forced = setTimeout(() => child?.kill('SIGKILL'), 5_000);
    await once(child, 'exit').catch(() => undefined);
    clearTimeout(forced);
  }
  cleanup();
  process.exit(signal === 'SIGINT' ? 130 : 143);
}

for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
  process.once(signal, () => void stop(signal));
}
process.once('exit', cleanup);

const build = start('cargo', [
  'build',
  '-p', 'agent-lab-nushell-mcp',
  '-p', 'agent-lab-driver-protocol',
  '-p', 'agent-lab-web',
  '--bins'
]);
const [buildCode] = await once(build, 'exit');
if (buildCode !== 0) {
  cleanup();
  process.exit(typeof buildCode === 'number' ? buildCode : 1);
}

const server = start(join(repoRoot, 'target/debug/agent-lab-web'), [
  '--port', String(port),
  '--data', dataDir,
  '--harness-config', 'apps/web/tests/fixtures/harnesses.toml'
]);
const [serverCode] = await once(server, 'exit');
cleanup();
process.exit(typeof serverCode === 'number' ? serverCode : 1);
