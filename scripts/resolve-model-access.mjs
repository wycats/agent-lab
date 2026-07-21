import { getVercelOidcToken } from '@vercel/oidc';

const OIDC_EXPIRATION_BUFFER_MS = 5 * 60 * 1000;
const mode = process.argv[2] ?? 'probe';
if (mode !== 'probe' && mode !== 'resolve') {
  process.stderr.write('usage: resolve-model-access.mjs [probe|resolve]\n');
  process.exit(2);
}

const ambientCredentials = [
  ['AI_GATEWAY_API_KEY', 'ai-gateway-api-key'],
  ['VERCEL_OIDC_TOKEN', 'vercel-oidc'],
  ['ANTHROPIC_API_KEY', 'anthropic-api-key'],
];

for (const [name, source] of ambientCredentials) {
  const value = process.env[name]?.trim();
  if (value) {
    const expiresAtMs = name === 'VERCEL_OIDC_TOKEN' ? jwtExpiry(value) : null;
    if (
      name === 'VERCEL_OIDC_TOKEN' &&
      (expiresAtMs === null || expiresAtMs <= Date.now() + OIDC_EXPIRATION_BUFFER_MS)
    ) {
      continue;
    }
    respond({
      status: 'ready',
      source,
      expiresAtMs,
      environment: mode === 'resolve' ? { [name]: value } : undefined,
    });
  }
}

try {
  const token = (await getVercelOidcToken({ expirationBufferMs: OIDC_EXPIRATION_BUFFER_MS })).trim();
  if (!token) throw new Error('empty token');
  respond({
    status: 'ready',
    source: 'vercel-project-oidc',
    expiresAtMs: jwtExpiry(token),
    environment: mode === 'resolve' ? { VERCEL_OIDC_TOKEN: token } : undefined,
  });
} catch {
  respond({
    status: 'needs-setup',
    source: null,
    expiresAtMs: null,
    message: 'Link Agent Lab to a Vercel project or provide an AI Gateway credential.',
  });
}

function jwtExpiry(token) {
  try {
    const payload = JSON.parse(Buffer.from(token.split('.')[1] ?? '', 'base64url').toString('utf8'));
    return typeof payload.exp === 'number' ? payload.exp * 1000 : null;
  } catch {
    return null;
  }
}

function respond(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
  process.exit(0);
}
