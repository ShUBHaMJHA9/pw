addEventListener('fetch', event => {
  event.respondWith(handle(event.request));
});

async function handle(request) {
  const requestUrl = new URL(request.url);
  const target = requestUrl.searchParams.get('url');
  const mode = requestUrl.searchParams.get('mode') || 'proxy';

  // CORS helper
  const corsHeaders = new Headers({
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,HEAD,OPTIONS',
    'Access-Control-Allow-Headers': 'Range,Content-Type,Authorization',
    'Access-Control-Expose-Headers': 'Content-Length,Content-Range,Accept-Ranges',
  });

  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders });
  }

  if (!target) {
    return new Response('Missing "url" query parameter', { status: 400, headers: corsHeaders });
  }

  if (mode === 'redirect') {
    // Simple redirect to target (cheap, scales well)
    return Response.redirect(target, 302);
  }

  // Proxy mode: forward request to upstream and stream response back
  const upstreamReq = new Request(target, {
    method: request.method,
    headers: request.headers,
    body: request.body,
    redirect: 'manual',
  });

  const upstreamRes = await fetch(upstreamReq);

  // Copy headers and ensure streaming-friendly entries
  const resHeaders = new Headers(upstreamRes.headers);
  if (!resHeaders.has('Accept-Ranges')) resHeaders.set('Accept-Ranges', 'bytes');
  resHeaders.set('Access-Control-Allow-Origin', '*');
  resHeaders.set('Access-Control-Expose-Headers', 'Content-Length,Content-Range,Accept-Ranges');

  return new Response(upstreamRes.body, {
    status: upstreamRes.status,
    statusText: upstreamRes.statusText,
    headers: resHeaders,
  });
}

export {};
