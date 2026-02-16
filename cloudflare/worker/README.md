Quick Cloudflare Worker proxy/redirect for streaming

Usage summary
- `mode=redirect`: Worker responds with a 302 redirect to the upstream `url` (recommended for scale).
- `mode=proxy`: Worker fetches upstream and streams the response back (useful for private origins or header rewriting).

Deploy
1. Install Wrangler (`npm i -g wrangler`) and login to your Cloudflare account.
2. Create a Worker project and replace the script with `worker.js` contents.
3. Deploy with `wrangler publish` and use the Worker URL.

Example
- Direct redirect: `https://<worker>/?mode=redirect&url=https://example.com/video.mp4`
- Proxy stream: `https://<worker>/?mode=proxy&url=https://example.com/video.mp4`

Notes
- Redirect is cheapest and delegates bandwidth to the origin/CDN.
- Proxy preserves/rewrites headers and enables private-origin streaming, but can increase Worker egress and execution usage.
- Ensure upstream supports `Range` requests for seeking.
