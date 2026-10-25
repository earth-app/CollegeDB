import type { ExecutionContext } from '@cloudflare/workers-types';
import { ShardCoordinator } from '../src/index';
import { alsOverlapProbe, alsRejectProbe, type AlsEnv, type AlsMode } from './als-probe';

export { ShardCoordinator };

export default {
	async fetch(request: Request, env: AlsEnv, ctx: ExecutionContext): Promise<Response> {
		const { pathname } = new URL(request.url);

		if (pathname === '/health') {
			return new Response('OK');
		}

		if (pathname !== '/api/probe/als' || request.method !== 'POST') {
			return new Response('Not Found', { status: 404 });
		}

		try {
			const body = (await request.json()) as { mode?: AlsMode; iterations?: number };
			const iterations = Math.max(1, Math.min(1000, Number(body.iterations) || 200));
			const report =
				body.mode === 'reject'
					? await alsRejectProbe(env, ctx, iterations)
					: await alsOverlapProbe(env, ctx, body.mode === 'unscoped' || body.mode === 'client' ? body.mode : 'scoped', iterations);
			return Response.json({ success: true, ...report });
		} catch (error) {
			return Response.json({ error: error instanceof Error ? error.message : String(error) }, { status: 500 });
		}
	}
};
