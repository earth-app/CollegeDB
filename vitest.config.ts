import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		globals: true,
		environment: 'node',
		pool: 'forks',
		coverage: {
			provider: 'v8',
			reporter: ['text', 'json', 'html', 'clover'],
			exclude: ['node_modules/', 'test/', 'dist/', 'coverage/', '**/*.d.ts', 'vitest.config.ts', 'demos/', 'examples/']
		}
	}
});
