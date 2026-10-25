import { vi } from 'vitest';

let spy: ReturnType<typeof vi.spyOn> | undefined;
let frozen = 0;

/** Freezes `Date.now()` at `ms`; the library reads time only through it, and both runners honor the spy. */
export function setClock(ms: number): void {
	frozen = ms;
	spy ??= vi.spyOn(Date, 'now').mockImplementation(() => frozen);
}

export function restoreClock(): void {
	spy?.mockRestore();
	spy = undefined;
}

/** Lets pending microtasks and zero-delay timers run on the real clock. */
export const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));
