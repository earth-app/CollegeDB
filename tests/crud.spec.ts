import { beforeEach, describe, expect, it } from 'vitest';
import {
	createInMemoryKVProvider,
	createInMemorySQLProvider,
	deleteById,
	deleteRow,
	first,
	initialize,
	insertInto,
	insertReturning,
	patch,
	resetConfig,
	updateRow,
	upsert
} from '../src/index';

const SCHEMA = `CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, qty INTEGER, note TEXT)`;

describe('Object CRUD helpers', () => {
	beforeEach(async () => {
		resetConfig();
		const kv = createInMemoryKVProvider();
		const east = createInMemorySQLProvider();
		const west = createInMemorySQLProvider();
		await east.prepare(SCHEMA).run();
		await west.prepare(SCHEMA).run();

		initialize({
			kv,
			shards: { 'db-east': east, 'db-west': west },
			strategy: 'hash',
			disableAutoMigration: true,
			hashShardMappings: false
		});
	});

	it('insertInto writes a routed row that first() reads back', async () => {
		await insertInto('item-1', 'items', { id: 'item-1', name: 'Widget', qty: 3 });

		const row = await first<{ id: string; name: string; qty: number }>('item-1', 'SELECT * FROM items WHERE id = ?', ['item-1']);
		expect(row).toMatchObject({ id: 'item-1', name: 'Widget', qty: 3 });
	});

	it('insertReturning returns the created row', async () => {
		const created = await insertReturning<{ id: string; name: string }>('item-2', 'items', { id: 'item-2', name: 'Gadget' });
		expect(created).toMatchObject({ id: 'item-2', name: 'Gadget' });
	});

	it('patch applies a partial update by id', async () => {
		await insertInto('item-3', 'items', { id: 'item-3', name: 'Old', qty: 1 });
		await patch('item-3', 'items', 'item-3', { name: 'New', qty: 9 });

		const row = await first<{ name: string; qty: number }>('item-3', 'SELECT * FROM items WHERE id = ?', ['item-3']);
		expect(row).toMatchObject({ name: 'New', qty: 9 });
	});

	it('updateRow scopes updates by a where map', async () => {
		await insertInto('item-4', 'items', { id: 'item-4', name: 'A', qty: 0 });
		const result = await updateRow('item-4', 'items', { qty: 5 }, { id: 'item-4' });
		expect(result.success).toBe(true);

		const row = await first<{ qty: number }>('item-4', 'SELECT qty FROM items WHERE id = ?', ['item-4']);
		expect(row?.qty).toBe(5);
	});

	it('deleteById removes the routed row', async () => {
		await insertInto('item-5', 'items', { id: 'item-5', name: 'Temp' });
		await deleteById('item-5', 'items', 'item-5');

		const row = await first('item-5', 'SELECT * FROM items WHERE id = ?', ['item-5']);
		expect(row).toBeNull();
	});

	it('deleteRow removes rows matching a where map', async () => {
		await insertInto('item-6', 'items', { id: 'item-6', name: 'ByName' });
		await deleteRow('item-6', 'items', { name: 'ByName' });

		const row = await first('item-6', 'SELECT * FROM items WHERE id = ?', ['item-6']);
		expect(row).toBeNull();
	});

	it('upsert inserts when new and updates the conflict columns on repeat', async () => {
		await upsert('item-7', 'items', { id: 'item-7', name: 'First', qty: 1, note: 'keep' }, 'id', { update: ['name', 'qty'] });
		await upsert('item-7', 'items', { id: 'item-7', name: 'Second', qty: 2, note: 'ignored' }, 'id', { update: ['name', 'qty'] });

		const row = await first<{ name: string; qty: number; note: string }>('item-7', 'SELECT * FROM items WHERE id = ?', ['item-7']);
		expect(row).toMatchObject({ name: 'Second', qty: 2, note: 'keep' });
	});

	it('routes writes and reads to the same shard by key', async () => {
		// The same key must resolve to the same shard for write and read.
		await insertInto('deterministic-key', 'items', { id: 'deterministic-key', name: 'Routed' });
		const row = await first<{ name: string }>('deterministic-key', 'SELECT name FROM items WHERE id = ?', ['deterministic-key']);
		expect(row?.name).toBe('Routed');
	});

	it('rejects an unfiltered delete before touching a shard', async () => {
		await expect(deleteRow('item-8', 'items', {})).rejects.toThrow('A WHERE condition is required');
	});
});
