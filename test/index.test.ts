import { env } from 'cloudflare:workers';
import { Kysely, sql } from 'kysely';
import { beforeEach, describe, expect, it } from 'vitest';
import { D1Dialect } from '../src/index.ts';

interface UsersTable {
  id: number;
  name: string;
  email: string;
}

interface Database {
  users: UsersTable;
}

function createDb() {
  return new Kysely<Database>({ dialect: new D1Dialect({ database: env.DB }) });
}

describe('D1Dialect', () => {
  let db: Kysely<Database>;

  beforeEach(async () => {
    db = createDb();
    await db.deleteFrom('users').execute();
  });

  it('selects from an empty table', async () => {
    const rows = await db.selectFrom('users').selectAll().execute();
    expect(rows).toEqual([]);
  });

  it('inserts a row and returns insertId', async () => {
    const result = await db
      .insertInto('users')
      .values({ name: 'Alice', email: 'alice@example.com' })
      .executeTakeFirstOrThrow();

    expect(result.insertId).toBe(1n);
  });

  it('selects inserted rows', async () => {
    await db.insertInto('users').values({ name: 'Alice', email: 'alice@example.com' }).execute();
    await db.insertInto('users').values({ name: 'Bob', email: 'bob@example.com' }).execute();

    const rows = await db.selectFrom('users').selectAll().orderBy('id').execute();
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ name: 'Alice', email: 'alice@example.com' });
    expect(rows[1]).toMatchObject({ name: 'Bob', email: 'bob@example.com' });
  });

  it('updates a row and returns numAffectedRows', async () => {
    await db.insertInto('users').values({ name: 'Alice', email: 'alice@example.com' }).execute();

    const result = await db
      .updateTable('users')
      .set({ name: 'Alicia' })
      .where('email', '=', 'alice@example.com')
      .executeTakeFirstOrThrow();

    expect(result.numUpdatedRows).toBe(1n);

    const user = await db.selectFrom('users').selectAll().where('email', '=', 'alice@example.com').executeTakeFirst();
    expect(user?.name).toBe('Alicia');
  });

  it('deletes a row and returns numAffectedRows', async () => {
    await db.insertInto('users').values({ name: 'Alice', email: 'alice@example.com' }).execute();

    const result = await db
      .deleteFrom('users')
      .where('email', '=', 'alice@example.com')
      .executeTakeFirstOrThrow();

    expect(result.numDeletedRows).toBe(1n);

    const rows = await db.selectFrom('users').selectAll().execute();
    expect(rows).toHaveLength(0);
  });

  it('commits multiple writes in a transaction', async () => {
    await db.transaction().execute(async (trx) => {
      await trx.insertInto('users').values({ name: 'Alice', email: 'alice@example.com' }).execute();
      await trx.insertInto('users').values({ name: 'Bob', email: 'bob@example.com' }).execute();
    });

    const rows = await db.selectFrom('users').selectAll().orderBy('id').execute();
    expect(rows).toHaveLength(2);
    expect(rows[0].name).toBe('Alice');
    expect(rows[1].name).toBe('Bob');
  });

  it('executes raw SQL queries', async () => {
    await db.insertInto('users').values({ name: 'Alice', email: 'alice@example.com' }).execute();

    const rows = await sql<UsersTable>`SELECT * FROM users`.execute(db);
    expect(rows.rows).toHaveLength(1);
    expect(rows.rows[0].name).toBe('Alice');
  });

  it('works with a D1 session', async () => {
    const session = env.DB.withSession('first-primary');
    const sessionDb = new Kysely<Database>({ dialect: new D1Dialect({ database: session }) });

    await sessionDb.insertInto('users').values({ name: 'Alice', email: 'alice@example.com' }).execute();
    const rows = await sessionDb.selectFrom('users').selectAll().execute();
    expect(rows).toHaveLength(1);
    expect(session.getBookmark()).not.toBeNull();
  });

  it('rolls back transaction on error — no rows committed', async () => {
    await expect(
      db.transaction().execute(async (trx) => {
        await trx.insertInto('users').values({ name: 'Alice', email: 'alice@example.com' }).execute();
        throw new Error('forced rollback');
      }),
    ).rejects.toThrow('forced rollback');

    const rows = await db.selectFrom('users').selectAll().execute();
    expect(rows).toHaveLength(0);
  });

  it('runs a query while another connection is still held', async () => {
    // Kysely >= 0.29 puts every query behind one mutex when the adapter reports a single connection. On Workers, a
    // request cancelled while holding it never releases it and every later query waits forever. Holding a
    // connection open here stands in for that request.
    let acquired!: () => void;
    const isAcquired = new Promise<void>((resolve) => (acquired = resolve));
    let release!: () => void;
    const released = new Promise<void>((resolve) => (release = resolve));
    const holder = db.connection().execute(async () => {
      acquired();
      await released;
    });

    try {
      await isAcquired;
      const answer = sql<{ one: number }>`SELECT 1 AS one`.execute(db).then((result) => result.rows[0]?.one);
      const timedOut = new Promise<'timed out'>((resolve) => setTimeout(() => resolve('timed out'), 1000));
      expect(await Promise.race([answer, timedOut])).toBe(1);
    } finally {
      release();
      await holder;
    }
  });
});
