import {
  AbortableOperationOptions,
  CompiledQuery,
  DatabaseConnection,
  DatabaseIntrospector,
  Dialect,
  Driver,
  Kysely,
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
  QueryCompiler,
  QueryResult,
} from 'kysely';
import type { D1Database, D1DatabaseSession, D1PreparedStatement } from '@cloudflare/workers-types';

/**
 * Config for the D1 dialect. Pass your D1 instance to this object that you bound in `wrangler.toml`.
 *
 * To use D1's read replication with sequential consistency, pass a session created via
 * `database.withSession()` instead of the raw database:
 *
 * ```typescript
 * const session = env.DB.withSession('first-primary');
 * const db = new Kysely({ dialect: new D1Dialect({ database: session }) });
 * const bookmark = session.getBookmark(); // forward to next request for consistency
 * ```
 */
export interface D1DialectConfig {
  database: D1Database | D1DatabaseSession;
}

/**
 * D1 dialect that adds support for [Cloudflare D1][0] in [Kysely][1].
 * The constructor takes the instance of your D1 database that you bound in `wrangler.toml`.
 *
 * ```typescript
 * new D1Dialect({
 *   database: env.DB,
 * })
 * ```
 *
 * [0]: https://blog.cloudflare.com/introducing-d1/
 * [1]: https://github.com/koskimas/kysely
 */
export class D1Dialect implements Dialect {
  #config: D1DialectConfig;

  constructor(config: D1DialectConfig) {
    this.#config = config;
  }

  createAdapter(): SqliteAdapter {
    return new D1Adapter();
  }

  createDriver(): Driver {
    return new D1Driver(this.#config);
  }

  createQueryCompiler(): QueryCompiler {
    return new SqliteQueryCompiler();
  }

  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    return new SqliteIntrospector(db);
  }
}

/**
 * SQLite dialect adapter for D1, which is a binding rather than a single connection: every `acquireConnection()`
 * returns a fresh `D1Connection` and nothing is shared between them (a transaction's statements live on its own
 * connection).
 *
 * Since 0.29, Kysely serializes every query behind one mutex when the adapter reports
 * `supportsMultipleConnections === false`, as `SqliteAdapter` does. On Workers a Kysely instance usually lives as
 * long as the isolate, so that mutex is shared by every request, and a request cancelled while holding it (client
 * disconnect, `waitUntil` past its limit) never releases it: every later query in the isolate then waits forever.
 */
class D1Adapter extends SqliteAdapter {
  get supportsMultipleConnections(): boolean {
    return true;
  }
}

class D1Driver implements Driver {
  #config: D1DialectConfig;

  constructor(config: D1DialectConfig) {
    this.#config = config;
  }

  async init(): Promise<void> {}

  async acquireConnection(): Promise<DatabaseConnection> {
    return new D1Connection(this.#config);
  }

  async beginTransaction(conn: D1Connection): Promise<void> {
    return await conn.beginTransaction();
  }

  async commitTransaction(conn: D1Connection): Promise<void> {
    return await conn.commitTransaction();
  }

  async rollbackTransaction(conn: D1Connection): Promise<void> {
    return await conn.rollbackTransaction();
  }

  async releaseConnection(_conn: D1Connection): Promise<void> {}

  async destroy(): Promise<void> {}
}

class D1Connection implements DatabaseConnection {
  #config: D1DialectConfig;
  #transactionStatements: D1PreparedStatement[] | undefined;

  constructor(config: D1DialectConfig) {
    this.#config = config;
  }

  async executeQuery<O>(compiledQuery: CompiledQuery, _options?: AbortableOperationOptions): Promise<QueryResult<O>> {
    const { query, sql, parameters } = compiledQuery;
    const stmt = this.#config.database.prepare(sql).bind(...parameters);

    if (this.#transactionStatements) {
      // Buffer statement for batch execution at commit time.
      this.#transactionStatements.push(stmt);
      return { rows: [], numAffectedRows: undefined, insertId: undefined };
    }

    // Use .run() only for known mutations without RETURNING — it reliably populates
    // meta.changes. Everything else (SELECT, raw SQL, DDL) uses .all() so rows are
    // always returned when the caller expects them.
    const isMutation =
      query.kind === 'InsertQueryNode' ||
      query.kind === 'UpdateQueryNode' ||
      query.kind === 'DeleteQueryNode' ||
      query.kind === 'MergeQueryNode';
    const hasReturning = 'returning' in query && query.returning !== undefined;
    const useRun = isMutation && !hasReturning;

    const results = useRun ? await stmt.run<O>() : await stmt.all<O>();

    const numAffectedRows = results.meta.changes > 0 ? BigInt(results.meta.changes) : undefined;

    return {
      insertId:
        results.meta.last_row_id === undefined || results.meta.last_row_id === null
          ? undefined
          : BigInt(results.meta.last_row_id),
      rows: results.results,
      numAffectedRows,
    };
  }

  async beginTransaction(): Promise<void> {
    this.#transactionStatements = [];
  }

  async commitTransaction(): Promise<void> {
    if (this.#transactionStatements) {
      await this.#config.database.batch(this.#transactionStatements);
      this.#transactionStatements = undefined;
    }
  }

  async rollbackTransaction(): Promise<void> {
    this.#transactionStatements = undefined;
  }

  async *streamQuery<O>(
    _compiledQuery: CompiledQuery,
    _chunkSize: number,
    _options?: AbortableOperationOptions,
  ): AsyncIterableIterator<QueryResult<O>> {
    throw new Error('D1 Driver does not support streaming');
  }
}
