import { promises as fs, existsSync } from 'fs';
import path from 'path';
import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

export interface Dataset {
  id: string;
  name: string;
  description: string;
  type: string;
  pricePerQuery: number;
  sellerWallet: string;
  data: Record<string, unknown>;
  queriesServed: number;
  totalEarned: number;
  createdAt: string;
}

export interface Transaction {
  id: string;
  datasetId: string;
  txHash: string;
  amount: number;
  buyerQuery?: string;
  aiSummary?: string;
  timestamp: string;
}

export type WebhookEvent =
  | 'payment.received'
  | 'payment.forwarded'
  | 'dataset.queried'
  | 'dataset.created'
  | 'ping';

export interface WebhookSubscription {
  id: string;
  sellerWallet: string;
  url: string;
  secret: string;
  events: WebhookEvent[];
  active: boolean;
  createdAt: string;
}

export interface Store {
  datasets: Dataset[];
  transactions: Transaction[];
  webhooks: WebhookSubscription[];
}

// Serialize all mutations to prevent concurrent read-modify-write data loss
let mutationQueue: Promise<void> = Promise.resolve();

// In-memory set to block replay of hashes that are mid-flight (not yet persisted)
const pendingTxHashes = new Set<string>();

async function readRaw(): Promise<Store> {
  if (!existsSync(DATA_PATH)) {
    const empty: Store = { datasets: [], transactions: [], webhooks: [] };
    await fs.writeFile(DATA_PATH, JSON.stringify(empty, null, 2), 'utf-8');
    return empty;
  }
  const raw = await fs.readFile(DATA_PATH, 'utf-8');
  const parsed = JSON.parse(raw) as Partial<Store>;
  if (!parsed.webhooks) parsed.webhooks = [];
  return parsed as Store;
}

export async function readStore(): Promise<Store> {
  return readRaw();
}

export async function writeStore(store: Store): Promise<void> {
  // Enqueue so concurrent external writes don't interleave
  mutationQueue = mutationQueue.then(() =>
    fs.writeFile(DATA_PATH, JSON.stringify(store, null, 2), 'utf-8'),
  );
  return mutationQueue;
}

// Runs fn inside the serialized queue. fn receives the current store and must
// return the (possibly mutated) store to persist, plus an optional result.
function enqueue<T>(fn: (store: Store) => Promise<[Store, T]>): Promise<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const result = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  mutationQueue = mutationQueue.then(async () => {
    try {
      const store = await readRaw();
      const [updated, value] = await fn(store);
      await fs.writeFile(DATA_PATH, JSON.stringify(updated, null, 2), 'utf-8');
      resolve(value);
    } catch (err) {
      reject(err);
    }
  });
  return result;
}

export async function getDataset(id: string): Promise<Dataset | undefined> {
  return (await readStore()).datasets.find((d) => d.id === id);
}

export async function getAllDatasets(): Promise<Dataset[]> {
  return (await readStore()).datasets;
}

export async function updateDataset(id: string, updates: Partial<Dataset>): Promise<Dataset | null> {
  return enqueue(async (store) => {
    const idx = store.datasets.findIndex((d) => d.id === id);
    if (idx === -1) return [store, null];
    store.datasets[idx] = { ...store.datasets[idx], ...updates };
    return [store, store.datasets[idx]];
  });
}

export async function addDataset(dataset: Dataset): Promise<void> {
  return enqueue(async (store) => {
    store.datasets.push(dataset);
    return [store, undefined];
  });
}

export async function addTransaction(tx: Transaction): Promise<void> {
  pendingTxHashes.add(tx.txHash);
  return enqueue(async (store) => {
    store.transactions.push(tx);
    return [store, undefined];
  }).finally(() => {
    pendingTxHashes.delete(tx.txHash);
  });
}

export async function getTransactions(datasetId?: string, limit?: number, offset?: number): Promise<Transaction[]> {
  const store = await readStore();
  let transactions = datasetId ? store.transactions.filter((t) => t.datasetId === datasetId) : store.transactions;

  if (offset !== undefined && offset > 0) {
    transactions = transactions.slice(offset);
  }

  if (limit !== undefined && limit > 0) {
    transactions = transactions.slice(0, limit);
  }

  return transactions;
}

export async function getTransactionsCount(datasetId?: string): Promise<number> {
  const store = await readStore();
  return datasetId ? store.transactions.filter((t) => t.datasetId === datasetId).length : store.transactions.length;
}

export async function txHashUsed(txHash: string): Promise<boolean> {
  if (pendingTxHashes.has(txHash)) return true;
  return (await readStore()).transactions.some((t) => t.txHash === txHash);
export async function getDataset(id: string): Promise<Dataset | undefined> {
  const { rows } = await pool.query<Dataset>(
    'SELECT * FROM datasets WHERE id = $1',
    [id],
  );
  return rows[0];
}

export async function getAllDatasets(): Promise<Dataset[]> {
  const { rows } = await pool.query<Dataset>('SELECT * FROM datasets ORDER BY created_at DESC');
  return rows;
}

export async function updateDataset(id: string, updates: Partial<Dataset>): Promise<Dataset | null> {
  const fields = Object.keys(updates) as (keyof Dataset)[];
  if (fields.length === 0) return getDataset(id) ?? null;

  const setClauses = fields.map((f, i) => `"${toSnake(f)}" = $${i + 2}`).join(', ');
  const values = fields.map((f) => updates[f]);

  const { rows } = await pool.query<Dataset>(
    `UPDATE datasets SET ${setClauses} WHERE id = $1 RETURNING *`,
    [id, ...values],
  );
  return rows[0] ?? null;
}

export async function addDataset(dataset: Dataset): Promise<void> {
  await pool.query(
    `INSERT INTO datasets
       (id, name, description, type, price_per_query, seller_wallet, data,
        queries_served, total_earned, created_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
    [
      dataset.id,
      dataset.name,
      dataset.description,
      dataset.type,
      dataset.pricePerQuery,
      dataset.sellerWallet,
      JSON.stringify(dataset.data),
      dataset.queriesServed,
      dataset.totalEarned,
      dataset.createdAt,
    ],
  );
}

export async function addTransaction(tx: Transaction): Promise<void> {
  await pool.query(
    `INSERT INTO transactions
       (id, dataset_id, tx_hash, amount, buyer_query, ai_summary, timestamp)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [tx.id, tx.datasetId, tx.txHash, tx.amount, tx.buyerQuery ?? null, tx.aiSummary ?? null, tx.timestamp],
  );
}

export async function getTransactions(
  datasetId?: string,
  limit?: number,
  offset?: number,
): Promise<Transaction[]> {
  const conditions: string[] = [];
  const values: unknown[] = [];

  if (datasetId) {
    values.push(datasetId);
    conditions.push(`dataset_id = $${values.length}`);
  }

  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  let query = `SELECT * FROM transactions ${where} ORDER BY timestamp DESC`;

  if (limit !== undefined && limit > 0) {
    values.push(limit);
    query += ` LIMIT $${values.length}`;
  }
  if (offset !== undefined && offset > 0) {
    values.push(offset);
    query += ` OFFSET $${values.length}`;
  }

  const { rows } = await pool.query<Transaction>(query, values);
  return rows;
}

export async function getTransactionsCount(datasetId?: string): Promise<number> {
  const { rows } = datasetId
    ? await pool.query<{ count: string }>('SELECT COUNT(*) FROM transactions WHERE dataset_id = $1', [datasetId])
    : await pool.query<{ count: string }>('SELECT COUNT(*) FROM transactions');
  return parseInt(rows[0].count, 10);
}

export async function txHashUsed(txHash: string): Promise<boolean> {
  const { rows } = await pool.query<{ count: string }>(
    'SELECT COUNT(*) FROM transactions WHERE tx_hash = $1',
    [txHash],
  );
  return parseInt(rows[0].count, 10) > 0;
}

/* ------------------------------------------------------------------ */
/*  Webhooks                                                           */
/* ------------------------------------------------------------------ */

export async function getAllWebhooks(): Promise<WebhookSubscription[]> {
  const { rows } = await pool.query<WebhookSubscription>('SELECT * FROM webhooks');
  return rows;
}

export async function getWebhooksForSeller(sellerWallet: string): Promise<WebhookSubscription[]> {
  const { rows } = await pool.query<WebhookSubscription>(
    'SELECT * FROM webhooks WHERE seller_wallet = $1 AND active = true',
    [sellerWallet],
  );
  return rows;
}

export async function getWebhookById(id: string): Promise<WebhookSubscription | undefined> {
  const { rows } = await pool.query<WebhookSubscription>('SELECT * FROM webhooks WHERE id = $1', [id]);
  return rows[0];
}

export async function addWebhook(webhook: WebhookSubscription): Promise<void> {
  await pool.query(
    `INSERT INTO webhooks (id, seller_wallet, url, secret, events, active, created_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [webhook.id, webhook.sellerWallet, webhook.url, webhook.secret, webhook.events, webhook.active, webhook.createdAt],
  );
}

export async function removeWebhook(id: string): Promise<boolean> {
  const { rowCount } = await pool.query('DELETE FROM webhooks WHERE id = $1', [id]);
  return (rowCount ?? 0) > 0;
}

export async function updateWebhook(
  id: string,
  updates: Partial<WebhookSubscription>,
): Promise<WebhookSubscription | null> {
  const fields = Object.keys(updates) as (keyof WebhookSubscription)[];
  if (fields.length === 0) return getWebhookById(id) ?? null;

export async function getAllWebhooks(): Promise<WebhookSubscription[]> {
  return (await readStore()).webhooks;
}

export async function getWebhooksForSeller(sellerWallet: string): Promise<WebhookSubscription[]> {
  return (await readStore()).webhooks.filter((w) => w.sellerWallet === sellerWallet && w.active);
}

export async function getWebhookById(id: string): Promise<WebhookSubscription | undefined> {
  return (await readStore()).webhooks.find((w) => w.id === id);
}

export async function addWebhook(webhook: WebhookSubscription): Promise<void> {
  return enqueue(async (store) => {
    store.webhooks.push(webhook);
    return [store, undefined];
  });
}

export async function removeWebhook(id: string): Promise<boolean> {
  return enqueue(async (store) => {
    const idx = store.webhooks.findIndex((w) => w.id === id);
    if (idx === -1) return [store, false];
    store.webhooks.splice(idx, 1);
    return [store, true];
  });
}

export async function updateWebhook(id: string, updates: Partial<WebhookSubscription>): Promise<WebhookSubscription | null> {
  return enqueue(async (store) => {
    const idx = store.webhooks.findIndex((w) => w.id === id);
    if (idx === -1) return [store, null];
    store.webhooks[idx] = { ...store.webhooks[idx], ...updates };
    return [store, store.webhooks[idx]];
  });
  const setClauses = fields.map((f, i) => `"${toSnake(f)}" = $${i + 2}`).join(', ');
  const values = fields.map((f) => updates[f]);

  const { rows } = await pool.query<WebhookSubscription>(
    `UPDATE webhooks SET ${setClauses} WHERE id = $1 RETURNING *`,
    [id, ...values],
  );
  return rows[0] ?? null;
}

/* ------------------------------------------------------------------ */
/*  Schema bootstrap (run once on startup)                            */
/* ------------------------------------------------------------------ */

export async function ensureSchema(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS datasets (
      id              TEXT PRIMARY KEY,
      name            TEXT NOT NULL,
      description     TEXT NOT NULL,
      type            TEXT NOT NULL,
      price_per_query NUMERIC NOT NULL,
      seller_wallet   TEXT NOT NULL,
      data            JSONB NOT NULL DEFAULT '{}',
      queries_served  INTEGER NOT NULL DEFAULT 0,
      total_earned    NUMERIC NOT NULL DEFAULT 0,
      created_at      TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS transactions (
      id          TEXT PRIMARY KEY,
      dataset_id  TEXT NOT NULL REFERENCES datasets(id),
      tx_hash     TEXT NOT NULL UNIQUE,
      amount      NUMERIC NOT NULL,
      buyer_query TEXT,
      ai_summary  TEXT,
      timestamp   TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS webhooks (
      id            TEXT PRIMARY KEY,
      seller_wallet TEXT NOT NULL,
      url           TEXT NOT NULL,
      secret        TEXT NOT NULL,
      events        TEXT[] NOT NULL DEFAULT '{}',
      active        BOOLEAN NOT NULL DEFAULT true,
      created_at    TEXT NOT NULL
    );
  `);
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function toSnake(camel: string): string {
  return camel.replace(/([A-Z])/g, '_$1').toLowerCase();
}
