import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { isDeepStrictEqual } from 'node:util';
import { overlayPins } from './overlay.mjs';
import { parse, splitStatements } from './sql.mjs';
import { compileIndex, compileTable } from './schema.mjs';

const filename = 'src/lib/db/core.ts';
const source = new URL('../upstream/omniroute/' + filename, import.meta.url);
export const artifactSchemaSql='CREATE TABLE IF NOT EXISTS ha_call_log_artifacts(path TEXT PRIMARY KEY,payload TEXT NOT NULL,mtime_ms INTEGER NOT NULL)';

export function readBaseSchema(text = readFileSync(source, 'utf8')) {
  if (createHash('sha256').update(text).digest('hex') !== overlayPins[filename]) throw new Error('Base schema source digest mismatch');
  const matches = [...text.matchAll(/const SCHEMA_SQL = `([\s\S]*?)`;/g)];
  if (matches.length !== 1 || matches[0][1].includes('${')) throw new Error('Expected one static upstream schema literal');
  const tables = new Map(), indexes = [], statements = splitStatements(matches[0][1]);
  for (const sql of statements) {
    const { ast } = parse(sql);
    if (!ast.if_not_exists || ast.type !== 'create') throw new Error('Unexpected baseline SQL');
    if (ast.keyword === 'table') {
      const spec = compileTable(ast);
      if (tables.has(spec._id)) throw new Error('Duplicate base table');
      tables.set(spec._id, spec);
    } else if (ast.keyword === 'index' && tables.has(ast.table.table)) {
      indexes.push({ table: ast.table.table, ...compileIndex(ast, tables.get(ast.table.table)) });
    } else throw new Error('Unrecognized base schema statement');
  }
  return { statements, tables, indexes };
}

// Additional columns, tables and indexes from later migrations are permitted.
// A missing collection, changed constraint/default, or wrong index order is not.
export function checkBaseSchema(plan, specs, catalog) {
  const fail = message => { throw Object.assign(new Error('MongoDB base schema mismatch: ' + message), { code: 'HA_SCHEMA_MISMATCH' }); };
  const byName = new Map(specs.map(spec => [spec._id, spec]));
  for (const [name, expected] of plan.tables) {
    const spec = byName.get(name);
    if (!spec || !catalog.has(name)) fail(name);
    if (!isDeepStrictEqual(spec.primaryKey, expected.primaryKey)) fail(name + ' primary key');
    for (const key of expected.unique) {
      if (!spec.unique?.some(actual => isDeepStrictEqual(actual, key))) fail(name + ' unique key');
    }
    const required = new Set(expected.columns.map(c => c.name));
    if (!Array.isArray(spec.columns) || !isDeepStrictEqual(spec.columns.filter(c => required.has(c.name)).map(c => c.name), [...required])) fail(name + ' column order');
    for (const column of expected.columns) {
      const actual = spec.columns?.find(c => c.name === column.name);
      if (!actual) fail(name + '.' + column.name);
      for (const field of ['type','primary','notNull','autoIncrement','default','defaultSql','defaultKind','allowed']) {
        if (!isDeepStrictEqual(actual[field] ?? null, column[field] ?? null)) fail(name + '.' + column.name + ' ' + field);
      }
    }
  }
  for (const index of plan.indexes) {
    const actual = catalog.get(index.table)?.find(i => i.name === index.name);
    if (!actual || !isDeepStrictEqual(Object.entries(actual.key), Object.entries(index.keys)) ||
      actual.unique || actual.sparse || actual.hidden || actual.collation || actual.expireAfterSeconds != null ||
      !isDeepStrictEqual(actual.partialFilterExpression ?? null, index.partialFilterExpression ?? null)) fail(index.name);
  }
  return { tables: plan.tables.size, indexes: plan.indexes.length };
}

export function installBaseSchema(adapter) {
  if (adapter.requiresOwnership) throw new Error('Install the base schema offline before starting HA instances');
  for (const sql of readBaseSchema().statements) adapter.exec(sql);
  return adapter.verifyBaseSchema();
}
