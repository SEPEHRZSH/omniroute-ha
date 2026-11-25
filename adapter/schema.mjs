import { unsupported } from './sql.mjs';
export const columnTypes=Object.freeze(['TEXT','INTEGER','REAL','BLOB','NUMERIC','BOOLEAN','DATETIME']);
export const generatedDefaults=Object.freeze(['datetime-now','utc-seconds','random-hex8']);
export function assertStoredSchema(spec) {
  if(!Array.isArray(spec?.columns)||spec.columns.some(column=>!columnTypes.includes(column.type)||column.defaultKind!=null&&!generatedDefaults.includes(column.defaultKind)))
    throw Object.assign(new Error('Unknown type or generated default in stored schema; use the matching reviewed adapter release'),{code:'HA_SCHEMA_MISMATCH'});
}

// Keep the upstream migration ledger separate from HA control tables.
const validIdentifier = name => typeof name === 'string' && (/^[A-Za-z][A-Za-z0-9_]*$/.test(name)||name==='_omniroute_migrations');
export function checkIdentifier(name) { if (!validIdentifier(name)) unsupported('identifier'); return name; }
export function literal(node, bindings) {
  if (!node || node.type === 'null') return null;
  if (node.type === 'number') {
    if (!Number.isFinite(Number(node.value))) unsupported('nonfinite numeric literal');
    return Number(node.value);
  }
  if (['single_quote_string', 'string'].includes(node.type)) return node.value;
  if (node.type === 'function' && node.name?.name?.[0]?.value === '__ha_bind__') return bindings[node.args.value[0].value];
  unsupported('non-literal INSERT/default expression');
}
export function currentTimestamp(node) {
  return node?.type === 'function' && node.name?.name?.length === 1 && !node.over &&
    node.name.name[0].value?.toUpperCase() === 'CURRENT_TIMESTAMP' && !node.args;
}
export function onlyKeys(value, allowed, context) {
  for (const [key, entry] of Object.entries(value)) {
    if (!allowed.includes(key) && entry != null && !(Array.isArray(entry) && entry.length === 0)) unsupported(`${context}.${key}`);
  }
}
export function columnDefinition(def) {
  onlyKeys(def, ['collate', 'reference_definition', 'column', 'definition', 'resource', 'nullable', 'primary_key', 'auto_increment', 'unique', 'default_val', 'check', 'action', 'keyword', 'type'], 'column definition');
  onlyKeys(def.definition, ['dataType'], 'column type');
  if (def.resource !== 'column' || def.generated) unsupported('table constraint');
  const defaultNode = def.default_val?.value;
  const call=(node,name,size)=>node?.type==='function'&&!node.over&&node.name?.name?.length===1&&node.name.name[0].value?.toLowerCase()===name&&node.args?.value?.length===size;
  const randomHex=call(defaultNode,'lower',1)&&call(defaultNode.args.value[0],'hex',1)&&call(defaultNode.args.value[0].args.value[0],'randomblob',1)&&
    defaultNode.args.value[0].args.value[0].args.value[0]?.type==='number'&&defaultNode.args.value[0].args.value[0].args.value[0].value===8;
  const utcDefault=call(defaultNode,'strftime',2)&&defaultNode.args.value.every(arg=>['single_quote_string','string'].includes(arg.type))&&
    defaultNode.args.value[0].value==='%Y-%m-%dT%H:%M:%SZ'&&defaultNode.args.value[1].value==='now';
  const timestampDefault = currentTimestamp(defaultNode);
  const datetimeDefault = timestampDefault || (defaultNode?.type === 'function' && defaultNode.name?.name?.[0]?.value?.toLowerCase() === 'datetime' &&
    defaultNode.args?.value?.length === 1 && defaultNode.args.value[0].value === 'now');
  const column = { name: checkIdentifier(def.column.column), type: def.definition.dataType.toUpperCase(),
    notNull: def.nullable?.type === 'not null', primary: !!def.primary_key,
    autoIncrement: !!def.auto_increment,
    default: datetimeDefault||randomHex||utcDefault ? null : defaultNode?.type === 'double_quote_string' ? defaultNode.value : literal(defaultNode, []),
    defaultSql: !defaultNode ? null : randomHex?'lower(hex(randomblob(8)))':utcDefault?"strftime('%Y-%m-%dT%H:%M:%SZ', 'now')":timestampDefault ? 'CURRENT_TIMESTAMP' : datetimeDefault ? "datetime('now')" :
      defaultNode.type === 'null' ? 'NULL' : defaultNode.type === 'number' ? String(defaultNode.value) :
      defaultNode.type === 'double_quote_string' ? '"' + defaultNode.value.replaceAll('"', '""') + '"' :
      "'" + String(literal(defaultNode, [])).replaceAll("'", "''") + "'",
    defaultKind:randomHex?'random-hex8':utcDefault?'utc-seconds':datetimeDefault ? 'datetime-now' : undefined };
  if (!columnTypes.includes(column.type)) unsupported(`type ${column.type}`);
  if((randomHex||utcDefault)&&column.type!=='TEXT')unsupported('generated text default on non-TEXT column');
  if (column.autoIncrement && !(column.primary && column.type === 'INTEGER')) unsupported('AUTOINCREMENT on non-rowid column');
  if (def.collate) {
    if (def.collate.collate?.name?.toUpperCase() !== 'NOCASE' || column.type !== 'TEXT') unsupported('column collation');
    column.collation = 'NOCASE';
  }
  if (def.check) {
    const [check] = def.check.definition;
    if (def.check.definition.length !== 1 || check.type !== 'binary_expr' ||
      check.left.type !== 'column_ref' || check.left.column !== column.name) {
      column.checkExpression = check; return column;
    }
    if (check.operator === 'IN' && check.right.type === 'expr_list') column.allowed = check.right.value.map(n => literal(n, []));
    else if (check.operator === '=') column.allowed = [literal(check.right, [])];
    else if (['>', '>=', '<', '<=', '!=', '<>'].includes(check.operator) &&
      ['INTEGER', 'REAL', 'NUMERIC'].includes(column.type) && check.right.type === 'number') {
      column.numericCheck = { op: check.operator, value: literal(check.right, []) };
    } else if(['BETWEEN','NOT BETWEEN'].includes(check.operator)&&['INTEGER','REAL','NUMERIC'].includes(column.type)&&check.right.type==='expr_list'&&check.right.value.length===2&&check.right.value.every(node=>node.type==='number'))column.checkExpression=check;
    else unsupported('CHECK expression');
  }
  return column;
}

export function compileTable(ast) {
  if (ast.type !== 'create' || ast.keyword !== 'table' || ast.table?.length !== 1 ||
    ast.table[0].db || ast.temporary || ast.as || ast.query_expr) unsupported('CREATE TABLE form');
  const withoutRowid = !!ast.table_options;
  if (withoutRowid && (ast.table_options.length !== 1 || ast.table_options[0].keyword?.toLowerCase() !== 'without rowid')) unsupported('table options');
  const name = checkIdentifier(ast.table[0].table);
  const columns = [], unique = [], primaryKey = [], foreignKeys = [], checks = [];
  const setPrimaryKey = keys => {
    if (primaryKey.length || !keys.length || new Set(keys).size !== keys.length) unsupported('invalid PRIMARY KEY declaration');
    primaryKey.push(...keys);
  };
  for (const def of ast.create_definitions ?? []) {
    if (def.resource === 'constraint') {
      const kind = def.constraint_type?.toLowerCase();
      onlyKeys(def, ['resource','constraint','definition','constraint_type','reference_definition'], 'table constraint');
      if (kind === 'check') { if (def.definition?.length !== 1) unsupported('CHECK declaration'); checks.push(def.definition[0]); continue; }
      if (kind === 'foreign key') {
        foreignKeys.push(compileReference(def.definition.map(c => checkIdentifier(c.column)), def.reference_definition));
        continue;
      }
      if (!['primary key', 'unique'].includes(kind)) unsupported('table constraint');
      const keys = def.definition.map(col => {
        onlyKeys(col, ['type', 'column'], 'key column');
        if (col.type !== 'column_ref') unsupported('key expression');
        return checkIdentifier(col.column);
      });
      unique.push(keys);
      if (kind === 'primary key') setPrimaryKey(keys);
    } else {
      const col = columnDefinition(def);
      if (def.reference_definition) foreignKeys.push(compileReference([col.name], def.reference_definition));
      if (col.primary || def.unique) unique.push([col.name]);
      if (col.primary) setPrimaryKey([col.name]);
      columns.push(col);
    }
  }
  const names = new Set(columns.map(c => c.name));
  if (!names.size || names.size !== columns.length || unique.flat().some(k => !names.has(k)) || foreignKeys.some(fk => fk.columns.some(k => !names.has(k)))) unsupported('invalid table columns/keys');
  if (withoutRowid) {
    if (!primaryKey.length || columns.some(c => c.autoIncrement)) unsupported('WITHOUT ROWID key declaration');
    for (const col of columns) if (primaryKey.includes(col.name)) col.notNull = true;
  }
  if (!withoutRowid && primaryKey.length === 1 && columns.find(c => c.name === primaryKey[0]).type === 'INTEGER') {
    columns.find(c => c.name === primaryKey[0]).primary = true;
  }
  return { _id: name, columns, unique, primaryKey, foreignKeys, checks, withoutRowid };
}

export function tableInfo(spec) {
  return spec.columns.map((c, cid) => ({
    cid, name: c.name, type: c.type, notnull: Number(c.notNull), dflt_value: c.defaultSql ?? null,
    pk: (spec.primaryKey ?? []).indexOf(c.name) + 1,
  }));
}

// DATETIME has bounded NUMERIC affinity: nonnumeric text and blobs retain their
// storage class. Partial UNIQUE indexes must include those values as well.
function nonNullIndexType(column) {
  return column.type==='DATETIME'?['number','string','binData']:
    column.type==='TEXT'?'string':column.type==='BLOB'?'binData':'number';
}

export function compileIndex(ast, spec) {
  onlyKeys(ast, ['type','index_type','keyword','if_not_exists','index','on_kw','table','index_columns','where'], 'index');
  if (ast.type !== 'create' || ast.keyword !== 'index' || (ast.index_type && ast.index_type !== 'unique') || ast.table.db || ast.index.schema) unsupported('index form');
  const keys = {};
  for (const col of ast.index_columns) {
    if (col.type !== 'column_ref' || col.collate || !spec.columns.some(c => c.name === col.column)) unsupported('index expression');
    keys[col.column] = col.order_by?.toUpperCase() === 'DESC' ? -1 : 1;
  }
  function partial(node) {
    if (node.type === 'binary_expr' && node.operator === 'AND') return { $and: [partial(node.left), partial(node.right)] };
    if (node.type !== 'binary_expr' || node.left.type !== 'column_ref' || node.left.table) unsupported('partial index predicate');
    const column = spec.columns.find(c => c.name === node.left.column);
    if (!column) unsupported('partial index column');
    if (node.operator === '=') return { [column.name]: literal(node.right, []) };
    if (node.right.type === 'null' && node.operator === 'IS') return { [column.name]: null };
    if (node.right.type === 'null' && node.operator === 'IS NOT') {
      return { [column.name]: { $type: nonNullIndexType(column) } };
    }
    unsupported('partial index expression');
  }
  const filters = ast.where ? [partial(ast.where)] : [];
  // SQLite UNIQUE permits multiple NULL keys. A native partial index must
  // exclude any nullable key component to preserve that contract.
  if (ast.index_type === 'unique') for (const name of Object.keys(keys)) {
    const c = spec.columns.find(c => c.name === name);
    if (!c.notNull) filters.push({ [name]: { $type: nonNullIndexType(c) } });
  }
  const partialFilterExpression = filters.length > 1 ? { $and: filters } : filters[0];
  return { name: checkIdentifier(ast.index.name), keys, partialFilterExpression, unique: ast.index_type === 'unique' };
}

function compileReference(columns, ref) {
  onlyKeys(ref, ['definition', 'table', 'keyword', 'on_action'], 'REFERENCES');
  if (ref.table?.length !== 1 || ref.table[0].db || !ref.definition?.length ||
    ref.definition.length !== columns.length) unsupported('foreign key target');
  const target = ref.definition.map(c => {
    onlyKeys(c, ['type', 'column'], 'foreign key column');
    return checkIdentifier(c.column);
  });
  let onDelete = 'NO ACTION';
  for (const action of ref.on_action ?? []) {
    if (action.type !== 'on delete' || !['cascade', 'no action', 'restrict'].includes(action.value?.value)) unsupported('foreign key action');
    onDelete = action.value.value.toUpperCase();
  }
  return { columns, table: checkIdentifier(ref.table[0].table), target, onDelete };
}
