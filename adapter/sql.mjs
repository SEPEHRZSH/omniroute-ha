import sqlParser from 'node-sql-parser';
import { parseConversationCandidates } from './conversationQuery.mjs';
import { parseLatestQuota } from './quotaQuery.mjs';
import { parseUsageQuery } from './usageQueries.mjs';
import { parseProviderQuery } from './providerQueries.mjs';
import { parseGamificationQuery } from './gamificationQueries.mjs';
import {parseRetentionQuery} from './retentionQueries.mjs';

const parser = new sqlParser.Parser();
export function unsupported(feature) {
  throw Object.assign(new Error(`Unsupported SQLite feature: ${feature}`), { code: 'HA_UNSUPPORTED_SQL' });
}

// Visit SQL outside literals, identifiers and comments. Values never become SQL text.
export function scan(sql, visit) {
  let output = '';
  for (let i = 0; i < sql.length;) {
    const start = i;
    const ch = sql[i];
    if (sql.startsWith('--', i)) {
      const end = sql.indexOf('\n', i);
      i = end < 0 ? sql.length : end + 1;
    } else if (sql.startsWith('/*', i)) {
      const end = sql.indexOf('*/', i + 2);
      if (end < 0) unsupported('unterminated comment');
      i = end + 2;
    } else if (['\'', '"', '`', '['].includes(ch)) {
      const end = ch === '[' ? ']' : ch;
      i++;
      let closed = false;
      while (i < sql.length) {
        if (sql[i++] !== end) continue;
        if (end !== ']' && sql[i] === end) { i++; continue; }
        closed = true;
        break;
      }
      if (!closed) unsupported('unterminated literal or identifier');
    } else {
      const replacement = visit(i);
      if (replacement) {
        output += replacement.text;
        i += replacement.length;
        continue;
      }
      i++;
    }
    output += sql.slice(start, i);
  }
  return output;
}

export function splitStatements(sql) {
  const cuts = [-1];
  scan(sql, i => { if (sql[i] === ';') cuts.push(i); });
  cuts.push(sql.length);
  return cuts.slice(1).map((end, i) => sql.slice(cuts[i] + 1, end).trim())
    .filter(statement => {
      let rest = statement;
      while (rest.startsWith('--') || rest.startsWith('/*')) {
        const line = rest.startsWith('--');
        const end = rest.indexOf(line ? '\n' : '*/', 2);
        rest = end < 0 ? '' : rest.slice(end + (line ? 1 : 2)).trimStart();
      }
      return rest.length > 0;
    });
}

export function parse(sql, args = []) {
  let named = {};
  if (args[0] && Object.getPrototypeOf(args[0]) === Object.prototype) {
    [named, ...args] = args;
  } else if (args.length === 1 && Array.isArray(args[0])) args = args[0];
  const bindings = [];
  let position = 0;
  let bound = scan(sql, i => {
    const tail = sql.slice(i);
    if (/^__ha_(?:bind|nocase|like_escape)__\b/i.test(tail)) unsupported('reserved compatibility function');
    let value, length;
    if (tail[0] === '?') {
      if (/^\?\d/.test(tail)) unsupported('numbered SQLite parameters');
      if (position >= args.length) throw Object.assign(new Error('Missing positional binding'), { code: 'SQLITE_RANGE' });
      value = args[position++]; length = 1;
    } else {
      const match = /^[@:$]([A-Za-z_][A-Za-z0-9_]*)/.exec(tail);
      if (!match) return;
      if (!Object.hasOwn(named, match[1])) throw Object.assign(new Error(`Missing named binding: ${match[1]}`), { code: 'SQLITE_RANGE' });
      value = named[match[1]]; length = match[0].length;
    }
    if (value === undefined) value = null;
    if (!(value === null || typeof value === 'string' || Buffer.isBuffer(value) ||
      (typeof value === 'number' && Number.isFinite(value)))) unsupported('binding type');
    const index = bindings.push(value) - 1;
    return { text: `__ha_bind__(${index})`, length };
  });
  if (position !== args.length) throw Object.assign(new Error('Extra positional bindings'), { code: 'SQLITE_RANGE' });
  const native = parseConversationCandidates(sql, bindings) || parseLatestQuota(sql, bindings) || parseUsageQuery(sql,bindings) || parseProviderQuery(sql,bindings) || parseGamificationQuery(sql,bindings) || parseRetentionQuery(sql,bindings);
  if (native) return { ast: native, bindings };
  // SQLite permits COUNT as a bare identifier; the reused parser reserves it.
  // Preserve COUNT(...) as a function and quote the identifier elsewhere,
  // outside literals/comments/quoted identifiers (including qualified fields).
  let statementType;
  scan(bound, i => {
    if (!statementType) statementType = /^[A-Za-z]+/.exec(bound.slice(i))?.[0].toUpperCase();
  });
  bound = scan(bound, i => {
    const limit = /^(LIMIT|OFFSET)\s+__ha_bind__\((\d+)\)/i.exec(bound.slice(i));
    if (limit && !/[A-Za-z0-9_]/.test(bound[i - 1] ?? '')) {
      const value = bindings[Number(limit[2])];
      if (!Number.isSafeInteger(value)) unsupported('noninteger LIMIT/OFFSET binding');
      // This parser's LIMIT grammar does not accept function markers. Only a
      // validated integer is rendered here; text values never enter SQL.
      return { text: `${limit[1]} ${value}`, length: limit[0].length };
    }
    const match = /^AS\s+count\b/i.exec(bound.slice(i));
    if (match && !/[A-Za-z0-9_]/.test(bound[i - 1] ?? '')) return { text: 'AS "count"', length: match[0].length };
    if(!/[A-Za-z0-9_]/.test(bound[i-1]??'') && /^count\b(?!\s*\()/i.test(bound.slice(i))) return {text:'`count`',length:5};
  });
  if (position !== args.length) throw Object.assign(new Error('Extra positional bindings'), { code: 'SQLITE_RANGE' });
  // Normalize ASCII NOCASE only for WHERE identifiers and bindings.
  // ORDER BY and DDL collations have separate parser representations.
  let inWhere = false;
  if (['SELECT', 'INSERT', 'UPDATE', 'DELETE'].includes(statementType)) bound = scan(bound, i => {
    if (/[A-Za-z0-9_]/.test(bound[i - 1] ?? '')) return;
    if (/^WHERE\b/i.test(bound.slice(i))) inWhere = true;
    if (/^(?:ORDER\s+BY|GROUP\s+BY|RETURNING|LIMIT|OFFSET)\b/i.test(bound.slice(i))) inWhere = false;
    if (!inWhere) return;
    // The upstream LKGP query uses a bound LIKE pattern with a backslash escape.
    const escapedLike = /^LIKE\s+__ha_bind__\((\d+)\)\s+ESCAPE\s+'\\'/i.exec(bound.slice(i));
    if (escapedLike) return {text: 'LIKE __ha_like_escape__(' + escapedLike[1] + ')', length: escapedLike[0].length};
    const match = /^(__ha_bind__\(\d+\)|[A-Za-z_][A-Za-z0-9_]*)\s+COLLATE\s+NOCASE\b/i.exec(bound.slice(i));
    if (match) return { text: `__ha_nocase__(${match[1]})`, length: match[0].length };
  });
  let ast;
  let conflictAt;
  scan(bound, i => {
    if (/^ON\s+CONFLICT\s*\(/i.test(bound.slice(i)) && !/[A-Za-z0-9_]/.test(bound[i - 1] ?? '')) conflictAt = i;
  });
  try {
    ast = parser.astify(conflictAt === undefined ? bound : bound.slice(0, conflictAt), { database: 'SQLite' });
    if (conflictAt !== undefined) {
      const clause = /^ON\s+CONFLICT\s*\(\s*([A-Za-z0-9_,\s]+)\)\s+DO\s+([\s\S]+)$/i.exec(bound.slice(conflictAt));
      if (!clause || Array.isArray(ast) || ast.type !== 'insert' || ast.or) unsupported('ON CONFLICT form');
      const columns = clause[1].split(',').map(c => c.trim());
      const nothing = /^NOTHING(?:\s+(RETURNING[\s\S]+?))?\s*;?$/i.exec(clause[2]);
      if (nothing) {
        if (nothing[1]) ast.returning = parser.astify(`DELETE FROM placeholder WHERE 1=1 ${nothing[1]}`, { database: 'SQLite' }).returning;
        ast.on_conflict = { columns, set: null };
      } else {
        const action = /^UPDATE\s+SET\s+([\s\S]+)$/i.exec(clause[2]);
        if (!action) unsupported('ON CONFLICT action');
        const update = parser.astify(`UPDATE placeholder SET ${action[1]}`, { database: 'SQLite' });
        if (Array.isArray(update) || update.where || update.limit || update.orderby) unsupported('ON CONFLICT update clause');
        ast.returning = update.returning;
        ast.on_conflict = { columns, set: update.set };
      }
    }
  }
  catch { unsupported('syntax outside supported SQLite grammar'); }
  if (Array.isArray(ast)) {
    if (ast.length !== 1) unsupported('multiple statements in prepare');
    [ast] = ast;
  }
  return { ast, bindings };
}

export function asciiLower(input) {
  let result = input;
  for (const letter of 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') result = { $replaceAll: { input: result, find: letter, replacement: letter.toLowerCase() } };
  return result;
}

// SQLite NOCASE stops folding at NUL, then breaks equal-prefix ties by byte
// length. Do not substitute Unicode lowercasing or MongoDB locale collation.
export function noCaseKey(value) {
  const isText = { $eq: [{ $type: value }, 'string'] };
  const text = { $cond: [isText, value, ''] }, nul = { $indexOfCP: [text, '\u0000'] };
  const prefix = { $cond: [{ $gte: [nul, 0] }, { $substrCP: [text, 0, { $max: [nul, 0] }] }, text] };
  return { $cond: [isText, [asciiLower(prefix), { $strLenBytes: text }], value] };
}

export function expression(node, bindings, columns) {
  if (!node) unsupported('empty expression');
  switch (node.type) {
    case 'cast': {
      const target=node.target?.[0],type=columns.textCastTypes?.get(node.expr?.column);
      if(columns.reasoningExpiry&&node.keyword==='cast'&&node.symbol==='as'&&node.target?.length===1&&target.dataType==='INTEGER'&&
        !target.suffix?.length&&node.expr?.type==='column_ref'&&node.expr.column==='expires_at'&&!node.expr.table) {
        const value=expression(node.expr,bindings,columns);
        return {$cond:[{$cond:[{$isNumber:value},{$lte:[{$abs:value},Number.MAX_SAFE_INTEGER]},false]},{$trunc:value},
          {$convert:{input:{$concat:['HA_REASONING_EXPIRY_REQUIRES_NUMERIC:',{$type:value}]},to:'long'}}]};
      }
      if(Object.keys(node).some(key=>!['type','keyword','expr','symbol','target'].includes(key))||node.keyword!=='cast'||node.symbol!=='as'||
        node.target?.length!==1||target.dataType!=='TEXT'||Object.keys(target).some(key=>!['dataType','suffix'].includes(key))||target.suffix?.length||
        node.expr?.type!=='column_ref'||!['INTEGER','TEXT'].includes(type))unsupported('CAST TEXT requires a certified joined INTEGER/TEXT column');
      const value=expression(node.expr,bindings,columns);
      // Never guess SQLite's floating formatter or decode blobs. A malformed
      // out-of-profile row raises an explicit compatibility fault, not NULL or
      // a truncated integer. The error expression depends on the row so Mongo
      // cannot evaluate an invalid constant while optimizing an unused branch.
      return{$let:{vars:{haCast:value},in:{$switch:{branches:[
        {case:{$eq:['$$haCast',null]},then:null},
        {case:{$eq:[{$type:'$$haCast'},'string']},then:'$$haCast'},
        {case:{$cond:[{$isNumber:'$$haCast'},{$and:[{$eq:[{$trunc:'$$haCast'},'$$haCast']},{$lte:[{$abs:'$$haCast'},Number.MAX_SAFE_INTEGER]}]},false]},then:{$toString:{$convert:{input:'$$haCast',to:'long'}}}},
      ],default:{$convert:{input:{$concat:['HA_SQLITE_TEXT_CAST_REQUIRES_INTEGER_OR_TEXT:',{$type:'$$haCast'}]},to:'long'}}}}}};
    }
    case 'case': {
      if(Object.keys(node).some(key=>!['type','expr','args'].includes(key)))unsupported('searched CASE options');
      if(node.expr||!Array.isArray(node.args)||!node.args.length||node.args.length>32)unsupported('searched CASE form');
      const branches=[];let fallback={$literal:null},seenElse=false;
      for(const branch of node.args) {
        if(Object.keys(branch).some(key=>!['type','cond','result'].includes(key)))unsupported('searched CASE branch options');
        if(branch.type==='else'&&!seenElse){seenElse=true;fallback=expression(branch.result,bindings,columns);continue;}
        if(seenElse||branch.type!=='when'||branch.cond?.type!=='binary_expr'||
            !['=','!=','<>','<','<=','>','>=','IS','IS NOT','AND','OR'].includes(branch.cond.operator))unsupported('searched CASE condition');
        branches.push({case:expression(branch.cond,bindings,columns),then:expression(branch.result,bindings,columns)});
      }
      if(!branches.length)unsupported('searched CASE without WHEN');
      return {$switch:{branches,default:fallback}};
    }
    case 'null': return { $literal: null };
    case 'number':
      if (!Number.isFinite(Number(node.value))) unsupported('nonfinite numeric literal');
      return { $literal: Number(node.value) };
    case 'single_quote_string': case 'string':
      return { $literal: node.value };
    case 'column_ref':
      if (node.table || node.collate || (!columns.has(node.column) && node.column !== 'rowid')) unsupported(`column ${node.column}`);
      if (node.column === 'rowid' && !columns.has('rowid')) {
        if (columns.withoutRowid) unsupported('rowid on WITHOUT ROWID table');
        return '$_ha_rowid';
      }
      return { $ifNull: [`$${node.column}`, null] };
    case 'function': {
      const name = node.name?.name?.[0]?.value?.toUpperCase();
      const args = node.args?.value ?? [];
      if (node.over) unsupported('window function');
      if (name === '__HA_BIND__' || name === '__HA_LIKE_ESCAPE__') return { $literal: bindings[args[0].value] };
      const values = args.map(arg => expression(arg, bindings, columns));
      if (name === 'NOT' && args.length === 1 && ['binary_expr', 'number', 'null', 'unary_expr'].includes(args[0].type)) return { $cond: [{ $eq: [values[0], null] }, null, { $cond: [values[0], 0, 1] }] };
      if (name === '__HA_NOCASE__' && args.length === 1) return values[0];
      if (name === 'LENGTH' && args.length === 1) {
        // SQLite text length counts Unicode code points and stops at NUL.
        const text = { $ifNull: [values[0], ''] };
        const nul = { $indexOfCP: [text, '\u0000'] };
        return { $cond: [{ $eq: [values[0], null] }, null,
          { $cond: [{ $gte: [nul, 0] }, nul, { $strLenCP: text }] }] };
      }
      if (name === 'DATETIME' && args.length === 1) {
        const date = args[0].value === 'now' ? '$$NOW' : { $convert: { input: values[0], to: 'date', onError: null, onNull: null } };
        return { $dateToString: { date, format: '%Y-%m-%d %H:%M:%S', timezone: 'UTC', onNull: null } };
      }
      if(name==='UNIXEPOCH'&&args.length===1&&args[0].value==='now')return {$floor:{$divide:[{$toLong:'$$NOW'},1000]}};
      if (name === 'CURRENT_TIMESTAMP' && !node.args) return { $dateToString: { date: '$$NOW', format: '%Y-%m-%d %H:%M:%S', timezone: 'UTC' } };
      if (name === 'LOWER' && args.length === 1) {
        // SQLite LOWER folds ASCII only unless its ICU extension is loaded.
        return asciiLower(values[0]);
      }
      if (['COALESCE', 'IFNULL'].includes(name) && values.length >= 2) return { $ifNull: values };
      if (name === 'NULLIF' && values.length === 2) return { $cond: [{ $eq: values }, null, values[0]] };
      if (['SUBSTR', 'SUBSTRING'].includes(name) && args.length === 3 &&
        args[1].type === 'number' && args[1].value >= 1 && args[2].type === 'number' && args[2].value >= 0) {
        return { $cond: [{ $eq: [values[0], null] }, null, { $substrCP: [values[0], args[1].value - 1, args[2].value] }] };
      }
      unsupported(`function ${name}`);
      break;
    }
    case 'unary_expr': {
      if (node.operator !== 'NOT' || !['binary_expr', 'number', 'null', 'unary_expr'].includes(node.expr?.type)) unsupported('unary expression');
      const value = expression(node.expr, bindings, columns);
      return { $cond: [{ $eq: [value, null] }, null, { $cond: [value, 0, 1] }] };
    }
    case 'binary_expr': {
      if(['BETWEEN','NOT BETWEEN'].includes(node.operator.toUpperCase())) {
        if(node.right?.type!=='expr_list'||node.right.value.length!==2)unsupported('BETWEEN bounds');
        const combined={type:'binary_expr',operator:'AND',left:{type:'binary_expr',operator:'>=',left:node.left,right:node.right.value[0]},right:{type:'binary_expr',operator:'<=',left:node.left,right:node.right.value[1]}};
        return expression(node.operator.toUpperCase()==='BETWEEN'?combined:{type:'unary_expr',operator:'NOT',expr:combined},bindings,columns);
      }
      let left = expression(node.left, bindings, columns);
      let right = node.right.type === 'expr_list'
        ? node.right.value.map(n => expression(n, bindings, columns))
        : expression(node.right, bindings, columns);
      const op = node.operator.toUpperCase();
      const collated = n => n.type === 'function' && n.name?.name?.[0]?.value?.toUpperCase() === '__HA_NOCASE__';
      if (collated(node.left) || collated(node.right)) {
        if (!['=', '!=', '<>'].includes(op)) unsupported('NOCASE operator outside equality');
        left = noCaseKey(left); right = noCaseKey(right);
      }
      const nullTest = { $or: [{ $eq: [left, null] }, { $eq: [right, null] }] };
      if (op === '/') {
        // Only provably integral expressions are accepted: MongoDB's floating
        // division must not silently replace SQLite integer division.
        const integral = n => n.type === 'number' && Number.isSafeInteger(n.value) ||
          n.type === 'function' && n.name?.name?.[0]?.value?.toUpperCase() === 'LENGTH' ||
          n.type === 'binary_expr' && ['+', '-', '*'].includes(n.operator) && integral(n.left) && integral(n.right);
        if (!integral(node.left) || !integral(node.right)) unsupported('division without proven integer operands');
        // Guard the divisor itself; MongoDB may constant-fold an otherwise
        // unreachable division-by-zero branch while optimizing a $cond.
        return { $trunc: { $divide: [left, { $cond: [{ $eq: [right, 0] }, null, right] }] } };
      }
      if (op === 'LIKE' || op === 'NOT LIKE') {
        const name = node.right.type === 'function' && node.right.name?.name?.[0]?.value;
        const pattern = node.right.type === 'single_quote_string' ? node.right.value
          : name === '__ha_bind__' || name === '__ha_like_escape__' ? bindings[node.right.args.value[0].value] : undefined;
        if (pattern === null) return { $literal: null };
        if (typeof pattern !== 'string' || pattern.length > 4096) unsupported('dynamic/oversized LIKE pattern');
        if (name === '__ha_like_escape__') {
          const chars = [...pattern.split('\0', 1)[0]];
          const literal = c => /[a-z]/i.test(c) ? '[' + c.toLowerCase() + c.toUpperCase() + ']' : c.replace(/[.*+?^$(){}|[\]\\]/g, '\\$&');
          let regex = '';
          for (let i = 0; i < chars.length; i++) {
            const char = chars[i];
            if (char === '\\') {
              if (++i === chars.length) return { $cond: [nullTest, null, op === 'NOT LIKE'] };
              regex += literal(chars[i]);
            } else regex += char === '%' ? '[\\s\\S]*' : char === '_' ? '[\\s\\S]' : literal(char);
          }
          const match = { $let: { vars: { text: { $ifNull: [left, ''] } }, in: { $let: {
            vars: { nul: { $indexOfCP: ['$$text', '\0'] } },
            in: { $regexMatch: {
              input: { $cond: [{ $lt: ['$$nul', 0] }, '$$text', { $substrCP: ['$$text', 0, { $max: ['$$nul', 0] }] }] },
              regex: '^' + regex + '\\z',
            } },
          } } } };
          return { $cond: [nullTest, null, op === 'LIKE' ? match : { $not: [match] }] };
        }
        const regex = [...pattern].map(c => c === '%' ? '[\\s\\S]*' : c === '_' ? '[\\s\\S]' : /[a-z]/i.test(c) ? `[${c.toLowerCase()}${c.toUpperCase()}]` : c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('');
        const match = { $regexMatch: { input: { $ifNull: [left, ''] }, regex: `^${regex}$` } };
        return { $cond: [nullTest, null, op === 'LIKE' ? match : { $not: [match] }] };
      }
      if (op === 'IS' || op === 'IS NOT') return { [op === 'IS' ? '$eq' : '$ne']: [left, right] };
      if (op === 'AND' || op === 'OR') {
        // SQL three-valued logic; null must survive until the WHERE truth test.
        const short = op === 'AND' ? 0 : 1;
        const truth = value => ({ $cond: [{ $eq: [value, null] }, null, { $cond: [value, 1, 0] }] });
        // Bind each operand once. Repeating the nested left expression in
        // truth/null tests makes a normal 15-term search grow exponentially
        // beyond MongoDB's BSON command limit.
        return {$let:{vars:{haBoolLeft:left,haBoolRight:right},in:{ $cond: [
          { $or: [{ $eq: [truth('$$haBoolLeft'), short] }, { $eq: [truth('$$haBoolRight'), short] }] },
          short, { $cond: [{$or:[{$eq:['$$haBoolLeft',null]},{$eq:['$$haBoolRight',null]}]}, null, op === 'AND' ? 1 : 0] },
        ] }}};
      }
      const ops = { '=': '$eq', '!=': '$ne', '<>': '$ne', '<': '$lt', '<=': '$lte', '>': '$gt', '>=': '$gte', '+': '$add', '-': '$subtract', '*': '$multiply' };
      if (ops[op]) return { $cond: [nullTest, null, { [ops[op]]: [left, right] }] };
      if (op === 'IN' || op === 'NOT IN') {
        const match = { $in: [left, right] };
        return { $cond: [{ $eq: [left, null] }, null,
          { $cond: [match, op === 'IN', { $cond: [{ $in: [null, right] }, null, op === 'NOT IN'] }] }] };
      }
      unsupported(`operator ${op}`);
      break;
    }
    default: unsupported(`expression ${node.type}`);
  }
}

export function predicate(node, bindings, columns) {
  return node ? { $expr: { $eq: [{ $cond: [expression(node, bindings, columns), true, false] }, true] } } : {};
}
