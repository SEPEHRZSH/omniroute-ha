// Strict two/three-table INNER/LEFT equijoins. Unsupported forms fail before
// running a pipeline; null join keys never match one another.
import { expression, predicate, asciiLower, unsupported } from './sql.mjs';
export function joinPipeline(ast, bindings, specs, mode) {
  if (mode === 'run' || ast.with || ast.distinct || ast.groupby || ast.having || ast.for_update || ast._next || ast.returning ||
      ![2, 3].includes(ast.from.length) || ast.from[0].join || ast.from.slice(1).some(r => !['JOIN', 'INNER JOIN', 'LEFT JOIN'].includes(r.join))) unsupported('JOIN form');
  const refs = ast.from, aliases = refs.map(r => r.as || r.table);
  if (new Set(aliases).size !== refs.length || refs.some(r => r.db)) unsupported('JOIN aliases');
  const prefix = i => i ? '_ha_join' + i + '.' : '';
  const resolve = node => {
    if (node.type !== 'column_ref' || node.collate || node.column === '*') unsupported('JOIN column');
    const choices = specs.map((s,i) => ({ s,i })).filter(({s,i}) => (!node.table || node.table === aliases[i]) && s.columns.some(c => c.name === node.column));
    if (choices.length !== 1) unsupported('unknown/ambiguous JOIN column');
    const {s,i} = choices[0];
    if (s.columns.find(c => c.name === node.column).collation) unsupported('collated JOIN column');
    return { index: i, name: node.column, path: prefix(i) + node.column, type: s.columns.find(c => c.name === node.column).type };
  };
  const available = new Set(specs.flatMap((s,i) => s.columns.map(c => prefix(i) + c.name)));
  // The original call-log search casts INTEGER HTTP status to TEXT. Preserve
  // the storage affinity; a REAL 1.0 has different SQLite text formatting.
  available.textCastTypes=new Map(specs.flatMap((s,i)=>s.columns.map(c=>[prefix(i)+c.name,c.type])));
  function normalized(node) {
    if (node?.type === 'column_ref') return { ...node, table: null, column: resolve(node).path };
    if (Array.isArray(node)) return node.map(normalized);
    return node && typeof node === 'object' ? Object.fromEntries(Object.entries(node).map(([k,v]) => [k,normalized(v)])) : node;
  }
  const pipeline = [];
  for (let i = 1; i < refs.length; i++) {
    const on = refs[i].on;
    if (on?.type !== 'binary_expr' || on.operator !== '=') unsupported('JOIN predicate');
    const keys = [resolve(on.left), resolve(on.right)].sort((a,b) => a.index-b.index);
    if (keys[0].index >= i || keys[1].index !== i || keys[0].type !== keys[1].type) unsupported('JOIN key type/source');
    pipeline.push({ $lookup: { from: specs[i]._id, let: { key: '$' + keys[0].path }, pipeline: [
      { $match: { $expr: { $and: [{ $ne: [{ $ifNull: ['$$key', null] }, null] }, { $eq: ['$' + keys[1].name, '$$key'] }] } } },
    ], as: '_ha_join' + i } }, { $unwind: { path: '$_ha_join' + i, preserveNullAndEmptyArrays: refs[i].join === 'LEFT JOIN' } });
  }
  pipeline.push({ $match: predicate(normalized(ast.where), bindings, available) });
  if(ast.orderby) {
    const sort={};
    for(const [index,item] of ast.orderby.entries()) {
      const collation=item.expr.collate?.collate?.name?.toUpperCase();
      if(item.expr.collate && !['NOCASE','BINARY'].includes(collation)) unsupported('JOIN ORDER BY collation');
      const column=resolve({...item.expr,collate:null}),direction=item.type==='DESC'?-1:1;
      if(collation==='NOCASE') {
        if(column.type!=='TEXT') unsupported('JOIN NOCASE on nontext column');
        const field='_ha_sort'+index,value={$ifNull:['$'+column.path,null]},nul={$indexOfCP:[{$ifNull:[value,'']},'\u0000']};
        pipeline.push({$set:{[field]:asciiLower({$cond:[{$gte:[nul,0]},{$substrCP:[value,0,nul]},value]}),[field+'Bytes']:{$strLenBytes:{$ifNull:[value,'']}}}});
        sort[field]=direction; sort[field+'Bytes']=direction;
      } else sort[column.path]=direction;
    }
    pipeline.push({$sort:sort});
  }
  if (ast.limit) {
    const values=ast.limit.value,offset=ast.limit.seperator==='offset';
    if ((ast.limit.seperator&&!offset)||values.length!==(offset?2:1)||values.some(node=>node.type!=='number'||!Number.isSafeInteger(node.value))||values[0].value<0)unsupported('JOIN LIMIT');
    if(offset&&values[1].value>0)pipeline.push({$skip:values[1].value});
    // MongoDB forbids $limit:0; SQLite returns an empty result.
    pipeline.push(values[0].value?{$limit:values[0].value}:{$match:{$expr:{$literal:false}}});
  }
  if (mode === 'get') pipeline.push({ $limit: 1 });
  const projection = { _id: 0 };
  for (const item of ast.columns) {
    if (item.expr.type === 'column_ref' && item.expr.column === '*') {
      const i = aliases.indexOf(item.expr.table);
      if (i < 0 || item.as) unsupported('unqualified JOIN wildcard');
      for (const c of specs[i].columns) projection[c.name] = { $ifNull: ['$' + prefix(i) + c.name, null] };
    } else {
      const key = item.as || item.expr.column || (item.expr.type === 'number' && Number.isSafeInteger(item.expr.value) ? String(item.expr.value) : undefined);
      if (!key || Object.hasOwn(projection,key)) unsupported('JOIN projection');
      projection[key] = expression(normalized(item.expr), bindings, available);
    }
  }
  pipeline.push({ $project: projection }); return pipeline;
}
