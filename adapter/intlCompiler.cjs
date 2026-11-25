// next-intl 4.14.1 loads its optional native extractor eagerly. Ordinary catalog
// localization needs no compiler. Defer the import without disabling extraction
// or any native security checks, at build and production-config loading alike.
let installed=false;
exports.install=function install() {
  if(installed)return;
  const fs=require('node:fs'),path=require('node:path'),{fileURLToPath,pathToFileURL}=require('node:url');
  const root=path.resolve(__dirname,'..');
  const files=new Map([
    ['development',['250c1a8b1251f202b5d40128ab7b94b1adc5f27ddd4ab9712f103e95108972b0',"import { transform } from '@swc/core';",'transform']],
    ['production',['3bc576a743c6574e799db68a5b61a4f0d7d3d314ebc3205dfc3351f2efe9a268','import{transform as s}from"@swc/core";','s']],
  ].map(([flavor,entry])=>[pathToFileURL(path.join(root,'node_modules/next-intl/dist/esm',flavor,'extractor/extractor/MessageExtractor.js')).href,entry]));
  const origins=new Map();
  // Resolve-only virtual ESM preserves Node 22's CommonJS cycle loader.
  require('node:module').registerHooks({resolve(specifier,context,next) {
    const origin=origins.get(context.parentURL),resolved=next(specifier,origin?{...context,parentURL:origin}:context);
    const entry=files.get(resolved.url);if(!entry)return resolved;
    const source=fs.readFileSync(fileURLToPath(resolved.url),'utf8'),[hash,anchor,name]=entry;
    if(require('node:crypto').createHash('sha256').update(source).digest('hex')!==hash||source.split(anchor).length!==2)throw new Error('next-intl extractor source changed; review lazy compiler integration');
    const lazy='const '+name+'=async(...args)=>{const {transform:compile}=await import("@swc/core");return compile(...args);};';
    const body=source.replace(anchor,lazy).replaceAll('import.meta.url',JSON.stringify(resolved.url));
    const url='data:text/javascript;base64,'+Buffer.from(body).toString('base64');origins.set(url,resolved.url);
    return {url,format:'module',shortCircuit:true};
  }});
  installed=true;
};
