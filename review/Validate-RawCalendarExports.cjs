'use strict';
// Diagnostic only: independent raw XER/calendar parser and integer-minute occupancy oracle.
// Does not load or call production calendar code; does not edit the input.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
assert.equal(process.argv.length, 5, 'Usage: node Validate-RawCalendarExports.cjs <original.xer> <csv-directory> <new-result.json>');
const input = path.resolve(process.argv[2]);
const exportDir = path.resolve(process.argv[3]);
const resultPath = path.resolve(process.argv[4]);
assert(!fs.existsSync(resultPath), 'Refuse to overwrite existing validation evidence');
const originalName = path.basename(input);
const bytes = fs.readFileSync(input);
const sourceHash = crypto.createHash('sha256').update(bytes).digest('hex').toUpperCase();
// Independent strict UTF-8/legacy decoding; scheduling/calendar fields are ASCII.
let decoded;
try { decoded = new TextDecoder('utf-8', {fatal:true}).decode(bytes); }
catch { decoded = new TextDecoder('windows-1252').decode(bytes); }
const tables = new Map();
let table, headers;
for (const line of decoded.split(/\r?\n/)) {
  const fields = line.split('\t');
  if (fields[0] === '%T') { table = fields[1]; if (!tables.has(table)) tables.set(table, []); }
  else if (fields[0] === '%F') headers = fields.slice(1);
  else if (fields[0] === '%R') tables.get(table).push(Object.fromEntries(headers.map((h,i) => [h, fields[i+1] ?? ''])));
}
function trim(s) { return s.replace(/^[\s\x00-\x1f\x7f]+|[\s\x00-\x1f\x7f]+$/g,''); }
function tree(s) {
  const root=[], stack=[root];
  for (const token of s.match(/\(|\)|[^()]+/g) || []) {
    if(token==='('){ const group=[]; stack.at(-1).push(group); stack.push(group); }
    else if(token===')'){ assert(stack.length>1,'unmatched close'); stack.pop(); }
    else if(trim(token)) stack.at(-1).push(trim(token));
  }
  assert.equal(stack.length,1,'unclosed raw groups');
  function node(g) {
    assert(Array.isArray(g) && g.length===3 && typeof g[0]==='string' && Array.isArray(g[1]) && Array.isArray(g[2]));
    assert(/^\d+\|\|/.test(g[0]), 'record metadata');
    return {name:g[0].split('||').slice(1).join('||'), attrs:g[1].join(''), children:g[2].map(node)};
  }
  return root.map(node);
}
function flatten(nodes) { return nodes.flatMap(n => [n, ...flatten(n.children)]); }
const shiftPatterns=new Map(), durationCounts=new Map();
let rawShiftCount=0, splitDayCount=0, overnightCount=0, overlapDayCount=0;
function shifts(day, where) {
  const out=day.children.map(n => {
    assert.equal(n.children.length,0, where);
    const a=n.attrs.split('|'); assert.equal(a.length,4,where);
    const values = {[a[0]]:a[1],[a[2]]:a[3]};
    const clock=(value,end) => {
      assert(/^\d{1,2}:\d{2}$/.test(value),where);
      const [h,m]=value.split(':').map(Number);
      assert(h>=0 && h<=24 && m>=0 && m<60 && (h!==24 || (end && m===0)),where);
      return h*60+m;
    };
    const start=clock(values.s,false); let finish=clock(values.f,true);
    if(finish<start || (!finish && !start)) finish+=1440;
    assert(finish-start<=1440,where);
    rawShiftCount++; if(finish>1440) overnightCount++;
    durationCounts.set((finish-start)/60,(durationCounts.get((finish-start)/60)||0)+1);
    shiftPatterns.set(`${values.s}-${values.f}`,(shiftPatterns.get(`${values.s}-${values.f}`)||0)+1);
    return [start,finish];
  });
  if(out.length>1) splitDayCount++;
  const occupancy = new Uint8Array(2880);
  let overlap=false;
  for(const [start,finish] of out) for(let m=start;m<finish;m++){ if(occupancy[m]) overlap=true; occupancy[m]=1; }
  if(overlap) overlapDayCount++;
  return out;
}
const calendars = new Map();
for(const raw of tables.get('CALENDAR')) {
  assert(!calendars.has(raw.clndr_id),'duplicate calendar ID');
  const nodes=flatten(tree(raw.clndr_data));
  const weeks=nodes.filter(n=>n.name==='DaysOfWeek'); assert.equal(weeks.length,1);
  const week=new Array(7);
  for(const d of weeks[0].children){ const index=+d.name-1; assert(index>=0 && index<7 && !week[index]); week[index]=shifts(d,`${raw.clndr_id} weekday ${d.name}`); }
  assert.equal(week.filter(Boolean).length,7);
  const exceptions=new Map();
  for(const section of nodes.filter(n=>['Exceptions','HolidayOrExceptions','HolidayOrException'].includes(n.name))) {
    for(const d of section.children){
      assert(/^d\|\d+(\|0)?$/.test(d.attrs));
      const serial=+d.attrs.split('|')[1]; assert(!exceptions.has(serial),'duplicate dated exception');
      exceptions.set(serial,shifts(d,`${raw.clndr_id} exception ${serial}`));
    }
  }
  calendars.set(raw.clndr_id,{raw, week, ownExceptions:exceptions});
}
function inherited(c,visiting=new Set()) {
  if(c.exceptions) return c.exceptions;
  assert(!visiting.has(c.raw.clndr_id),'cyclic inheritance'); visiting.add(c.raw.clndr_id);
  const base=c.raw.base_clndr_id;
  const combined=base && base!=='0' && base!=='-1' ? new Map(inherited(calendars.get(base),visiting)) : new Map();
  c.inheritedOnly=[...combined.keys()].filter(d=>!c.ownExceptions.has(d));
  c.overridden=[...combined.keys()].filter(d=>c.ownExceptions.has(d));
  c.changedOverrides=c.overridden.filter(d=>JSON.stringify(combined.get(d))!==JSON.stringify(c.ownExceptions.get(d)));
  for(const [date,slots] of c.ownExceptions) combined.set(date,slots);
  visiting.delete(c.raw.clndr_id); c.exceptions=combined; return combined;
}
const baseTime=Date.UTC(1899,11,30), dayMs=86400000;
const date=(serial)=>new Date(baseTime+serial*dayMs).toISOString().slice(0,10);
const dow=(serial)=>new Date(baseTime+serial*dayMs).getUTCDay();
function occupancy(c,serial) {
  const out=new Uint8Array(1440), explicit=c.exceptions.has(serial);
  const today=explicit?c.exceptions.get(serial):c.week[dow(serial)];
  for(const [start,end] of today) out.fill(1,start,Math.min(end,1440));
  if(!explicit) {
    const prior=c.exceptions.has(serial-1)?c.exceptions.get(serial-1):c.week[(dow(serial)+6)%7];
    for(const [,end] of prior) if(end>1440) out.fill(1,0,end-1440);
  }
  return out;
}
const minutes=(bits)=>bits.reduce((sum,x)=>sum+x,0);
const expected11=new Map(), summaries=[];
const weekdays=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
for(const c of calendars.values()) {
  inherited(c);
  c.standard=c.week.map((slots,d)=>{
    const bits=new Uint8Array(1440);
    for(const [start,end] of slots) bits.fill(1,start,Math.min(end,1440));
    for(const [,end] of c.week[(d+6)%7]) if(end>1440) bits.fill(1,0,end-1440);
    return bits;
  });
  for(let d=0;d<7;d++) expected11.set(`${c.raw.clndr_id}|weekday|${d}`,{id:c.raw.clndr_id,date:'',dow:d,hours:minutes(c.standard[d])/60});
  const affected=new Set([...c.exceptions.keys()].flatMap(d=>[d,d+1]));
  c.effectiveExceptions=new Map();
  for(const serial of [...affected].sort((a,b)=>a-b)) {
    const bits=occupancy(c,serial);
    if(c.exceptions.has(serial) || !bits.every((n,i)=>n===c.standard[dow(serial)][i])) {
      c.effectiveExceptions.set(serial,bits);
      expected11.set(`${c.raw.clndr_id}|date|${date(serial)}`,{id:c.raw.clndr_id,date:date(serial),dow:dow(serial),hours:minutes(bits)/60});
    }
  }
  const weeklyHours=c.standard.map(bits=>minutes(bits)/60);
  const assigned=(tables.get('TASK')||[]).filter(t=>t.clndr_id===c.raw.clndr_id);
  const endpointDates=assigned.flatMap(t=>[t.restart_date,t.reend_date]).filter(Boolean).sort();
  const exceptionEnd=c.exceptions.size?date(Math.max(...c.exceptions.keys())):null;
  summaries.push({id:c.raw.clndr_id,name:c.raw.clndr_name,base:c.raw.base_clndr_id,hpd:+c.raw.day_hr_cnt,weekConversion:+c.raw.week_hr_cnt,weeklyHours,actualWeeklyHours:weeklyHours.reduce((a,b)=>a+b,0),ownExceptionCount:c.ownExceptions.size,resolvedExceptionCount:c.exceptions.size,inheritedOnlyCount:c.inheritedOnly.length,inheritedOnlyDates:c.inheritedOnly.map(date),overrideCount:c.overridden.length,changedOverrideCount:c.changedOverrides.length,changedOverrideDates:c.changedOverrides.map(date),workingExceptions:[...c.exceptions].filter(([d])=>minutes(occupancy(c,d))>0).map(([d])=>({date:date(d),hours:minutes(occupancy(c,d))/60})),exceptionStart:c.exceptions.size?date(Math.min(...c.exceptions.keys())):null,exceptionEnd,tasks:assigned.length,remainingEndpointStart:endpointDates[0]??null,remainingEndpointEnd:endpointDates.at(-1)??null,tasksFinishingAfterLastException:exceptionEnd?assigned.filter(t=>t.reend_date.slice(0,10)>exceptionEnd).length:null,taskStates:Object.fromEntries([...new Set(assigned.map(t=>t.status_code))].map(s=>[s,assigned.filter(t=>t.status_code===s).length]))});
}
function parseCsv(file){
  const text=fs.readFileSync(file,'utf8').replace(/^\uFEFF/,'');
  const rows=[]; let row=[],value='',quoted=false;
  for(let i=0;i<text.length;i++) {const ch=text[i];
    if(quoted) {if(ch==='"' && text[i+1]==='"'){value+='"';i++;} else if(ch==='"') quoted=false; else value+=ch;}
    else if(ch==='"') quoted=true;
    else if(ch===','){row.push(value);value='';}
    else if(ch==='\n'){row.push(value.replace(/\r$/,''));rows.push(row);row=[];value='';}
    else value+=ch;
  }
  if(value || row.length){row.push(value);rows.push(row);}
  const h=rows.shift();return rows.map(r=>{assert.equal(r.length,h.length);return Object.fromEntries(h.map((f,i)=>[f,r[i]]));});
}
const errors=[];
let checked10=0,checked11=0;
const csv10=path.join(exportDir,'10_XER_CALENDAR.csv'),csv11=path.join(exportDir,'11_XER_CALENDAR_DETAILED.csv');
assert(fs.existsSync(csv10) && fs.existsSync(csv11), 'Both current calendar CSVs are required');
if(fs.existsSync(csv10)) {
  const rows=parseCsv(csv10).filter(row => row.FileName === originalName);assert.equal(rows.length,calendars.size);
  for(const r of rows){const raw=calendars.get(r.clndr_id)?.raw;assert(raw);for(const [field,v] of Object.entries(raw)) if(r[field]!==v) errors.push({table:10,id:r.clndr_id,field,raw:v,export:r[field]});checked10++;}
}
if(fs.existsSync(csv11)) {
  const rows=parseCsv(csv11).filter(row => row.FileName === originalName); const seen=new Set();
  for(const r of rows) {
    const d=weekdays.indexOf(r.day_of_week);
    const key=r.date?`${r.clndr_id}|date|${r.date}`:`${r.clndr_id}|weekday|${d}`;
    if(seen.has(key)) errors.push({duplicate:key}); seen.add(key);
    const expected=expected11.get(key);
    if(!expected) errors.push({unexpected:key});
    else for(const [field,value] of Object.entries({work_hours:String(expected.hours),day_of_week:weekdays[expected.dow],day_of_week_num:String(expected.dow||7),working_day:expected.hours>0?'Y':'N',working_day_int:expected.hours>0?'1':'0',exception_type:expected.date?(expected.hours>0?'Exception - Working':'Exception - Non-Working'):'Standard'})) if(r[field]!==value) errors.push({key,field,expected:value,actual:r[field]});
    checked11++;
  }
  for(const key of expected11.keys()) if(!seen.has(key)) errors.push({missing:key});
}
const unknownTaskCalendars=[...new Set((tables.get('TASK')||[]).filter(t=>!calendars.has(t.clndr_id)).map(t=>t.clndr_id))];
const result={input: path.basename(input),sourceHash,calendarCount:calendars.size,rawShiftCount,splitDayCount,overnightCount,overlapDayCount,shiftDurationCounts:Object.fromEntries(durationCounts),shiftPatterns:Object.fromEntries(shiftPatterns),taskCount:tables.get('TASK')?.length,unknownTaskCalendars,expected11Rows:expected11.size,checked10,checked11,errors,summaries};
fs.writeFileSync(resultPath,JSON.stringify(result,null,2)+'\n', {flag:'wx'});
console.log(JSON.stringify({input:result.input, sourceHash, calendarCount:result.calendarCount, rawShiftCount, checked10, checked11, errors},null,2));
assert.equal(errors.length,0,'Calendar reconciliation mismatches');
