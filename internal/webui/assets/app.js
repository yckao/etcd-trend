"use strict";
(() => {
  const $ = id => document.getElementById(id);
  const state = { info:null, curve:[], from:0, to:1, scope:"resource", metric:"writes", search:"", resource:"", namespace:"", labelKey:"", labelValue:"", labelFilter:false, groupScope:"", groupValue:"", groupName:"", offset:0, limit:10, objectOffset:0, objectSearch:"", mode:"capacity", detail:null, detailID:null, totals:null, requests:{} };
  const descriptions = {
    writes:"Measured value bytes actually written, including repeated updates.",
    history:"Measured positive net history growth over the entire selected interval; decreases are excluded from the ranking pool.",
    retained:"Measured positive net retained-record growth over the selected interval, including current values and history.",
    compacted:"Measured MVCC record bytes physically removed by compaction.",
    "db-growth":"Estimated share of observed DB growth, associated with changed record bytes in each commit.",
    "free-growth":"Estimated gross reusable-space creation, associated with preceding compaction batches. Reuse is shown separately.",
    "free-net":"Estimated free-space growth minus attributed reuse across the entire interval. Only positive net contributors are ranked."
  };
  function node(tag, attrs={}, text) { const e=document.createElement(tag); for(const [k,v] of Object.entries(attrs)) { if(k==="class") e.className=v; else e.setAttribute(k,v); } if(text!==undefined)e.textContent=text; return e; }
  function svgNode(tag,attrs={},text) { const e=document.createElementNS("http://www.w3.org/2000/svg",tag);for(const[k,v]of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;return e; }
  function bytes(n, signed=false) { n=Number(n)||0;const sign=n<0?"−":signed&&n>0?"+":"";n=Math.abs(n);if(n<1024)return sign+n.toLocaleString("en-US")+" B";const u=["KiB","MiB","GiB","TiB","PiB"];let i=-1;do{n/=1024;i++;}while(n>=1024&&i<u.length-1);return sign+n.toFixed(n>=100?1:n>=10?2:2)+" "+u[i]; }
  function exact(n){return (Number(n)||0).toLocaleString("en-US");}
  function time(ns) { const s=Math.max(0,Number(ns)||0)/1e9;if(s<60)return `${Number(s.toFixed(3))} s`;if(s<3600)return `${Math.floor(s/60)}m ${Number((s%60).toFixed(1))}s`;return `${Math.floor(s/3600)}h ${Math.floor(s%3600/60)}m`; }
  function startTime(){return Math.min(state.info.duration_ns,state.from*state.info.window_ns);}
  function endTime(){return Math.min(state.info.duration_ns,state.to*state.info.window_ns);}
  function rangeText(){return `${time(startTime())} → ${time(endTime())}`;}
  function identityName(id) { return id.name || id.key || id.resource || "Unknown key"; }
  function objectSub(id){return [id.resource,id.namespace || (id.scope==="cluster"?"cluster scoped":"")].filter(Boolean).join(" / ");}
  function displayKey(id){return id.key_encoding==="base64"?"base64:"+id.key:id.key;}
  function setError(id,message){$(id).textContent=message||"";$(id).classList.toggle("hidden",!message);}
  async function request(key,path,params={}) {
    if(state.requests[key])state.requests[key].abort();
    const controller=new AbortController();state.requests[key]=controller;
    const query=new URLSearchParams();for(const[k,v]of Object.entries(params))if(v!==""&&v!==undefined&&v!==null)query.set(k,String(v));
    try {
      const r=await fetch(path+(query.size?"?"+query.toString():""),{signal:controller.signal,credentials:"same-origin"});
      let data;try{data=await r.json();}catch(err){if(ignoreAbort(err))throw err;throw new Error("The local server returned an invalid response.");}
      if(!r.ok)throw new Error(data.error||`Request failed (${r.status})`);
      if(controller.signal.aborted||state.requests[key]!==controller)throw new DOMException("Superseded request","AbortError");
      return data;
    } catch(err) {
      // A superseded fetch or body read must not replace the current query's UI.
      if(controller.signal.aborted||state.requests[key]!==controller)throw new DOMException("Superseded request","AbortError");
      throw err;
    }
  }
  function ignoreAbort(err){return err.name==="AbortError";}
  function debounce(fn,delay=220){let timer;return(...args)=>{clearTimeout(timer);timer=setTimeout(()=>fn(...args),delay);};}

  function drawChart(svg,points,series,options={}) {
    svg.replaceChildren();const width=svg.clientWidth||options.width||1120,height=svg.clientHeight||options.height||300,pad={left:options.leftMargin??(width<500?52:64),right:options.ratio?42:18,top:18,bottom:37};svg.setAttribute("viewBox",`0 0 ${width} ${height}`);
    const chartWidth=width-pad.left-pad.right,chartHeight=height-pad.top-pad.bottom;
    let minimum=options.start??(points[0]?.time_ns||0),maximum=options.end??(points[points.length-1]?.time_ns||1);if(maximum<=minimum)maximum=minimum+1;
    let maxY=1024;for(const p of points)for(const s of series)if(!s.ratio)maxY=Math.max(maxY,Number(s.value(p))||0);maxY*=1.12;
    const x=t=>pad.left+(t-minimum)/(maximum-minimum)*chartWidth,y=v=>pad.top+chartHeight-Math.max(0,v)/maxY*chartHeight,ry=v=>pad.top+chartHeight-Math.max(0,v)/100*chartHeight;
    if(options.selection){const [a,b]=options.selection;svg.append(svgNode("rect",{x:x(a),y:pad.top,width:Math.max(0,x(b)-x(a)),height:chartHeight,class:"selected-area"}));}
    for(let i=0;i<=4;i++){const yy=pad.top+chartHeight*(1-i/4);svg.append(svgNode("line",{x1:pad.left,y1:yy,x2:width-pad.right,y2:yy,class:"grid-line"}));svg.append(svgNode("text",{x:pad.left-10,y:yy+3,"text-anchor":"end"},bytes(maxY*i/4)));if(options.ratio)svg.append(svgNode("text",{x:width-pad.right+10,y:yy+3},`${i*25}%`));}
    const ticks=width<500?3:5;for(let i=0;i<=ticks;i++){const t=minimum+(maximum-minimum)*i/ticks;svg.append(svgNode("text",{x:x(t),y:height-10,"text-anchor":i===0?"start":i===ticks?"end":"middle"},time(t)));}
    for(const s of series){if(!points.length)continue;const path=points.map((p,i)=>`${i?"L":"M"}${x(p.time_ns).toFixed(2)},${(s.ratio?ry(s.value(p)):y(s.value(p))).toFixed(2)}`).join(" ");if(s.fill){const area=path+` L${x(points[points.length-1].time_ns)},${y(0)} L${x(points[0].time_ns)},${y(0)} Z`;svg.append(svgNode("path",{d:area,fill:s.color,opacity:.075}));}svg.append(svgNode("path",{d:path,stroke:s.color,class:"data-line","stroke-dasharray":s.ratio?"5 5":"none"}));for(const p of points)if(points.length<=20)svg.append(svgNode("circle",{cx:x(p.time_ns),cy:s.ratio?ry(s.value(p)):y(s.value(p)),r:2.5,fill:s.color}));}
    if(options.selection){for(const t of options.selection)svg.append(svgNode("line",{x1:x(t),x2:x(t),y1:pad.top,y2:pad.top+chartHeight,class:"selection-edge"}));}
    return{minimum,maximum,x,pad,chartWidth,width,height};
  }
  let mainGeometry=null,drag=null;
  function renderCurve(){if(!state.info)return;const series=state.mode==="capacity"?[{color:"var(--blue)",value:p=>p.db_size,fill:true},{color:"var(--mint)",value:p=>p.db_size_in_use},{color:"var(--amber)",value:p=>p.free_bytes}]:[{color:"var(--amber)",value:p=>p.free_bytes,fill:true},{color:"var(--pink)",value:p=>p.fragmentation_percent,ratio:true}];mainGeometry=drawChart($("main-chart"),state.curve,series,{start:0,end:state.info.duration_ns,selection:[startTime(),endTime()],ratio:state.mode==="fragmentation"});$("capacity-mode").classList.toggle("active",state.mode==="capacity");$("fragmentation-mode").classList.toggle("active",state.mode==="fragmentation");const legend=document.querySelectorAll(".chart-legend>span");legend[0].classList.toggle("hidden",state.mode!=="capacity");legend[1].classList.toggle("hidden",state.mode!=="capacity");legend[3].classList.toggle("hidden",state.mode!=="fragmentation");}
  function eventTime(e){const rect=$("main-chart").getBoundingClientRect();const xx=(e.clientX-rect.left)/rect.width*mainGeometry.width;const f=Math.max(0,Math.min(1,(xx-mainGeometry.pad.left)/mainGeometry.chartWidth));return f*state.info.duration_ns;}
  function rangeFromTimes(a,b){const start=Math.min(a,b),end=Math.max(a,b);let from=Math.floor(start/state.info.window_ns),to=Math.ceil(end/state.info.window_ns);from=Math.max(0,Math.min(state.info.window_count-1,from));to=Math.max(from+1,Math.min(state.info.window_count,to));return[from,to];}
  function updateRange(from,to){if(!state.info)return;from=Number.isFinite(from)?Math.trunc(from):0;to=Number.isFinite(to)?Math.trunc(to):state.info.window_count;state.from=Math.max(0,Math.min(state.info.window_count-1,from));state.to=Math.max(state.from+1,Math.min(state.info.window_count,to));state.offset=0;$("from-window").value=state.from;$("to-window").value=state.to-1;$("range-label").textContent=rangeText();$("range-resolution").textContent=`Windows ${state.from}–${state.to-1} · ${state.to-state.from} selected`;renderCurve();loadRankings();if(state.detailID)loadDetail(state.detailID);}
  $("main-chart").addEventListener("pointerdown",e=>{if(!mainGeometry)return;drag={start:eventTime(e)};$("main-chart").setPointerCapture(e.pointerId);$("chart-tooltip").classList.add("hidden");});
  $("main-chart").addEventListener("pointermove",e=>{if(!mainGeometry)return;if(drag){const [from,to]=rangeFromTimes(drag.start,eventTime(e));const s=Math.min(state.info.duration_ns,from*state.info.window_ns),t=Math.min(state.info.duration_ns,to*state.info.window_ns);const existing=$("main-chart").querySelector(".selected-area");if(existing){existing.setAttribute("x",mainGeometry.x(s));existing.setAttribute("width",Math.max(0,mainGeometry.x(t)-mainGeometry.x(s)));}return;}if(!state.curve.length)return;const at=eventTime(e);const p=state.curve.reduce((best,p)=>Math.abs(p.time_ns-at)<Math.abs(best.time_ns-at)?p:best,state.curve[0]);const tip=$("chart-tooltip");tip.replaceChildren(node("strong",{},`${time(p.time_ns)} · ${p.window<0?"baseline":"window "+p.window}`));for(const[label,value]of[["DB size",bytes(p.db_size)],["In use",bytes(p.db_size_in_use)],["Reusable free",bytes(p.free_bytes)],["Free / DB",`${p.fragmentation_percent.toFixed(2)}%`]]){const row=node("div");row.append(node("span",{},label),node("span",{},value));tip.append(row);}tip.classList.remove("hidden");const r=$("main-chart").getBoundingClientRect();tip.style.left=Math.min(Math.max(0,e.clientX-r.left+15),r.width-190)+"px";tip.style.top="18px";});
  $("main-chart").addEventListener("pointerup",e=>{if(!drag)return;const [from,to]=rangeFromTimes(drag.start,eventTime(e));drag=null;updateRange(from,to);});
  $("main-chart").addEventListener("pointercancel",()=>{drag=null;renderCurve();});
  $("main-chart").addEventListener("pointerleave",()=>$("chart-tooltip").classList.add("hidden"));

  function renderTotals(t){state.totals=t;const first=t.physical_start,last=t.physical_end;const f0=first.db_size-first.db_size_in_use,f1=last.db_size-last.db_size_in_use;$("stat-db").textContent=bytes(last.db_size);$("stat-db-delta").textContent=bytes(last.db_size-first.db_size,true)+" over selection";$("stat-inuse").textContent=bytes(last.db_size_in_use);$("stat-inuse-delta").textContent=bytes(last.db_size_in_use-first.db_size_in_use,true)+" over selection";$("stat-free").textContent=bytes(f1);$("stat-free-delta").textContent=bytes(f1-f0,true)+" net change";$("stat-ratio").textContent=(last.db_size?100*f1/last.db_size:0).toFixed(2)+"%";$("interval-summary").textContent=`Selected interval: gross free creation ${bytes(t.free_growth_bytes)}, free reuse ${bytes(t.free_reuse_bytes)}. Unattributed: DB growth ${bytes(t.unattributed_db_growth_bytes)}, free growth ${bytes(t.unattributed_free_growth_bytes)}, free reuse ${bytes(t.unattributed_free_reuse_bytes)}. Pending pages at end: ${exact(last.pending_pages)}. Free / DB describes reusable pages, not slack inside allocated pages.`;}
  function clearRankings(message) {
    state.totals=null;
    $("rank-body").replaceChildren();
    $("rank-empty").classList.add("hidden");
    $("rank-denominator").textContent="";$("rank-page").textContent="";
    $("rank-prev").disabled=true;$("rank-next").disabled=true;
    for(const id of ["stat-db","stat-inuse","stat-free","stat-ratio"])$(id).textContent="—";
    for(const id of ["stat-db-delta","stat-inuse-delta","stat-free-delta"])$(id).textContent=message;
    $("interval-summary").textContent=message;
  }
  async function loadRankings() {
    if(!state.info)return;
    const query={from:state.from,to:state.to,scope:state.scope,metric:state.metric,search:state.search,resource:state.resource,namespace:state.namespace,label_key:state.labelKey,label_value:state.labelFilter?state.labelValue:"",label_filter:state.labelFilter,group_scope:state.groupScope,group_value:state.groupValue,limit:state.limit,offset:state.offset};
    const scope=query.scope;
    clearRankings("Computing selected interval…");
    setError("rank-error","");$("rank-status").textContent="Computing…";
    $("rank-body").setAttribute("aria-busy","true");
    $("metric-description").textContent=descriptions[query.metric];
    $("resource-scope").classList.toggle("active",scope==="resource");$("object-scope").classList.toggle("active",scope==="object");
    $("group-scope").value=scope;
    $("identity-heading").textContent=scope==="object"?"OBJECT / STORAGE KEY":scope==="resource"?"RESOURCE / STORAGE PREFIX":scope.toUpperCase();
    $("group-filter").classList.toggle("hidden",!query.group_scope);
    $("group-filter").textContent=state.groupName?state.groupName+" ×":"";
    $("resource-filter").classList.toggle("hidden",!query.resource);$("resource-filter").textContent=query.resource?query.resource+" ×":"";
    try {
      const r=await request("rankings","/api/rankings",query);
      renderTotals(r.totals);
      $("attribution-note").textContent=r.attribution||"Metadata was not recorded in this analysis.";
      for(const[itemIndex,item]of(r.items||[]).entries()) {
        const row=node("tr",{tabindex:"0",role:"button","aria-label":item.group?`Explore ${item.group.value} objects`:scope==="resource"?`Explore ${item.identity.resource} objects`:`Open ${identityName(item.identity)}`});
        row.append(node("td",{class:"rank-number"},String(r.offset+itemIndex+1)));
        const id=node("td");id.append(node("div",{class:"identity-main"},item.group?item.group.value:scope==="resource"?item.identity.resource:identityName(item.identity)));
        id.append(node("div",{class:"identity-sub"},item.group?`${item.group.status} · ${item.group.label_key||item.group.scope}`:scope==="resource"?(item.identity.api_group?"API group hint: "+item.identity.api_group:"Storage prefix"):(objectSub(item.identity)||displayKey(item.identity))));row.append(id);
        row.append(node("td",{class:"number",title:exact(item.bytes)+" bytes"},bytes(item.bytes)));
        const share=node("td",{class:"number"}),wrap=node("div",{class:"share-wrap"}),bar=node("span",{class:"share-bar"}),fill=node("i");
        fill.style.width=Math.min(100,item.share_percent)+"%";bar.append(fill);wrap.append(bar,node("span",{},item.share_percent.toFixed(2)+"%"));share.append(wrap);row.append(share,node("td",{class:"arrow-cell"},"↗"));
        const open=()=>{if(item.group){state.groupScope=item.group.scope;state.groupValue=item.group.key;state.groupName=item.group.value;state.scope="object";state.search="";$("rank-search").value="";state.offset=0;loadRankings();}else if(scope==="resource"){state.resource=item.identity.resource;state.scope="object";state.offset=0;loadRankings();}else loadDetail(item.id);};
        row.addEventListener("click",open);row.addEventListener("keydown",e=>{if(e.key==="Enter"||e.key===" "){e.preventDefault();open();}});$("rank-body").append(row);
      }
      $("rank-empty").classList.toggle("hidden",(r.items||[]).length>0);
      $("rank-status").textContent=`${exact(r.total)} contributors`;
      $("rank-denominator").textContent=`Share of ${bytes(r.denominator_bytes)} · ${r.denominator_label}`;
      $("rank-page").textContent=r.total?`${r.offset+1}–${Math.min(r.offset+r.limit,r.total)} / ${exact(r.total)}`:"0";
      $("rank-prev").disabled=r.offset===0;$("rank-next").disabled=r.offset+r.limit>=r.total;
      $("rank-body").setAttribute("aria-busy","false");
    } catch(err) {
      if(ignoreAbort(err))return;
      clearRankings("Selected interval unavailable.");
      setError("rank-error",err.message);$("rank-status").textContent="Unavailable";
      $("rank-body").setAttribute("aria-busy","false");
    }
  }
  async function loadObjects(){setError("object-search-error","");try{const r=await request("objects","/api/objects",{search:state.objectSearch,limit:8,offset:state.objectOffset});$("object-list").replaceChildren();for(const item of r.items||[]){const b=node("button",{type:"button",class:"object-item"});b.append(node("strong",{},identityName(item.identity)),node("span",{},objectSub(item.identity)||displayKey(item.identity)));b.addEventListener("click",()=>loadDetail(item.id));$("object-list").append(b);}if(!(r.items||[]).length)$("object-list").append(node("p",{class:"subtle",style:"padding:20px 13px"},"No matching objects."));$("object-count").textContent=`${exact(r.total)} observed keys`;$("more-objects").disabled=r.total<=8;$("more-objects").textContent=r.offset+r.limit>=r.total?"Back to first ←":"More →";$("more-objects").onclick=()=>{state.objectOffset=r.offset+r.limit>=r.total?0:r.offset+r.limit;loadObjects();};}catch(err){if(!ignoreAbort(err))setError("object-search-error",err.message);}}

  let previousFocus=null;
  async function loadDetail(id) {
    if(!state.detailID)previousFocus=document.activeElement;
    state.detailID=id;state.detail=null;
    $("object-title").textContent="Loading object…";
    $("detail-content").classList.add("hidden");
    $("object-drawer").setAttribute("aria-busy","true");
    $("object-drawer").classList.remove("hidden");$("drawer-backdrop").classList.remove("hidden");
    document.body.classList.add("drawer-open");$("object-drawer").focus();setError("detail-error","");
    try {
      const d=await request("detail","/api/object",{id,from:state.from,to:state.to,points:600});
      state.detail=d;
      // Restore layout before measuring the responsive SVG's dimensions.
      $("detail-content").classList.remove("hidden");renderDetail(d);
      $("object-drawer").setAttribute("aria-busy","false");
    } catch(err) {
      if(ignoreAbort(err))return;
      state.detail=null;$("detail-content").classList.add("hidden");
      $("object-title").textContent="Object unavailable";setError("detail-error",err.message);
      $("object-drawer").setAttribute("aria-busy","false");
    }
  }
  function metadataText(m={}) {
    const labels=Object.entries(m.labels||{}).sort(([a],[b])=>a.localeCompare(b)).map(([k,v])=>k+"="+v).join("\n");
    const c=m.controller;
    return {Status:m.status||"not-recorded",API:m.api_version||"—",Kind:m.kind||"—",Namespace:m.namespace||"(cluster-scoped or unavailable)",UID:m.uid||"—",Labels:labels||"(none of the configured keys)",Controller:c?`${c.api_version} ${c.kind}/${c.name}\nUID ${c.uid}`:"(none recorded)","Manager set":(m.managers||[]).join(", ")||"(none recorded)"};
  }
  function renderMetadata(d) {
    const m=d.metadata||{}, start=d.metadata_start||{};
    $("object-metadata").replaceChildren();
    for(const[label,value]of Object.entries(metadataText(m)))$("object-metadata").append(node("dt",{},label),node("dd",{},value));
    const changed=JSON.stringify([start.uid,start.labels,start.controller,start.managers])!==JSON.stringify([m.uid,m.labels,m.controller,m.managers]);
    let note="Latest observed metadata at this interval's end. Manager sets describe managedFields, not an authenticated last writer. Controller is the immediate owner.";
    if(m.identity_mismatch)note+=" Metadata identity differs from the storage path.";
    if(changed){note+=" Metadata differs from the interval start.";const before=metadataText(start);$("object-metadata").append(node("dt",{},"Start labels"),node("dd",{},before.Labels),node("dt",{},"Start UID"),node("dd",{},before.UID));}
    $("metadata-note").textContent=note;
  }
  function closeDetail(){if(state.requests.detail)state.requests.detail.abort();state.detailID=null;state.detail=null;$("object-drawer").classList.add("hidden");$("drawer-backdrop").classList.add("hidden");document.body.classList.remove("drawer-open");if(previousFocus?.isConnected)previousFocus.focus();}
  function renderDetail(d){renderMetadata(d);$("object-title").textContent=identityName(d.identity);$("object-key").textContent=displayKey(d.identity);$("object-badges").replaceChildren();for(const label of [d.identity.resource,d.identity.namespace?"namespace: "+d.identity.namespace:d.identity.scope,"mapping: "+d.identity.confidence])if(label)$("object-badges").append(node("span",{},label));$("detail-range").textContent=`Selected interval ${rangeText()} · windows ${d.from}–${d.to-1}`;$("object-stats").replaceChildren();for(const[label,value,delta]of[["Live value at end",d.end.live_value_bytes,d.end.live_value_bytes-d.start.live_value_bytes],["Retained MVCC at end",d.end.retained_mvcc_bytes,d.end.retained_mvcc_bytes-d.start.retained_mvcc_bytes],["History at end",d.end.history_bytes,d.end.history_bytes-d.start.history_bytes]]){const box=node("div");box.append(node("span",{},label),node("strong",{},bytes(value)),node("small",{},bytes(delta,true)+" in selection"));$("object-stats").append(box);}drawChart($("object-chart"),(d.points||[]).map(p=>({...p,...p.stock})),[{color:"var(--blue)",value:p=>p.retained_mvcc_bytes,fill:true},{color:"var(--mint)",value:p=>p.live_record_bytes},{color:"var(--amber)",value:p=>p.history_bytes}],{width:700,height:230,leftMargin:78,start:startTime(),end:endTime()});const m=d.metrics;$("object-impact").replaceChildren();for(const[label,value]of[["Puts / deleted keys",`${exact(m.puts)} / ${exact(m.deletes)}`],["Value bytes written",bytes(m.value_write_bytes)],["Compacted MVCC bytes",bytes(m.compacted_mvcc_bytes)],["Estimated DB growth",bytes(m.estimated_db_growth_bytes)],["Estimated free growth",bytes(m.estimated_free_growth_bytes)],["Estimated free reuse",bytes(m.estimated_free_reuse_bytes)],["Estimated net free",bytes(m.estimated_free_growth_bytes-m.estimated_free_reuse_bytes,true)],["Contributing Raft indexes",d.first_index?`${exact(d.first_index)}–${exact(d.last_index)}`:"No interval activity"]]){const row=node("div",{class:"impact-row"});row.append(node("span",{},label),node("strong",{},value));$("object-impact").append(row);}$("object-changes").replaceChildren();const changes=(d.points||[]).filter(p=>p.changed);for(const p of changes){const row=node("tr"),m=p.metrics;row.append(node("td",{},time(p.time_ns)),node("td",{class:"number"},`${exact(m.puts)} / ${exact(m.deletes)}`),node("td",{class:"number"},bytes(m.value_write_bytes)),node("td",{class:"number"},bytes(m.compacted_mvcc_bytes)),node("td",{class:"number"},bytes(m.estimated_free_growth_bytes-m.estimated_free_reuse_bytes,true)));$("object-changes").append(row);}if(!changes.length){const row=node("tr");row.append(node("td",{colspan:"5"},"No changes in this interval. Stock is carried forward."));$("object-changes").append(row);}$("detail-sampling").textContent=(d.sampled?`Timeline sampled to ${d.points.length} of ${exact(d.total_points)} boundary points; the change table shows sampled points. Totals above include every selected window. `:"Timeline includes every selected window boundary. ")+`Baseline live value: ${bytes(d.baseline.live_value_bytes)}; final run live value: ${bytes(d.final.live_value_bytes)}. Identity follows the storage key across recreation.`;}

  $("capacity-mode").onclick=()=>{state.mode="capacity";renderCurve();};$("fragmentation-mode").onclick=()=>{state.mode="fragmentation";renderCurve();};
  $("reset-range").onclick=()=>updateRange(0,state.info.window_count);
  $("from-window").onchange=()=>updateRange(Number($("from-window").value),Math.max(Number($("from-window").value)+1,state.to));
  $("to-window").onchange=()=>updateRange(Math.min(state.from,Number($("to-window").value)),Number($("to-window").value)+1);
  function chooseScope(scope){state.scope=scope;state.groupScope="";state.groupValue="";state.groupName="";state.offset=0;loadRankings();}
  $("resource-scope").onclick=()=>chooseScope("resource");$("object-scope").onclick=()=>chooseScope("object");
  $("group-scope").onchange=()=>chooseScope($("group-scope").value);
  $("namespace-filter").oninput=debounce(()=>{state.namespace=$("namespace-filter").value;state.offset=0;loadRankings();});
  $("label-key").onchange=()=>{state.labelKey=$("label-key").value;state.groupScope="";state.groupValue="";state.groupName="";state.offset=0;loadRankings();};
  function applyLabelFilter(){state.labelValue=$("label-value").value;state.labelFilter=$("label-filter-enabled").checked;state.offset=0;loadRankings();}
  $("label-value").oninput=debounce(applyLabelFilter);$("label-filter-enabled").onchange=applyLabelFilter;
  $("group-filter").onclick=()=>{state.groupScope="";state.groupValue="";state.groupName="";state.offset=0;loadRankings();};
  $("metric").onchange=()=>{state.metric=$("metric").value;state.offset=0;loadRankings();};
  $("rank-search").oninput=debounce(()=>{state.search=$("rank-search").value;state.offset=0;loadRankings();});
  $("resource-filter").onclick=()=>{state.resource="";state.offset=0;loadRankings();};
  $("rank-prev").onclick=()=>{state.offset=Math.max(0,state.offset-state.limit);loadRankings();};$("rank-next").onclick=()=>{state.offset+=state.limit;loadRankings();};
  $("object-search").oninput=debounce(()=>{state.objectSearch=$("object-search").value;state.objectOffset=0;loadObjects();});
  $("model-toggle").onclick=()=>{const expanded=$("model-details").classList.toggle("hidden");$("model-toggle").textContent=expanded?"View model details":"Hide model details";};
  $("close-drawer").onclick=closeDetail;$("drawer-backdrop").onclick=closeDetail;
  document.addEventListener("keydown",e=>{if(e.key==="Escape"&&state.detailID)closeDetail();if(e.key==="Tab"&&state.detailID){const f=$("object-drawer").querySelectorAll("button,input,select,[tabindex='0']");if(!f.length)return;const first=f[0],last=f[f.length-1];if(e.shiftKey&&(document.activeElement===first||document.activeElement===$("object-drawer"))){e.preventDefault();last.focus();}else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}}});
  let theme="dark";try{theme=localStorage.getItem("etcd-trend-theme")||"dark";}catch(_){}document.documentElement.dataset.theme=theme;
  $("theme").onclick=()=>{theme=theme==="dark"?"light":"dark";document.documentElement.dataset.theme=theme;try{localStorage.setItem("etcd-trend-theme",theme);}catch(_){};};
  window.addEventListener("resize",debounce(()=>{renderCurve();if(state.detail)renderDetail(state.detail);},100));
  async function init(){try{state.info=await request("info","/api/info");if(!state.info.window_count)throw new Error("This run has no analysis windows.");state.to=state.info.window_count;
      const keys=state.info.label_keys||[];
      $("label-key").replaceChildren(...(keys.length?keys.map(k=>node("option",{value:k},k)):[node("option",{value:""},"No labels recorded")]));
      state.labelKey=keys[0]||"";
      $("label-key").disabled=!keys.length;$("label-value").disabled=!keys.length;$("label-filter-enabled").disabled=!keys.length;
      for(const option of $("group-scope").options)if(option.value==="label"||option.value==="namespace-label")option.disabled=!keys.length;
      $("source-version").textContent="Source "+state.info.source_version;$("engine-version").textContent=state.info.engine;$("window-count").textContent=exact(state.info.window_count)+" windows";$("from-window").max=state.info.window_count-1;$("to-window").max=state.info.window_count-1;$("model-details").textContent=state.info.model;$("data-resolution").textContent=`Analysis window ${time(state.info.window_ns)} · revision ${exact(state.info.final_revision)}`;state.curve=await request("curve","/api/curve",{points:1200});updateRange(0,state.to);loadObjects();}catch(err){if(!ignoreAbort(err)){setError("fatal",err.message);$("range-label").textContent="Replay could not be loaded";}}}
  init();
})();
