"use strict";(self.webpackChunksite=self.webpackChunksite||[]).push([[5994],{3905:(e,t,r)=>{r.d(t,{Zo:()=>p,kt:()=>f});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var c=n.createContext({}),l=function(e){var t=n.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},p=function(e){var t=l(e.components);return n.createElement(c.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,c=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),u=l(r),m=a,f=u["".concat(c,".").concat(m)]||u[m]||d[m]||i;return r?n.createElement(f,o(o({ref:t},p),{},{components:r})):n.createElement(f,o({ref:t},p))}));function f(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=m;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s[u]="string"==typeof e?e:a,o[1]=s;for(var l=2;l<i;l++)o[l]=r[l];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},4377:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>c,contentTitle:()=>o,default:()=>d,frontMatter:()=>i,metadata:()=>s,toc:()=>l});var n=r(7462),a=(r(7294),r(3905));const i={description:"Flow's default reduction behaviors and available strategies to customize them"},o="Reduction strategies",s={unversionedId:"reference/reduction-strategies/README",id:"reference/reduction-strategies/README",title:"Reduction strategies",description:"Flow's default reduction behaviors and available strategies to customize them",source:"@site/docs/reference/reduction-strategies/README.md",sourceDirName:"reference/reduction-strategies",slug:"/reference/reduction-strategies/",permalink:"/pr-preview/pr-1183/reference/reduction-strategies/",draft:!1,editUrl:"https://github.com/estuary/flow/edit/master/site/docs/reference/reduction-strategies/README.md",tags:[],version:"current",frontMatter:{description:"Flow's default reduction behaviors and available strategies to customize them"},sidebar:"tutorialSidebar",previous:{title:"Configuring task shards",permalink:"/pr-preview/pr-1183/reference/Configuring-task-shards"},next:{title:"append",permalink:"/pr-preview/pr-1183/reference/reduction-strategies/append"}},c={},l=[{value:"Reduction guarantees",id:"reduction-guarantees",level:3}],p={toc:l},u="wrapper";function d(e){let{components:t,...r}=e;return(0,a.kt)(u,(0,n.Z)({},p,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"reduction-strategies"},"Reduction strategies"),(0,a.kt)("p",null,"Flow uses ",(0,a.kt)("a",{parentName:"p",href:"/pr-preview/pr-1183/concepts/schemas#reductions"},"reductions"),"\nto aggregate data in the runtime in order to improve endpoint performance.\nReductions tell Flow how two versions of a document can be meaningfully combined. Guarantees that underlie all Flow reduction behavior are explained in depth ",(0,a.kt)("a",{parentName:"p",href:"./#reduction-guarantees"},"below"),"."),(0,a.kt)("p",null,"Some reductions occur automatically during captures and materializations to optimize performance, but you can define more advanced behavior using reduction annotations in collection schemas."),(0,a.kt)("p",null,"The available strategies are:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"/pr-preview/pr-1183/reference/reduction-strategies/append"},"append")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"/pr-preview/pr-1183/reference/reduction-strategies/firstwritewins-and-lastwritewins"},"firstWriteWins and lastWriteWins")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"/pr-preview/pr-1183/reference/reduction-strategies/merge"},"merge")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"/pr-preview/pr-1183/reference/reduction-strategies/minimize-and-maximize"},"minimize and maximize")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"/pr-preview/pr-1183/reference/reduction-strategies/set"},"set")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"/pr-preview/pr-1183/reference/reduction-strategies/sum"},"sum"))),(0,a.kt)("p",null,"When no other strategy is specified in a schema, Flow defaults to ",(0,a.kt)("inlineCode",{parentName:"p"},"lastWriteWins"),".  For even more customization, you can use ",(0,a.kt)("a",{parentName:"p",href:"/pr-preview/pr-1183/reference/reduction-strategies/composing-with-conditionals"},"conditional statements"),"."," "),(0,a.kt)("admonition",{type:"info"},(0,a.kt)("p",{parentName:"admonition"},"Estuary has many future plans for reduction annotations:"),(0,a.kt)("blockquote",{parentName:"admonition"},(0,a.kt)("ul",{parentName:"blockquote"},(0,a.kt)("li",{parentName:"ul"},"More strategies, including data sketches like HyperLogLogs, T-Digests, and others."),(0,a.kt)("li",{parentName:"ul"},"Eviction policies and constraints, for bounding the sizes of objects and arrays with fine-grained removal ordering."))),(0,a.kt)("p",{parentName:"admonition"},"What\u2019s here today can be considered a minimal, useful proof-of-concept.")),(0,a.kt)("h3",{id:"reduction-guarantees"},"Reduction guarantees"),(0,a.kt)("p",null,"In Flow, documents that share the same collection key and are written to the same logical partition have a ",(0,a.kt)("strong",{parentName:"p"},"total order,")," meaning that one document is universally understood to have been written before the other."),(0,a.kt)("p",null,"This isn't true of documents of the same key written to different logical partitions. These documents can be considered \u201cmostly\u201d ordered: Flow uses timestamps to understand the relative ordering of these documents, and while this largely produces the desired outcome, small amounts of re-ordering are possible and even likely."),(0,a.kt)("p",null,"Flow guarantees ",(0,a.kt)("strong",{parentName:"p"},"exactly-once")," semantics within derived collections and materializations (so long as the target system supports transactions), and a document reduction will be applied exactly one time."),(0,a.kt)("p",null,"Flow does ",(0,a.kt)("em",{parentName:"p"},"not")," guarantee that documents are reduced in sequential order, directly into a base document. For example, documents of a single Flow capture transaction are combined together into one document per collection key at capture time \u2013 and that document may be again combined with still others, and so on until a final reduction into the base document occurs."),(0,a.kt)("p",null,"Taken together, these total-order and exactly-once guarantees mean that reduction strategies must be ",(0,a.kt)("em",{parentName:"p"},"associative")," ","[","as in (2 + 3) + 4 = 2 + (3 + 4) ], but need not be commutative ","["," 2 + 3 = 3 + 2 ] or idempotent ","["," S u S = S ]. They expand the palette of strategies that can be implemented, and allow for more efficient implementations as compared to, for example ",(0,a.kt)("a",{parentName:"p",href:"https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type"},"CRDTs"),"."),(0,a.kt)("p",null,"In this documentation, we\u2019ll refer to the \u201cleft-hand side\u201d (LHS) as the preceding document and the \u201cright-hand side\u201d (RHS) as the following one. Keep in mind that both the LHS and RHS may themselves represent a combination of still more ordered documents because, for example, reductions are applied ",(0,a.kt)("em",{parentName:"p"},"associatively"),"."))}d.isMDXComponent=!0}}]);