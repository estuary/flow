"use strict";(self.webpackChunksite=self.webpackChunksite||[]).push([[9935],{58071:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>a,default:()=>f,frontMatter:()=>o,metadata:()=>i,toc:()=>c});var r=n(74848),s=n(28453);const o={},a="SingleStore (Cloud)",i={id:"reference/Connectors/dekaf/dekaf-singlestore",title:"SingleStore (Cloud)",description:"This guide demonstrates how to use Estuary Flow to stream data to SingleStore using the Kafka-compatible Dekaf API.",source:"@site/docs/reference/Connectors/dekaf/dekaf-singlestore.md",sourceDirName:"reference/Connectors/dekaf",slug:"/reference/Connectors/dekaf/dekaf-singlestore",permalink:"/reference/Connectors/dekaf/dekaf-singlestore",draft:!1,unlisted:!1,editUrl:"https://github.com/estuary/flow/edit/master/site/docs/reference/Connectors/dekaf/dekaf-singlestore.md",tags:[],version:"current",frontMatter:{},sidebar:"tutorialSidebar",previous:{title:"Materialize",permalink:"/reference/Connectors/dekaf/dekaf-materialize"},next:{title:"StarTree",permalink:"/reference/Connectors/dekaf/dekaf-startree"}},l={},c=[{value:"Connecting Estuary Flow to SingleStore",id:"connecting-estuary-flow-to-singlestore",level:2}];function d(e){const t={a:"a",code:"code",h1:"h1",h2:"h2",li:"li",ol:"ol",p:"p",pre:"pre",...(0,s.R)(),...e.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(t.h1,{id:"singlestore-cloud",children:"SingleStore (Cloud)"}),"\n",(0,r.jsx)(t.p,{children:"This guide demonstrates how to use Estuary Flow to stream data to SingleStore using the Kafka-compatible Dekaf API."}),"\n",(0,r.jsxs)(t.p,{children:[(0,r.jsx)(t.a,{href:"https://www.singlestore.com/",children:"SingleStore"})," is a distributed SQL database designed for data-intensive applications,\noffering high performance for both transactional and analytical workloads."]}),"\n",(0,r.jsx)(t.h2,{id:"connecting-estuary-flow-to-singlestore",children:"Connecting Estuary Flow to SingleStore"}),"\n",(0,r.jsxs)(t.ol,{children:["\n",(0,r.jsxs)(t.li,{children:["\n",(0,r.jsxs)(t.p,{children:[(0,r.jsx)(t.a,{href:"/guides/how_to_generate_refresh_token",children:"Generate a refresh token"})," for the SingleStore connection from the Estuary\nAdmin Dashboard."]}),"\n"]}),"\n",(0,r.jsxs)(t.li,{children:["\n",(0,r.jsx)(t.p,{children:"In the SingleStore Cloud Portal, navigate to the SQL Editor section of the Data Studio."}),"\n"]}),"\n",(0,r.jsxs)(t.li,{children:["\n",(0,r.jsx)(t.p,{children:"Execute the following script to create a table and an ingestion pipeline to hydrate it."}),"\n",(0,r.jsx)(t.p,{children:"This example will ingest data from the demo wikipedia collection in Estuary Flow."}),"\n",(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{className:"language-sql",children:'CREATE TABLE test_table (id NUMERIC, server_name VARCHAR(255), title VARCHAR(255));\n\nCREATE PIPELINE test AS\n        LOAD DATA KAFKA "dekaf.estuary-data.com:9092/demo/wikipedia/recentchange-sampled"\n        CONFIG \'{\n            "security.protocol":"SASL_SSL",\n            "sasl.mechanism":"PLAIN",\n            "sasl.username":"{}",\n            "broker.address.family": "v4",\n            "schema.registry.username": "{}",\n            "fetch.wait.max.ms": "2000"\n        }\'\n        CREDENTIALS \'{\n            "sasl.password": "ESTUARY_ACCESS_TOKEN",\n            "schema.registry.password": "ESTUARY_ACCESS_TOKEN"\n        }\'\n        INTO table test_table\n        FORMAT AVRO SCHEMA REGISTRY \'https://dekaf.estuary-data.com\'\n        ( id <- id, server_name <- server_name, title <- title );\n'})}),"\n"]}),"\n",(0,r.jsxs)(t.li,{children:["\n",(0,r.jsx)(t.p,{children:"Your pipeline should now start ingesting data from Estuary Flow into SingleStore."}),"\n"]}),"\n"]})]})}function f(e={}){const{wrapper:t}={...(0,s.R)(),...e.components};return t?(0,r.jsx)(t,{...e,children:(0,r.jsx)(d,{...e})}):d(e)}},28453:(e,t,n)=>{n.d(t,{R:()=>a,x:()=>i});var r=n(96540);const s={},o=r.createContext(s);function a(e){const t=r.useContext(o);return r.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function i(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(s):e.components||s:a(e.components),r.createElement(o.Provider,{value:t},e.children)}}}]);