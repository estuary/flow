"use strict";(self.webpackChunksite=self.webpackChunksite||[]).push([[2277],{46937:(e,t,o)=>{o.r(t),o.d(t,{assets:()=>l,contentTitle:()=>r,default:()=>d,frontMatter:()=>a,metadata:()=>i,toc:()=>c});var n=o(74848),s=o(28453);const a={},r="Quickstart for Flow",i={id:"getting-started/quickstart/quickstart",title:"Quickstart for Flow",description:"In this tutorial, you will learn how to set up a streaming Change Data Capture (CDC) pipeline from PostgreSQL to",source:"@site/docs/getting-started/quickstart/quickstart.md",sourceDirName:"getting-started/quickstart",slug:"/getting-started/quickstart/",permalink:"/pr-preview/pr-1782/getting-started/quickstart/",draft:!1,unlisted:!1,editUrl:"https://github.com/estuary/flow/edit/master/site/docs/getting-started/quickstart/quickstart.md",tags:[],version:"current",frontMatter:{},sidebar:"tutorialSidebar",previous:{title:"What is Estuary Flow?",permalink:"/pr-preview/pr-1782/"},next:{title:"Deployment options",permalink:"/pr-preview/pr-1782/getting-started/deployment-options"}},l={},c=[{value:"Step 1. Set up a Capture<a></a>",id:"step-1-set-up-a-capture",level:2},{value:"Step 2. Set up a Materialization<a></a>",id:"step-2-set-up-a-materialization",level:2},{value:"Next Steps<a></a>",id:"next-steps",level:2}];function u(e){const t={a:"a",em:"em",h1:"h1",h2:"h2",img:"img",li:"li",ol:"ol",p:"p",strong:"strong",...(0,s.R)(),...e.components},{Head:o}=t;return o||function(e,t){throw new Error("Expected "+(t?"component":"object")+" `"+e+"` to be defined: you likely forgot to import, pass, or provide it.")}("Head",!0),(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(t.h1,{id:"quickstart-for-flow",children:"Quickstart for Flow"}),"\n",(0,n.jsx)(o,{children:(0,n.jsx)("meta",{property:"og:image",content:"https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//architecture_6bbaf2c5a6/architecture_6bbaf2c5a6.png"})}),"\n",(0,n.jsx)(t.p,{children:"In this tutorial, you will learn how to set up a streaming Change Data Capture (CDC) pipeline from PostgreSQL to\nSnowflake using Estuary Flow."}),"\n",(0,n.jsx)(t.p,{children:"Before you get started, make sure you do two things."}),"\n",(0,n.jsxs)(t.ol,{children:["\n",(0,n.jsxs)(t.li,{children:["\n",(0,n.jsxs)(t.p,{children:["Sign up for Estuary Flow ",(0,n.jsx)(t.a,{href:"https://dashboard.estuary.dev/register",children:"here"}),". It\u2019s simple, fast and free."]}),"\n"]}),"\n",(0,n.jsxs)(t.li,{children:["\n",(0,n.jsxs)(t.p,{children:["Make sure you also join\nthe ",(0,n.jsx)(t.a,{href:"https://estuary-dev.slack.com/ssb/redirect#/shared-invite/email",children:"Estuary Slack Community"}),". Don\u2019t struggle. Just\nask a question."]}),"\n"]}),"\n"]}),"\n",(0,n.jsx)(t.p,{children:"When you register for Flow, your account will use Flow's secure cloud storage bucket to store your data.\nData in Flow's cloud storage bucket is deleted 20 days after collection."}),"\n",(0,n.jsxs)(t.p,{children:["For production use cases, you\nshould ",(0,n.jsx)(t.a,{href:"#configuring-your-cloud-storage-bucket-for-use-with-flow",children:"configure your own cloud storage bucket to use with Flow"}),"."]}),"\n",(0,n.jsxs)(t.h2,{id:"step-1-set-up-a-capture",children:["Step 1. Set up a Capture",(0,n.jsx)("a",{id:"step-2-set-up-a-capture"})]}),"\n",(0,n.jsxs)(t.p,{children:["Head over to your Flow dashboard (if you haven\u2019t registered yet, you can do\nso ",(0,n.jsx)(t.a,{href:"https://dashboard.estuary.dev/register",children:"here"}),".) and create a new ",(0,n.jsx)(t.strong,{children:"Capture."})," A capture is how Flow ingests data\nfrom an external source."]}),"\n",(0,n.jsxs)(t.p,{children:["Go to the sources page by clicking on the ",(0,n.jsx)(t.strong,{children:"Sources"})," on the left hand side of your screen, then click on ",(0,n.jsx)(t.strong,{children:"+ New\nCapture"})]}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.img,{src:"https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//new_capture_4583a8a120/new_capture_4583a8a120.png",alt:"Add new Capture"})}),"\n",(0,n.jsxs)(t.p,{children:["Configure the connection to the database and press ",(0,n.jsx)(t.strong,{children:"Next."})]}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.img,{src:"https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//capture_configuration_89e2133f83/capture_configuration_89e2133f83.png",alt:"Configure Capture"})}),"\n",(0,n.jsx)(t.p,{children:"On the following page, we can configure how our incoming data should be represented in Flow as collections. As a quick\nrefresher, let\u2019s recap how Flow represents data on a high level."}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.strong,{children:"Documents"})}),"\n",(0,n.jsxs)(t.p,{children:["The documents of your flows are stored in collections: real-time data lakes of JSON documents in cloud storage.\nDocuments being backed by an object storage mean that once you start capturing data, you won\u2019t have to worry about it\nnot being available to replay \u2013 object stores such as S3 can be configured to cheaply store data forever.\nSee ",(0,n.jsx)(t.a,{href:"https://docs.estuary.dev/concepts/collections/#documents",children:"docs page"})," for more information."]}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.strong,{children:"Schemas"})}),"\n",(0,n.jsx)(t.p,{children:"Flow documents and collections always have an associated schema that defines the structure, representation, and\nconstraints of your documents. In most cases, Flow generates a functioning schema on your behalf during the discovery\nphase of capture, which has already automatically happened - that\u2019s why you\u2019re able to take a peek into the structure of\nthe incoming data!"}),"\n",(0,n.jsx)(t.p,{children:"To see how Flow parsed the incoming records, click on the Collection tab and verify the inferred schema looks correct."}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.img,{src:"https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//collections_configuration_34e53025c7/collections_configuration_34e53025c7.png",alt:"Configure Collections"})}),"\n",(0,n.jsxs)(t.h2,{id:"step-2-set-up-a-materialization",children:["Step 2. Set up a Materialization",(0,n.jsx)("a",{id:"step-3-set-up-a-materialization"})]}),"\n",(0,n.jsx)(t.p,{children:"Similarly to the source side, we\u2019ll need to set up some initial configuration in Snowflake to allow Flow to materialize\ncollections into a table."}),"\n",(0,n.jsxs)(t.p,{children:["Head over to the ",(0,n.jsx)(t.strong,{children:"Destinations"})," page, where you\ncan ",(0,n.jsx)(t.a,{href:"https://dashboard.estuary.dev/materializations/create",children:"create a new Materialization"}),"."]}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.img,{src:"https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//new_materialization_31df04d81f/new_materialization_31df04d81f.png",alt:"Add new Materialization"})}),"\n",(0,n.jsx)(t.p,{children:"Choose Snowflake and start filling out the connection details based on the values inside the script you executed in the\nprevious step. If you haven\u2019t changed anything, this is how the connector configuration should look like:"}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.img,{src:"https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//materialization_endpoint_configuration_0d540a12b5/materialization_endpoint_configuration_0d540a12b5.png",alt:"Configure Materialization endpoint"})}),"\n",(0,n.jsx)(t.p,{children:"You can grab your Snowflake host URL and account identifier by navigating to these two little buttons on the Snowflake\nUI."}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.img,{src:"https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//snowflake_account_id_af1cc78df8/snowflake_account_id_af1cc78df8.png",alt:"Grab your Snowflake account id"})}),"\n",(0,n.jsx)(t.p,{children:"After the connection details are in place, the next step is to link the capture we just created to Flow is able to see\ncollections we are loading data into from Postgres."}),"\n",(0,n.jsx)(t.p,{children:"You can achieve this by clicking on the \u201cSource from Capture\u201d button, and selecting the name of the capture from the\ntable."}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.img,{src:"https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//link_source_to_capture_b0d37a738f/link_source_to_capture_b0d37a738f.png",alt:"Link Capture"})}),"\n",(0,n.jsxs)(t.p,{children:["After pressing continue, you are met with a few configuration options, but for now, feel free to press ",(0,n.jsx)(t.strong,{children:"Next,"})," then *\n",(0,n.jsx)(t.em,{children:"Save and Publish"}),"* in the top right corner, the defaults will work perfectly fine for this tutorial."]}),"\n",(0,n.jsx)(t.p,{children:"A successful deployment will look something like this:"}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.img,{src:"https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//publish_successful_4e18642288/publish_successful_4e18642288.png",alt:"Successful Deployment screen"})}),"\n",(0,n.jsx)(t.p,{children:"And that\u2019s it, you\u2019ve successfully published a real-time CDC pipeline. Let\u2019s check out Snowflake to see how\nthe data looks."}),"\n",(0,n.jsx)(t.p,{children:(0,n.jsx)(t.img,{src:"https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//snowflake_verification_2eb047efec/snowflake_verification_2eb047efec.png",alt:"Results in Snowflake"})}),"\n",(0,n.jsx)(t.p,{children:"Looks like the data is arriving as expected, and the schema of the table is properly configured by the connector based\non the types of the original table in Postgres."}),"\n",(0,n.jsx)(t.p,{children:"To get a feel for how the data flow works; head over to the collection details page on the Flow web UI to see your\nchanges immediately. On the Snowflake end, they will be materialized after the next update."}),"\n",(0,n.jsxs)(t.h2,{id:"next-steps",children:["Next Steps",(0,n.jsx)("a",{id:"next-steps"})]}),"\n",(0,n.jsx)(t.p,{children:"That\u2019s it! You should have everything you need to know to create your own data pipeline for loading data into Snowflake!"}),"\n",(0,n.jsx)(t.p,{children:"Now try it out on your own PostgreSQL database or other sources."}),"\n",(0,n.jsxs)(t.p,{children:["If you want to learn more, make sure you read through the ",(0,n.jsx)(t.a,{href:"https://docs.estuary.dev/",children:"Estuary documentation"}),"."]}),"\n",(0,n.jsxs)(t.p,{children:["You\u2019ll find instructions on how to use other connectors ",(0,n.jsx)(t.a,{href:"https://docs.estuary.dev/",children:"here"}),". There are more\ntutorials ",(0,n.jsx)(t.a,{href:"https://docs.estuary.dev/guides/",children:"here"}),"."]}),"\n",(0,n.jsxs)(t.p,{children:["Also, don\u2019t forget to join\nthe ",(0,n.jsx)(t.a,{href:"https://estuary-dev.slack.com/ssb/redirect#/shared-invite/email",children:"Estuary Slack Community"}),"!"]})]})}function d(e={}){const{wrapper:t}={...(0,s.R)(),...e.components};return t?(0,n.jsx)(t,{...e,children:(0,n.jsx)(u,{...e})}):u(e)}},28453:(e,t,o)=>{o.d(t,{R:()=>r,x:()=>i});var n=o(96540);const s={},a=n.createContext(s);function r(e){const t=n.useContext(a);return n.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function i(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(s):e.components||s:r(e.components),n.createElement(a.Provider,{value:t},e.children)}}}]);