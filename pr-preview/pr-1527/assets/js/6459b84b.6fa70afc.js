"use strict";(self.webpackChunksite=self.webpackChunksite||[]).push([[6459],{24519:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>u,contentTitle:()=>d,default:()=>x,frontMatter:()=>c,metadata:()=>h,toc:()=>p});var s=t(74848),o=t(28453),r=t(96540);const i=()=>{const e=window.location.href,[n,t]=r.useState("");return r.useMemo((()=>new URLSearchParams(window.location.search.slice(1)).get("code")),[])?(0,s.jsx)("span",{style:{color:"green"},children:"You have successfully added the application to your tenant"}):(0,s.jsxs)(s.Fragment,{children:[(0,s.jsxs)("span",{children:["Input your ",(0,s.jsx)("b",{children:"Tenant ID"})," and follow the prompts to add our application to your tenant:"]}),(0,s.jsx)("br",{}),(0,s.jsx)("br",{}),(0,s.jsx)("input",{placeholder:"Tenant ID",value:n,onChange:e=>t(e.target.value)}),(0,s.jsx)("a",{style:{marginLeft:8,color:n.length<1?"inherit":void 0},href:n.length>0?(o=n,`https://login.microsoftonline.com/${o}/oauth2/authorize?client_id=42cb0c6c-dab0-411f-9c21-16d5a2b1b025&response_type=code&redirect_uri=${encodeURIComponent(e)}&resource_id=${encodeURIComponent("https://storage.azure.com")}`):null,children:"Authorize"})]});var o};var l=t(92303);function a(e){let{children:n,fallback:t}=e;return(0,l.A)()?(0,s.jsx)(s.Fragment,{children:n?.()}):t??null}const c={sidebar_position:1},d="Registration and setup",h={id:"getting-started/installation",title:"Registration and setup",description:"Estuary Flow is a fully managed web application that also offers a robust CLI.",source:"@site/docs/getting-started/installation.mdx",sourceDirName:"getting-started",slug:"/getting-started/installation",permalink:"/pr-preview/pr-1527/getting-started/installation",draft:!1,unlisted:!1,editUrl:"https://github.com/estuary/flow/edit/master/site/docs/getting-started/installation.mdx",tags:[],version:"current",sidebarPosition:1,frontMatter:{sidebar_position:1},sidebar:"tutorialSidebar",previous:{title:"Comparisons",permalink:"/pr-preview/pr-1527/overview/comparisons"},next:{title:"Flow tutorials",permalink:"/pr-preview/pr-1527/getting-started/tutorials/"}},u={},p=[{value:"Get started with the Flow web application",id:"get-started-with-the-flow-web-application",level:2},{value:"Get started with the Flow CLI",id:"get-started-with-the-flow-cli",level:2},{value:"Configuring your cloud storage bucket for use with Flow",id:"configuring-your-cloud-storage-bucket-for-use-with-flow",level:2},{value:"Google Cloud Storage buckets",id:"google-cloud-storage-buckets",level:3},{value:"Amazon S3 buckets",id:"amazon-s3-buckets",level:3},{value:"Azure Blob Storage",id:"azure-blob-storage",level:3},{value:"Give us a ring",id:"give-us-a-ring",level:3},{value:"Self-hosting Flow",id:"self-hosting-flow",level:2},{value:"What&#39;s next?",id:"whats-next",level:2}];function g(e){const n={a:"a",admonition:"admonition",code:"code",h1:"h1",h2:"h2",h3:"h3",img:"img",li:"li",ol:"ol",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,o.R)(),...e.components};return(0,s.jsxs)(s.Fragment,{children:[(0,s.jsx)(n.h1,{id:"registration-and-setup",children:"Registration and setup"}),"\n",(0,s.jsx)(n.p,{children:"Estuary Flow is a fully managed web application that also offers a robust CLI.\nOnce you register in the web application, you can seamlessly use either or both tools to do your work."}),"\n",(0,s.jsx)(n.h2,{id:"get-started-with-the-flow-web-application",children:"Get started with the Flow web application"}),"\n",(0,s.jsxs)(n.p,{children:["To start using Flow for free, visit the ",(0,s.jsx)(n.a,{href:"https://go.estuary.dev/dashboard",children:"web application"}),".\nSee what the free tier includes on the ",(0,s.jsx)(n.a,{href:"https://estuary.dev/pricing/",children:"Pricing"})," page."]}),"\n",(0,s.jsx)(n.p,{children:"When you register for Flow, your account will use Flow's secure cloud storage bucket to store your data.\nData in Flow's cloud storage bucket is deleted 30 days after collection."}),"\n",(0,s.jsxs)(n.p,{children:["For production use cases, you should ",(0,s.jsx)(n.a,{href:"#configuring-your-cloud-storage-bucket-for-use-with-flow",children:"configure your own cloud storage bucket to use with Flow"}),"."]}),"\n",(0,s.jsx)(n.h2,{id:"get-started-with-the-flow-cli",children:"Get started with the Flow CLI"}),"\n",(0,s.jsxs)(n.p,{children:["After your account has been activated through the ",(0,s.jsx)(n.a,{href:"#get-started-with-the-flow-web-application",children:"web app"}),", you can begin to work with your data flows from the command line.\nThis is not required, but it enables more advanced workflows or might simply be your preference."]}),"\n",(0,s.jsxs)(n.p,{children:["Flow has a single binary, ",(0,s.jsx)(n.strong,{children:"flowctl"}),"."]}),"\n",(0,s.jsx)(n.p,{children:"flowctl is available for:"}),"\n",(0,s.jsxs)(n.ul,{children:["\n",(0,s.jsxs)(n.li,{children:[(0,s.jsx)(n.strong,{children:"Linux"})," x86-64. All distributions are supported."]}),"\n",(0,s.jsxs)(n.li,{children:[(0,s.jsx)(n.strong,{children:"MacOS"})," 11 (Big Sur) or later. Both Intel and M1 chips are supported."]}),"\n"]}),"\n",(0,s.jsxs)(n.p,{children:["To install, copy and paste the appropriate script below into your terminal. This will download flowctl, make it executable, and add it to your ",(0,s.jsx)(n.code,{children:"PATH"}),"."]}),"\n",(0,s.jsxs)(n.ul,{children:["\n",(0,s.jsx)(n.li,{children:"For Linux:"}),"\n"]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-console",children:"sudo curl -o /usr/local/bin/flowctl -L 'https://github.com/estuary/flow/releases/latest/download/flowctl-x86_64-linux' && sudo chmod +x /usr/local/bin/flowctl\n"})}),"\n",(0,s.jsxs)(n.ul,{children:["\n",(0,s.jsx)(n.li,{children:"For Mac:"}),"\n"]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-console",children:"sudo curl -o /usr/local/bin/flowctl -L 'https://github.com/estuary/flow/releases/latest/download/flowctl-multiarch-macos' && sudo chmod +x /usr/local/bin/flowctl\n"})}),"\n",(0,s.jsx)(n.p,{children:"Alternatively, Mac users can install with Homebrew:"}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-console",children:"brew tap estuary/flowctl\nbrew install flowctl\n"})}),"\n",(0,s.jsxs)(n.p,{children:["flowctl isn't currently available for Windows.\nFor Windows users, we recommend running the Linux version inside ",(0,s.jsx)(n.a,{href:"https://learn.microsoft.com/en-us/windows/wsl/",children:"WSL"}),",\nor using a remote development environment."]}),"\n",(0,s.jsxs)(n.p,{children:["The flowctl source files are also on GitHub ",(0,s.jsx)(n.a,{href:"https://go.estuary.dev/flowctl",children:"here"}),"."]}),"\n",(0,s.jsx)(n.p,{children:"Once you've installed flowctl and are ready to begin working, authenticate your session using an access token."}),"\n",(0,s.jsxs)(n.ol,{children:["\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsx)(n.p,{children:"Ensure that you have an Estuary account and have signed into the Flow web app before."}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsx)(n.p,{children:"In the terminal of your local development environment, run:"}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-console",children:"flowctl auth login\n"})}),"\n",(0,s.jsx)(n.p,{children:"In a browser window, the web app opens to the CLI-API tab."}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsx)(n.p,{children:"Copy the access token."}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsx)(n.p,{children:"Return to the terminal, paste the access token, and press Enter."}),"\n"]}),"\n"]}),"\n",(0,s.jsx)(n.p,{children:"The token will expire after a predetermined duration. Repeat this process to re-authenticate."}),"\n",(0,s.jsx)(n.p,{children:(0,s.jsx)(n.a,{href:"/pr-preview/pr-1527/concepts/flowctl",children:"Learn more about using flowctl."})}),"\n",(0,s.jsx)(n.h2,{id:"configuring-your-cloud-storage-bucket-for-use-with-flow",children:"Configuring your cloud storage bucket for use with Flow"}),"\n",(0,s.jsx)(n.p,{children:"New Flow accounts are connected to Flow's secure cloud storage bucket to store collection data.\nTo switch to your own bucket, choose a cloud provider and complete the setup steps:"}),"\n",(0,s.jsxs)(n.ul,{children:["\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsx)(n.p,{children:(0,s.jsx)(n.a,{href:"#google-cloud-storage-buckets",children:"Google Cloud Storage"})}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsx)(n.p,{children:(0,s.jsx)(n.a,{href:"#amazon-s3-buckets",children:"Amazon S3"})}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsx)(n.p,{children:(0,s.jsx)(n.a,{href:"#azure-blob-storage",children:"Azure Blob Storage"})}),"\n"]}),"\n"]}),"\n",(0,s.jsxs)(n.p,{children:["Once you're done, ",(0,s.jsx)(n.a,{href:"#give-us-a-ring",children:"get in touch"}),"."]}),"\n",(0,s.jsx)(n.h3,{id:"google-cloud-storage-buckets",children:"Google Cloud Storage buckets"}),"\n",(0,s.jsx)(n.p,{children:"You'll need to grant Estuary Flow access to your GCS bucket."}),"\n",(0,s.jsxs)(n.ol,{children:["\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:[(0,s.jsx)(n.a,{href:"https://cloud.google.com/storage/docs/creating-buckets",children:"Create a bucket to use with Flow"}),", if you haven't already."]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["Follow the steps to ",(0,s.jsx)(n.a,{href:"https://cloud.google.com/storage/docs/access-control/using-iam-permissions#bucket-add",children:"add a principal to a bucket level policy"}),".\nAs you do so:"]}),"\n",(0,s.jsxs)(n.ul,{children:["\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["For the principal, enter ",(0,s.jsx)(n.code,{children:"flow-258@helpful-kingdom-273219.iam.gserviceaccount.com"})]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["Select the ",(0,s.jsx)(n.a,{href:"https://cloud.google.com/storage/docs/access-control/iam-roles",children:(0,s.jsx)(n.code,{children:"roles/storage.admin"})})," role."]}),"\n"]}),"\n"]}),"\n"]}),"\n"]}),"\n",(0,s.jsx)(n.h3,{id:"amazon-s3-buckets",children:"Amazon S3 buckets"}),"\n",(0,s.jsxs)(n.p,{children:["Your bucket must be in the us-east-1 ",(0,s.jsx)(n.a,{href:"https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html",children:"region"}),".\nYou'll need to grant Estuary Flow access to your S3 bucket."]}),"\n",(0,s.jsxs)(n.ol,{children:["\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:[(0,s.jsx)(n.a,{href:"https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html",children:"Create a bucket to use with Flow"}),", if you haven't already."]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["Follow the steps to ",(0,s.jsx)(n.a,{href:"https://docs.aws.amazon.com/AmazonS3/latest/userguide/add-bucket-policy.html",children:"add a bucket policy"}),", pasting the policy below.\nBe sure to replace ",(0,s.jsx)(n.code,{children:"YOUR-S3-BUCKET"})," with the actual name of your bucket."]}),"\n"]}),"\n"]}),"\n",(0,s.jsx)(n.pre,{children:(0,s.jsx)(n.code,{className:"language-json",children:'{\n  "Version": "2012-10-17",\n  "Statement": [\n    {\n      "Sid": "AllowUsersToAccessObjectsUnderPrefix",\n      "Effect": "Allow",\n      "Principal": {\n        "AWS": "arn:aws:iam::789740162118:user/flow-aws"\n      },\n      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],\n      "Resource": "arn:aws:s3:::YOUR-S3-BUCKET/*"\n    },\n    {\n      "Effect": "Allow",\n      "Principal": {\n        "AWS": "arn:aws:iam::789740162118:user/flow-aws"\n      },\n      "Action": "s3:ListBucket",\n      "Resource": "arn:aws:s3:::YOUR-S3-BUCKET"\n    },\n    {\n      "Effect": "Allow",\n      "Principal": {\n        "AWS": "arn:aws:iam::789740162118:user/flow-aws"\n      },\n      "Action": "s3:GetBucketPolicy",\n      "Resource": "arn:aws:s3:::YOUR-S3-BUCKET"\n    }\n  ]\n}\n'})}),"\n",(0,s.jsx)(n.h3,{id:"azure-blob-storage",children:"Azure Blob Storage"}),"\n",(0,s.jsx)(n.p,{children:"You'll need to grant Estuary Flow access to your storage account and container.\nYou'll also need to provide some identifying information."}),"\n",(0,s.jsxs)(n.ol,{children:["\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:[(0,s.jsx)(n.a,{href:"https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container",children:"Create an Azure Blob Storage container"})," to use with Flow, if you haven't already."]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsx)(n.p,{children:"Gather the following information. You'll need this when you contact us to complete setup."}),"\n",(0,s.jsxs)(n.ul,{children:["\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["Your ",(0,s.jsx)(n.strong,{children:"Azure AD tenant ID"}),". You can find this in the ",(0,s.jsx)(n.strong,{children:"Azure Active Directory"})," page.\n",(0,s.jsx)(n.img,{src:t(63997).A+"",width:"1147",height:"507"})]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["Your ",(0,s.jsx)(n.strong,{children:"Azure Blob Storage account ID"}),". You can find this in the ",(0,s.jsx)(n.strong,{children:"Storage Accounts"})," page.\n",(0,s.jsx)(n.img,{src:t(33024).A+"",width:"852",height:"317"})]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["Your ",(0,s.jsx)(n.strong,{children:"Azure Blob Storage container ID"}),". You can find this inside your storage account.\n",(0,s.jsx)(n.img,{src:t(74200).A+"",width:"997",height:"599"})]}),"\n"]}),"\n"]}),"\n",(0,s.jsxs)(n.p,{children:["You'll grant Flow access to your storage resources by connecting to Estuary's\n",(0,s.jsx)(n.a,{href:"https://learn.microsoft.com/en-us/azure/active-directory/manage-apps/what-is-application-management",children:"Azure application"}),"."]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsx)(n.p,{children:"Add Estuary's Azure application to your tenant."}),"\n"]}),"\n"]}),"\n","\n",(0,s.jsx)(a,{children:()=>(0,s.jsx)(i,{})}),"\n",(0,s.jsxs)(n.ol,{start:"4",children:["\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["Grant the application access to your storage account via the\n",(0,s.jsx)(n.a,{href:"https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-owner",children:(0,s.jsx)(n.code,{children:"Storage Blob Data Owner"})})," IAM role."]}),"\n",(0,s.jsxs)(n.ul,{children:["\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["Inside your storage account's ",(0,s.jsx)(n.strong,{children:"Access Control (IAM)"})," tab, click ",(0,s.jsx)(n.strong,{children:"Add Role Assignment"}),"."]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["Search for ",(0,s.jsx)(n.code,{children:"Storage Blob Data Owner"})," and select it."]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["On the next page, make sure ",(0,s.jsx)(n.code,{children:"User, group, or service principal"})," is selected, then click ",(0,s.jsx)(n.strong,{children:"+ Select Members"}),"."]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:["You must search for the exact name of the application, otherwise it won't show up: ",(0,s.jsx)(n.code,{children:"Estuary Storage Mappings Prod"})]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsx)(n.p,{children:"Once you've selected the application, finish granting the role."}),"\n"]}),"\n"]}),"\n",(0,s.jsxs)(n.p,{children:["For more help, see the ",(0,s.jsx)(n.a,{href:"https://learn.microsoft.com/en-us/azure/role-based-access-control/role-assignments-portal",children:"Azure docs"}),"."]}),"\n"]}),"\n"]}),"\n",(0,s.jsx)(n.h3,{id:"give-us-a-ring",children:"Give us a ring"}),"\n",(0,s.jsxs)(n.p,{children:["Once you've finished the above steps, contact us.\nSend ",(0,s.jsx)(n.a,{href:"mailto:support@estuary.dev",children:"support@estuary.dev"})," an email with the name of the storage bucket and any other information you gathered per the steps above.\nLet us know whether you want to use this storage bucket to for your whole Flow account, or just a specific ",(0,s.jsx)(n.a,{href:"/pr-preview/pr-1527/concepts/catalogs#namespace",children:"prefix"}),".\nWe'll be in touch when it's done!"]}),"\n",(0,s.jsx)(n.h2,{id:"self-hosting-flow",children:"Self-hosting Flow"}),"\n",(0,s.jsxs)(n.p,{children:["The Flow runtime is available under the ",(0,s.jsx)(n.a,{href:"https://github.com/estuary/flow/blob/master/LICENSE-BSL",children:"Business Source License"}),". It's possible to self-host Flow using a cloud provider of your choice."]}),"\n",(0,s.jsx)(n.admonition,{title:"Beta",type:"caution",children:(0,s.jsxs)(n.p,{children:["Setup for self-hosting is not covered in this documentation, and full support is not guaranteed at this time.\nWe recommend using the ",(0,s.jsx)(n.a,{href:"#get-started-with-the-flow-web-application",children:"hosted version of Flow"})," for the best experience.\nIf you'd still like to self-host, refer to the ",(0,s.jsx)(n.a,{href:"https://github.com/estuary/flow",children:"GitHub repository"})," or the ",(0,s.jsx)(n.a,{href:"https://join.slack.com/t/estuary-dev/shared_invite/zt-86nal6yr-VPbv~YfZE9Q~6Zl~gmZdFQ",children:"Estuary Slack"}),"."]})}),"\n",(0,s.jsx)(n.h2,{id:"whats-next",children:"What's next?"}),"\n",(0,s.jsx)(n.p,{children:"Start using Flow with these recommended resources."}),"\n",(0,s.jsxs)(n.ul,{children:["\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:[(0,s.jsx)(n.strong,{children:(0,s.jsx)(n.a,{href:"/pr-preview/pr-1527/guides/create-dataflow",children:"Create your first data flow"})}),":\nFollow this guide to create your first data flow in the Flow web app, while learning essential flow concepts."]}),"\n"]}),"\n",(0,s.jsxs)(n.li,{children:["\n",(0,s.jsxs)(n.p,{children:[(0,s.jsx)(n.strong,{children:(0,s.jsx)(n.a,{href:"/pr-preview/pr-1527/concepts/",children:"High level concepts"})}),": Start here to learn more about important Flow terms."]}),"\n"]}),"\n"]})]})}function x(e={}){const{wrapper:n}={...(0,o.R)(),...e.components};return n?(0,s.jsx)(n,{...e,children:(0,s.jsx)(g,{...e})}):g(e)}},63997:(e,n,t)=>{t.d(n,{A:()=>s});const s=t.p+"assets/images/Azure_AD_Tenant_ID-755966905d34f909de14009536374173.png"},74200:(e,n,t)=>{t.d(n,{A:()=>s});const s=t.p+"assets/images/Azure_Container_ID-de1a2aac02163282fc8694374ac22c27.png"},33024:(e,n,t)=>{t.d(n,{A:()=>s});const s=t.p+"assets/images/Azure_Storage_Account_Name-0f3730e71af4f785f38be9df59f63d5e.png"},28453:(e,n,t)=>{t.d(n,{R:()=>i,x:()=>l});var s=t(96540);const o={},r=s.createContext(o);function i(e){const n=s.useContext(r);return s.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function l(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(o):e.components||o:i(e.components),s.createElement(r.Provider,{value:n},e.children)}}}]);