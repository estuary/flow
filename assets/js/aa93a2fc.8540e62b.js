"use strict";(self.webpackChunksite=self.webpackChunksite||[]).push([[5853],{54884:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>c,contentTitle:()=>r,default:()=>h,frontMatter:()=>o,metadata:()=>i,toc:()=>a});var s=n(74848),l=n(28453);const o={},r="Getting Started With flowctl",i={id:"guides/get-started-with-flowctl",title:"Getting Started With flowctl",description:"After your account has been activated through the web app, you can begin to work with your data flows from the command line.",source:"@site/docs/guides/get-started-with-flowctl.md",sourceDirName:"guides",slug:"/guides/get-started-with-flowctl",permalink:"/guides/get-started-with-flowctl",draft:!1,unlisted:!1,editUrl:"https://github.com/estuary/flow/edit/master/site/docs/guides/get-started-with-flowctl.md",tags:[],version:"current",frontMatter:{},sidebar:"tutorialSidebar",previous:{title:"How to transform data using SQL",permalink:"/guides/derivation_tutorial_sql"},next:{title:"How to generate an Estuary Flow Refresh Token",permalink:"/guides/how_to_generate_refresh_token"}},c={},a=[];function d(e){const t={a:"a",code:"code",h1:"h1",li:"li",ol:"ol",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,l.R)(),...e.components};return(0,s.jsxs)(s.Fragment,{children:[(0,s.jsx)(t.h1,{id:"getting-started-with-flowctl",children:"Getting Started With flowctl"}),"\n",(0,s.jsxs)(t.p,{children:["After your account has been activated through the ",(0,s.jsx)(t.a,{href:"#get-started-with-the-flow-web-application",children:"web app"}),", you can begin to work with your data flows from the command line.\nThis is not required, but it enables more advanced workflows or might simply be your preference."]}),"\n",(0,s.jsxs)(t.p,{children:["Flow has a single binary, ",(0,s.jsx)(t.strong,{children:"flowctl"}),"."]}),"\n",(0,s.jsx)(t.p,{children:"flowctl is available for:"}),"\n",(0,s.jsxs)(t.ul,{children:["\n",(0,s.jsxs)(t.li,{children:[(0,s.jsx)(t.strong,{children:"Linux"})," x86-64. All distributions are supported."]}),"\n",(0,s.jsxs)(t.li,{children:[(0,s.jsx)(t.strong,{children:"MacOS"})," 11 (Big Sur) or later. Both Intel and M1 chips are supported."]}),"\n"]}),"\n",(0,s.jsxs)(t.p,{children:["To install, copy and paste the appropriate script below into your terminal. This will download flowctl, make it executable, and add it to your ",(0,s.jsx)(t.code,{children:"PATH"}),"."]}),"\n",(0,s.jsxs)(t.ul,{children:["\n",(0,s.jsx)(t.li,{children:"For Linux:"}),"\n"]}),"\n",(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{className:"language-console",children:"sudo curl -o /usr/local/bin/flowctl -L 'https://github.com/estuary/flow/releases/latest/download/flowctl-x86_64-linux' && sudo chmod +x /usr/local/bin/flowctl\n"})}),"\n",(0,s.jsxs)(t.ul,{children:["\n",(0,s.jsx)(t.li,{children:"For Mac:"}),"\n"]}),"\n",(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{className:"language-console",children:"sudo curl -o /usr/local/bin/flowctl -L 'https://github.com/estuary/flow/releases/latest/download/flowctl-multiarch-macos' && sudo chmod +x /usr/local/bin/flowctl\n"})}),"\n",(0,s.jsx)(t.p,{children:"Alternatively, Mac users can install with Homebrew:"}),"\n",(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{className:"language-console",children:"brew tap estuary/flowctl\nbrew install flowctl\n"})}),"\n",(0,s.jsxs)(t.p,{children:["flowctl isn't currently available for Windows.\nFor Windows users, we recommend running the Linux version inside ",(0,s.jsx)(t.a,{href:"https://learn.microsoft.com/en-us/windows/wsl/",children:"WSL"}),",\nor using a remote development environment."]}),"\n",(0,s.jsxs)(t.p,{children:["The flowctl source files are also on GitHub ",(0,s.jsx)(t.a,{href:"https://go.estuary.dev/flowctl",children:"here"}),"."]}),"\n",(0,s.jsx)(t.p,{children:"Once you've installed flowctl and are ready to begin working, authenticate your session using an access token."}),"\n",(0,s.jsxs)(t.ol,{children:["\n",(0,s.jsxs)(t.li,{children:["\n",(0,s.jsx)(t.p,{children:"Ensure that you have an Estuary account and have signed into the Flow web app before."}),"\n"]}),"\n",(0,s.jsxs)(t.li,{children:["\n",(0,s.jsx)(t.p,{children:"In the terminal of your local development environment, run:"}),"\n",(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{className:"language-console",children:"flowctl auth login\n"})}),"\n",(0,s.jsx)(t.p,{children:"In a browser window, the web app opens to the CLI-API tab."}),"\n"]}),"\n",(0,s.jsxs)(t.li,{children:["\n",(0,s.jsx)(t.p,{children:"Copy the access token."}),"\n"]}),"\n",(0,s.jsxs)(t.li,{children:["\n",(0,s.jsx)(t.p,{children:"Return to the terminal, paste the access token, and press Enter."}),"\n"]}),"\n"]}),"\n",(0,s.jsx)(t.p,{children:"The token will expire after a predetermined duration. Repeat this process to re-authenticate."}),"\n",(0,s.jsx)(t.h1,{id:"next-steps",children:"Next steps"}),"\n",(0,s.jsxs)(t.ol,{children:["\n",(0,s.jsxs)(t.li,{children:[(0,s.jsx)(t.a,{href:"/concepts/flowctl",children:"flowctl concepts"}),": Learn more about using flowctl."]}),"\n",(0,s.jsxs)(t.li,{children:[(0,s.jsx)(t.a,{href:"/guides/flowctl/",children:"User guides"}),": Check out some of the detailed user guides to see flowctl in action."]}),"\n"]})]})}function h(e={}){const{wrapper:t}={...(0,l.R)(),...e.components};return t?(0,s.jsx)(t,{...e,children:(0,s.jsx)(d,{...e})}):d(e)}},28453:(e,t,n)=>{n.d(t,{R:()=>r,x:()=>i});var s=n(96540);const l={},o=s.createContext(l);function r(e){const t=s.useContext(o);return s.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function i(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(l):e.components||l:r(e.components),s.createElement(o.Provider,{value:t},e.children)}}}]);