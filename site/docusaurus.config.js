// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

import { themes } from 'prism-react-renderer';
import { codeImport } from 'remark-code-import'

const lightCodeTheme = themes.github;
const darkCodeTheme = themes.dracula;

const BASE_URL = process.env.BASE_URL || "/"
const URL = process.env.URL || "https://docs.estuary.dev"

console.log(`Building for: ${URL}${BASE_URL}`)

// Sort docs & folders on the same level for connector pages.
/**
 * @param {any[]} items
 */
function sortSidebarAlphabetically(items) {
  const result = items.map((item) => {
    if (item.type === 'category') {
      if (item.label == 'Capture connectors' || item.label == 'Materialization connectors') {
        item.items.forEach((i) => {
          if (i.type === 'category') {
            i.sortkey = i.label;
          } else {
            // id: 'reference/Connectors/materialization-connectors/timescaledb' -> timescaledb
            i.sortkey = i.link ? i.link.id.split('/').pop() : i.id.split('/').pop();
          }
        });
        item.items.sort((a, b) => a.sortkey.localeCompare(b.sortkey));
        item.items.forEach((i) => {
          delete i.sortkey;
        });
      }
      return { ...item, items: sortSidebarAlphabetically(item.items) };
    }
    return item;
  });
  return result;
}

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Estuary Flow Documentation',
  tagline: 'Dinosaurs are cool',
  url: URL,
  baseUrl: BASE_URL,
  onBrokenAnchors: 'warn', // TODO(johnny): Fix broken links and make this 'throw'.
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'throw',
  favicon: 'img/favicon-2.ico',
  organizationName: 'estuary',
  projectName: 'flow',
  trailingSlash: true,

  scripts: [
    {
      id: "runllm-widget-script",
      type: "module",
      src: "https://widget.runllm.com",
      "runllm-name": "Estuary AI Assistant",
      "runllm-assistant-id": "253",
      "runllm-position": "BOTTOM_RIGHT",
      "runllm-keyboard-shortcut": "Mod+j",
      "runllm-preset": "docusaurus",
      async: true,
      "runllm-theme-color": "#5072EB",
      "runllm-brand-logo": "https://storage.googleapis.com/estuary-marketing-strapi-uploads/uploads//estuary_logo_a373037876/estuary_logo_a373037876.png",
      "runllm-support-email": "support@estuary.dev",
      "runllm-community-url": "https://go.estuary.dev/slack",
      "runllm-community-type": "slack",
    },
  ],

  plugins: [
    [
      require.resolve('docusaurus-lunr-search'),
      {
        // @ts-ignore
        excludeRoutes: [
          'blog/**/*',
        ]
      },],
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          {
            to: '/guides/flowctl/create-derivation/',
            from: '/guides/create-derivation/',
          },
        ],
      },
    ],
    [
      '@docusaurus/plugin-google-tag-manager',
      {
        containerId: 'GTM-WK8SB2L',
      },
    ],
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/estuary/flow/edit/master/site/',
          routeBasePath: '/',
          remarkPlugins: [codeImport],
          async sidebarItemsGenerator({ defaultSidebarItemsGenerator, ...args }) {
            const sidebarItems = await defaultSidebarItemsGenerator(args);
            return sortSidebarAlphabetically(sidebarItems);
          },
        },
        /*
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          editUrl: 'https://github.com/estuary/flow/edit/master/site/blog',
        },
        */
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/estuary-new.png',
      navbar: {
        title: 'Estuary Flow',
        logo: {
          alt: 'Estuary Flow Logo',
          src: 'img/estuary-new.png',
        },
        items: [
          {
            type: 'doc',
            docId: 'getting-started/getting-started',
            position: 'left',
            label: 'Documentation',
          },
          {
            type: 'html',
            position: 'left',
            value: '<a href="https://estuary.dev/blog">Blog</a>',
          },
          {
            type: 'html',
            position: 'left',
            className: 'button-link',
            value: '<a href="https://dashboard.estuary.dev/register">Try Estuary</a>',
          },
          /*
          {to: '/blog', label: 'Blog', position: 'left'},
          */
          {
            href: 'https://github.com/estuary/flow',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Flow Documentation',
                to: '/',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Slack',
                href: 'https://go.estuary.dev/slack'
              },
              {
                label: 'Twitter',
                href: 'https://twitter.com/EstuaryDev',
              },
              /*
              {
                label: 'Stack Overflow',
                href: 'https://stackoverflow.com/questions/tagged/docusaurus',
              },
              {
                label: 'Discord',
                href: 'https://discordapp.com/invite/docusaurus',
              },
              */
            ],
          },
          {
            title: 'More',
            items: [
              /*
              {
                label: 'Blog',
                to: '/blog',
              },
              */
              {
                label: 'GitHub',
                href: 'https://github.com/estuary/flow',
              },
              {
                label: 'YouTube',
                href: 'https://www.youtube.com/@estuarydev'
              }
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Estuary Technologies, Inc. Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
