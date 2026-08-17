# Website

_Full guidelines for writing and deploying Estuary docs can be found on [Google Drive](https://docs.google.com/document/d/1SRC9VS9zyCzWl3n4HXHbc4wPB1eLxJHkA2rtu9ZNokM/edit#)._

This website is built using [Docusaurus 2](https://docusaurus.io/), a modern static website generator.

### Installation

```
$ npm install
```

### Local Development

```
$ npm start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

### Build

```
$ npm build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

**macOS note:** `npm run build` fails locally on macOS's default case-insensitive
filesystem with `The redirect plugin is not supposed to override existing files`
for the `hubspot-real-time` redirect in `docusaurus.config.js`. That redirect's
`from` path differs from the real `HubSpot-real-time` page only by case, so the
plugin sees a collision when writing the redirect stub. This does not affect
production: CI builds on Linux (case-sensitive), so the two paths are genuinely
distinct there. To build locally on macOS, temporarily comment out that redirect
entry, or build inside a case-sensitive volume/Docker.

### Deployment

Using SSH:

```
$ USE_SSH=true npm deploy
```

Not using SSH:

```
$ GIT_USER=<Your GitHub username> npm deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.
