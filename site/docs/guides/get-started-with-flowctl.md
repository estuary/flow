# Getting Started With flowctl

After your account has been activated through the [web app](#get-started-with-the-flow-web-application), you can begin to work with your data flows from the command line.
This is not required, but it enables more advanced workflows or might simply be your preference.

Flow has a single binary, **flowctl**.

flowctl is available for:

- **Linux** x86-64. All distributions are supported.
- **MacOS** 11 (Big Sur) or later. Both Intel and M1 chips are supported.

To install, copy and paste the appropriate script below into your terminal. This will download flowctl, make it executable, and add it to your `PATH`.

- For Linux:

```console
sudo curl -o /usr/local/bin/flowctl -L 'https://github.com/estuary/flow/releases/latest/download/flowctl-x86_64-linux' && sudo chmod +x /usr/local/bin/flowctl
```

- For Mac:

```console
sudo curl -o /usr/local/bin/flowctl -L 'https://github.com/estuary/flow/releases/latest/download/flowctl-multiarch-macos' && sudo chmod +x /usr/local/bin/flowctl
```

Alternatively, Mac users can install with Homebrew:

```console
brew tap estuary/flowctl
brew install flowctl
```

flowctl isn't currently available for Windows.
For Windows users, we recommend running the Linux version inside [WSL](https://learn.microsoft.com/en-us/windows/wsl/),
or using a remote development environment.

The flowctl source files are also on GitHub [here](https://go.estuary.dev/flowctl).

Once you've installed flowctl and are ready to begin working, authenticate your session using an access token.

1. Ensure that you have an Estuary account and have signed into the Flow web app before.

2. In the terminal of your local development environment, run:

   ```console
   flowctl auth login
   ```

   In a browser window, the web app opens to the CLI-API tab.

3. Copy the access token.

4. Return to the terminal, paste the access token, and press Enter.

The token will expire after a predetermined duration. Repeat this process to re-authenticate.

[Learn more about using flowctl.](../concepts/flowctl.md)