# flowstash-cli

CLI tool for the flowstash Managed Platform.

## Installation

```bash
pip install flowstash-cli
```

## Usage

```bash
# Login to your account
flowstash login

# Initialize a project in current directory
flowstash init

# Build and deploy
flowstash deploy
```

## Commands

- `flowstash login`: Authenticate with the platform.
- `flowstash init`: Initialize a `.flowstash.yaml` config.
- `flowstash build`: Bundle source and trigger a remote build.
- `flowstash deploy`: Deploy a build artifact to Cloud Run.
- `flowstash whoami`: Show current session info.
- `flowstash logout`: Clear local session.
