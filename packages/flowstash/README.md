# flowstash

`flowstash` is a convenience meta-package for the [flowstash managed platform](https://github.com/TODO/flowstash). 

By installing this package, you will automatically install both:
- `flowstash-cli`: The Command Line Interface for initialization and deployment.
- `flowstash-runtime`: The actual runtime engine and worker layer.

If you only need the CLI, or only the internals (libraries/clients), you can install those packages individually (e.g. `pip install flowstash-cli`).

## Installation

```bash
pip install flowstash
```

## Usage

Once installed, the `flowstash` command will be available through `flowstash-cli`:

```bash
flowstash --help
```

For full documentation and details on how this fits into the larger ecosystem, please refer to the [flowstash Monorepo Root](https://github.com/TODO/flowstash).
