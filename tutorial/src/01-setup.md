# Chapter 1: Setup
In this first chapter, we will see how to setup a Rust toolchain from scratch, an IDE to develop Rust, and the Maelstrom testbench that we will use to evaluate your solutions to the various Distributed System challenges.

## Rust setup
### Install Rust toolchain
Rust is installed through `rustup`, an utility which manages your rust toolchain (like `nvm` for node for instance). Install it with: 
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

This repo builds on the latest stable version of Rust, `Rust 1.68`.
When installing rustup, it will prompt you to install the latest stable Rust version. If you already have rust installed, run `rustup upgrade` to get the latest version.

Note that as stated by the installer, you will need to export an env variable in your shell .rc file after the install process, and restart your shell to pick it up.

You can sanity check your installation by running `cargo --version`, more on cargo later.

You will also need to install the MacOS vendored LLVM compiler (it's possible you already have it, but it tends to disappear after a major MacOS upgrade): run `xcode-select --install`.


I also highly recommend to export these two env variables in your shell:
```sh
export CARGO_NET_GIT_FETCH_WITH_CLI=true  # fixes issues with git that tend to happen with ssh
export CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse  # makes git operations a lot faster, requires latest rust version
```

### Setting up IDE

I recommend VSCode for the easiest dev experience possible. We will configure it to leverage `rust-analyzer`, the official LSP server for Rust, `clippy` the official linter for Rust and `rustfmt` the official formatter for Rust.

Note that both `rustfmt` and `clippy` ship with the default Rust toolchain install so you don't have to do anything to get them.

To get `rust-analyzer`, we will install the VSCode integration called `rust-lang.rust-analyzer`. This extension will handle itself the process of downloading the binaries for `rust-analyzer` transparently.

I then recommend to alter your VSCode config (`Cmd+,`) and add the following keys:
```json
{
    "editor.formatOnSave": true,
    "rust-analyzer.checkOnSave.command": "clippy",
}
```

Here are some other additional extensions that will make your developer experience smoother:
- `serayuzgur.crates` Augmented functionalities to manage the crates used in your project
- `tamasfe.even-better-toml` LSP for the TOML language which is often used with Rust
- `usernamehw.errorlens` Inline errors in your code, making it easier to see them in context

### Optional: installing a nightly toolchain for nicer formatting
Some nice formatting options for your code are gated behind the nightly toolchain of Rust. In particular, the option to sort imports by type (std,3p,prj for instance). To get this, we can install the latest nightly toolchain using rustup (`rustup toolchain install nightly`). Then add the following in your vscode config file:
```json
{
    "rust-analyzer.rustfmt.extraArgs": [
        "+nightly"
    ],
}
```
This will force the usage of the formatter coming with the nightly toolchain (this does not mean your project will be compiled with the nightly toolchain though, only the formatter will use it).

Then, you can specify a `.rustfmt.toml` file at the root of your project with this content:
```toml
group_imports = "StdExternalCrate"
```

## Maelstrom setup
The challenges we will solve are built on top of a platform called [Maelstrom](https://github.com/jepsen-io/maelstrom). This platform lets you build out a "node" in your distributed system and Maelstrom will handle the routing of messages between the those nodes. This lets Maelstrom inject failures and perform verification checks based on the consistency guarantees required by each challenge. You can see it as a simulator and test bench for developing distributed systems.

Maelstrom is built in Clojure and therefore requires the Java JDK to run. It also provides some plotting and graphing utilities which rely on Graphviz & gnuplot. You can install all the required dependencies with this command:
```bash
brew install openjdk graphviz gnuplot
```
You will need to tell your system about this java version by running
```bash
sudo ln -sfn /opt/homebrew/opt/openjdk/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk
```

We are now ready to download Maelstrom binary, the following commands will install it under `~/maelstrom/maelstrom`:
```bash
curl -L https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2 > /tmp/maelstrom.tar.bz2
tar -C ~/ -xvf /tmp/maelstrom.tar.bz2
rm -rf /tmp/maelstrom.tar.bz2
```

Each problem will come with a set of Maelstrom commands to run it and test it so you don't have to worry on understanding all the parameters you can pass to the CLI.
