# Canals

<p align="center" float="left">
  <img alt="" src="https://raw.githubusercontent.com/deepset-ai/canals/main/images/canals-logo-light.png#gh-dark-mode-only"/>
  <img alt="" src="https://raw.githubusercontent.com/deepset-ai/canals/main/images/canals-logo-dark.png#gh-light-mode-only"/>
</p>

[![PyPI - Version](https://img.shields.io/pypi/v/canals.svg)](https://pypi.org/project/canals)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/canals.svg)](https://pypi.org/project/canals)

Canals is a **component orchestration engine**. Components are Python objects that can execute a task, like reading a file, performing calculations, or making API calls. Canals connects these objects together: it builds a graph of components and takes care of managing their execution order, making sure that each object receives the input it expects from the other components of the pipeline.

Canals powers version 2.0 of the [Haystack framework](https://github.com/deepset-ai/haystack).

## Installation

Running:

```console
pip install canals
```

gives you the bare minimum necessary to run Canals.

To be able to draw pipelines, please make sure you have either an internet connection (to reach the Mermaid graph renderer at `https://mermaid.ink`) or [graphviz](https://graphviz.org/download/) installed and then install Canals as:

### Mermaid
```console
pip install canals[mermaid]
```

### GraphViz
```console
sudo apt install graphviz  # You may need `graphviz-dev` too: sudo apt install graphviz-dev
pip install canals[graphviz]
```
