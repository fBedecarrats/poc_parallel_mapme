This repo aims at benchmarking different parallelization strategies and configuration for the [mapme.biodiversity](https://github.com/mapme-initiative/mapme.biodiversity) package, version 0.3.0.

It was tested on SSP Cloud platforms, with 2 different containers:   

- R + RStudio; and
- R + python + julia + VSCode.

The tests on R + RStudio were run with poc_alternative.R (which doesn't include multicore strategies, as advised for the future package on RStudio platforms.
The tests on R + VSCode aimed at including multicore but produced surprising results (no gains from paralellization).

First results w. RStudio   

![histogram with results on rsudio](https://user-images.githubusercontent.com/3328347/222668887-707a3413-2880-4fd7-b1f2-81c7f1c058d2.png)

First results w. VSCode   

![histogram with results on vscode](https://raw.githubusercontent.com/fBedecarrats/poc_parallel_mapme/main/Results%20VSCode.png)
