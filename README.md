This repo aims at benchmarking different parallelization strategies and configuration for the [mapme.biodiversity](https://github.com/mapme-initiative/mapme.biodiversity) package, version 0.3.0.

It was tested on SSP Cloud platforms, with 2 different containers:   

- R + RStudio; and
- R + python + julia + VSCode.

The tests on R + RStudio were run with poc_alternative.R (which doesn't include multicore strategies, as advised for the future package on RStudio platforms.
The tests on R + VSCode aimed at including multicore but produced surprising results (no gains from paralellization).

Below are the results.

