# Restaurant seed

`restaurants.jsonl.gz` is the normalised restaurant seed that the generator builds its
synthetic world from. It holds 8,673 restaurants across 9 Indian cities, and
`restaurants.meta.json` records its SHA-256. The seed is committed so that scheduled runs,
CI and new clones need no Kaggle account. `fd seed` (the default source `committed`) unpacks
it and checks that checksum.

**Source:** the [Swiggy Restaurants dataset](https://www.kaggle.com/datasets/abhijitdahatonde/swiggy-restuarant-dataset)
by Abhijit Dahatonde on Kaggle, published under
[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) (public domain). Attribution is
not legally required, but it is given here anyway. The file was produced by `load_swiggy_csv`
in `src/fooddelivery/generator/seed.py`, which keeps only the columns the generator uses and
normalises them.

To rebuild it from Kaggle (needs a Kaggle API token):

    FD_SEED_SOURCE=kaggle uv run --extra seed fd seed --force

Changing the seed changes every generated record, so don't do this against a workspace that
already holds data generated from the current seed.
