# ADR 0006: Kaggle seed (CC0) plus a synthetic generator

**Status:** accepted

## Decision
- The seed is **`abhijitdahatonde/swiggy-restuarant-dataset`** (`swiggy.csv`, about 8.7k restaurants in 9 Indian
  cities, **CC0-1.0**). It provides restaurant names, neighbourhoods, cuisines, price-for-two, rating, rating
  count (popularity) and listed delivery time (which shapes prep time).
- Other candidates were rejected on licence grounds. `gauravmalik26/food-delivery-dataset` and the Zomato
  delivery-ops dataset are licensed "other"; `zomato-bangalore-restaurants` is "copyright-authors".
- Raw Kaggle data is never committed. Tests use a synthetic seed with the same shape
  (`FD_SEED_SOURCE=synthetic` also lets anyone run without a Kaggle account).
- Geography is synthesised: each neighbourhood gets a stable point within the city's disc around public
  city-centre coordinates, since the dataset has no coordinates.
- Orders, events, riders, GPS, payments, refunds and ratings are fully synthetic. They are generated from
  documented behavioural models: lunch/dinner peaks, weekday/festival factors, monsoon rain by city and month,
  traffic by hour, rider shifts with a dispatch simulation, and surge.
