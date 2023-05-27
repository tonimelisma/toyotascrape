# toyotascrape

Scrape current US Toyota inventory, showing unsold vehicles at different dealerships. Also calculate dealer adjustments, mandatory dealer accessories and other mark-ups for each care (where advertised).

Outputs Parquet and Excel

Pipeline

- Scrape cars
- Transform cars
- Scrape dealers
- Transform cars with dealers
- Calculate dealer markups
- Calculate new cars w/o markups

TODO

- Do calculations to old daily directories idempotently
