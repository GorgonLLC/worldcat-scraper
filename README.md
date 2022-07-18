# WorldCat Scraper

Scrapes WorldCat books and stores the results in a SQLite database.

## Usage

```
$ scrapy crawl worldcat
```

To read the scraped data:

```
$ sqlite3 worldcat.db
sqlite>.headers on
sqlite>.mode markdown
sqlite>
SELECT
  oclc_id AS oclc,
  data ->> '$.isbn[1]' AS isbn,
  data ->> '$.title' AS title,
  data ->> '$.authors' AS authors
  FROM books
  WHERE length(title) < 50
  AND json_array_length(data ->> '$.isbn') > 1
  AND json_array_length(authors) < 3
  ORDER BY RANDOM()
  LIMIT 5;

| oclc |     isbn      |                   title                    |               authors               |
|------|---------------|--------------------------------------------|-------------------------------------|
| 1065 | 9780486620107 | Optical aberration coefficients            | ["H  A Buchdahl"]                   |
| 772  | 9780812275827 | Theodore Roosevelt : confident imperialist | ["David H Burton"]                  |
| 786  | 9780816502288 | Southeast Asia; a critical bibliography    | ["K  G Tregonning"]                 |
| 594  | 9780819143471 | The Cambridge Platonists                   | ["Gerald R Cragg"]                  |
| 999  | 9780813808000 | Modern sportswriting                       | ["Louis I Gelfand","Harry E Heath"] |
```

## License

This project is licensed under the terms of the MIT license.
