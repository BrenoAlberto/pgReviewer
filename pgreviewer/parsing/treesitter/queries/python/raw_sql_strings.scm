; Match strings that look like raw SQL statements
(string
  (string_content) @sql_text
  (#match? @sql_text "(?i)^\\s*(SELECT|INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|TRUNCATE|WITH)\\b"))
