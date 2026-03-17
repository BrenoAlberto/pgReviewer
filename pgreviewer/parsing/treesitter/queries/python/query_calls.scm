; Match dynamic SQL execution calls like cursor.execute("...")
(call
  function: (attribute
    attribute: (identifier) @method_name)
  arguments: (argument_list
    (string (string_content) @sql_text))
  @query_call
  (#match? @method_name "^(execute|fetch|fetchrow|fetchval|fetchone|fetchall)$"))

; Also match query-like calls when SQL text is not a static string.
(call
  function: (attribute
    attribute: (identifier) @method_name)
  @query_call
  (#match? @method_name "^(execute|fetch|fetchrow|fetchval|fetchone|fetchall)$"))
