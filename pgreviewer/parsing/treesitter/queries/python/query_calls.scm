; Match dynamic SQL execution calls like cursor.execute("...")
(call
  function: (attribute
    attribute: (identifier) @method_name)
  arguments: (argument_list
    (string (string_content) @sql_text))
  (#match? @method_name "^(execute|fetch|fetchrow|fetchval|fetchone|fetchall)$"))
