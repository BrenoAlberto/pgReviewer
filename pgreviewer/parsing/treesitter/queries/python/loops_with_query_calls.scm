; Match for/async-for loop body with direct query call.
(for_statement
  body: (block
    (expression_statement
      (call
        function: (attribute
          attribute: (identifier) @method_name
        )
      ) @query_call
    )
  )
) @loop

; Match for/async-for loop body with awaited query call.
(for_statement
  body: (block
    (expression_statement
      (await
        (call
          function: (attribute
            attribute: (identifier) @method_name
          )
        ) @query_call
      )
    )
  )
) @loop

; Match while loop body with direct query call.
(while_statement
  body: (block
    (expression_statement
      (call
        function: (attribute
          attribute: (identifier) @method_name
        )
      ) @query_call
    )
  )
) @loop

; Match while loop body with awaited query call.
(while_statement
  body: (block
    (expression_statement
      (await
        (call
          function: (attribute
            attribute: (identifier) @method_name
          )
        ) @query_call
      )
    )
  )
) @loop
