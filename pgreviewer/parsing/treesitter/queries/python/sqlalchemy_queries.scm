; sqlalchemy_queries.scm
; Tree-sitter queries for SQLAlchemy ORM query detection.
; Used by pgreviewer/parsing/sqlalchemy_query_extractor.py

; ---- session.query(Model) and any .query() calls ----
; Captures the call node; Python post-processor walks up to the chain root,
; then collects all chained methods (filter, where, join, order_by, etc.).
(call
  function: (attribute
    attribute: (identifier) @query_method)
  (#eq? @query_method "query")) @query_call

; ---- select(Model) – SQLAlchemy Core style ----
; Captures the call node; Python post-processor walks the chain.
(call
  function: (identifier) @select_func
  (#eq? @select_func "select")) @select_call
