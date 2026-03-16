; sqlalchemy_models.scm
; Tree-sitter queries for SQLAlchemy declarative model analysis.
; Used by pgreviewer/parsing/sqlalchemy_analyzer.py

; --- Model class definitions ---
; Matches classes that inherit from Base, DeclarativeBase, or DeclarativeBaseNoMeta.
; Python post-processing filters @base_class to the known base-class names.
(class_definition
  name: (identifier) @class_name
  superclasses: (argument_list
    (identifier) @base_class))

; --- Table name ---
; __tablename__ = "table_name"
(assignment
  left: (identifier) @tablename_attr
  right: (string) @tablename_value
  (#eq? @tablename_attr "__tablename__"))

; --- Column definitions ---
; column_name = Column(Type, ForeignKey("target"), index=True, nullable=False, ...)
(assignment
  left: (identifier) @col_name
  right: (call
    function: (identifier) @col_func
    arguments: (argument_list) @col_args)
  (#eq? @col_func "Column"))

; --- ForeignKey references ---
; ForeignKey("target_table.column")
; Post-processing extracts the target string from @fk_args.
(call
  function: (identifier) @fk_func
  arguments: (argument_list) @fk_args
  (#eq? @fk_func "ForeignKey"))

; --- Relationship definitions ---
; rel_name = relationship("Model", back_populates="...", foreign_keys=[...])
(assignment
  left: (identifier) @rel_name
  right: (call
    function: (identifier) @rel_func
    arguments: (argument_list) @rel_args)
  (#eq? @rel_func "relationship"))

; --- Explicit Index definitions ---
; Captures Index() calls anywhere: standalone assignments and inside __table_args__.
; Index("ix_name", "col1", "col2", unique=True)
(call
  function: (identifier) @idx_func
  arguments: (argument_list) @idx_args
  (#eq? @idx_func "Index"))

; --- __table_args__ ---
; __table_args__ = (Index(...), UniqueConstraint(...), {...})
; Captures the full right-hand side for Python-side traversal.
(assignment
  left: (identifier) @table_args_attr
  right: (_) @table_args_value
  (#eq? @table_args_attr "__table_args__"))
