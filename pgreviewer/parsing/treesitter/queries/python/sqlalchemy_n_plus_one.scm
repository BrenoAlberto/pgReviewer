; sqlalchemy_n_plus_one.scm
; Captures attribute access nodes to be filtered against loop/body scope in Python code.

(attribute
  object: (identifier) @attribute_object
  attribute: (identifier) @attribute_name) @attribute_access
