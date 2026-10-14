# Errors

The framework's errors live in `interloper.errors`. The errors the platform's persistence raises, and its
services map to responses, live in `interloper_db.errors`; both families subclass `InterloperError`.

::: interloper.errors
    options:
      show_root_heading: false

::: interloper_db.errors
    options:
      show_root_heading: true
