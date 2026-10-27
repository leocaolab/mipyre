//! The family of errors whose decider is the app's author: what the app declares, taken
//! together, cannot be served, and only a change to its code fixes it.

pyo3::create_exception!(
    mipyre.engine,
    DeclarationError,
    pyo3::exceptions::PyException,
    "What the app declares cannot be served: the server does not start (or the call refuses), \
     and the app's code has to change. `except DeclarationError` catches the whole family."
);
