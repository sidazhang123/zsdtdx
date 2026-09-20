# Third-Party Notices

This project (`zsdtdx`) is developed with reference to and partial adaptation of
the upstream project:

- Project: `pytdx`
- Homepage: <https://github.com/rainx/pytdx>
- PyPI: <https://pypi.org/project/pytdx/>

## Attribution

Portions of protocol parsing, socket framing, and API surface naming are derived
from or inspired by `pytdx`, with additional wrapper, retry, routing, batching,
and parallel-fetch logic implemented in this project.

However, `zsdtdx` is **not** a direct secondary wrapper of `pytdx` as a runtime
dependency. A number of request builders and response parsers (including, but
not limited to, handshake packets, K-line requests, security/instrument catalog
pages, company-info / F10 paging, and related decode paths) have been
**re-analyzed and reimplemented** against live client/server captures. Their
wire formats and field layouts may therefore diverge from `pytdx` and must not
be assumed byte-identical to upstream.

## License Status Note

During packaging preparation, the `pytdx` 1.72 source distribution metadata on
PyPI reports `License: UNKNOWN`, and the source archive does not contain a
standalone `LICENSE` file.

Before public distribution, maintainers should verify the upstream license terms
from the authoritative upstream repository and retain any required notices in
this file.
