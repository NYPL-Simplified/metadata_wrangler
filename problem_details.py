from core.util.problem_detail import ProblemDetail as pd
from core.problem_details import *
from flask.ext.babel import lazy_gettext as _

NO_OPDS_URL = pd(
      "http://librarysimplified.org/terms/problem/no-opds-url",
      400,
      _("No OPDS URL"),
      _("You must provide an OPDS URL to register a library."),
)

INVALID_OPDS_FEED = pd(
      "http://librarysimplified.org/terms/problem/invalid-opds-feed",
      400,
      _("Invalid OPDS feed"),
      _("The submitted URL did not return a valid OPDS feed."),
)

AUTH_DOCUMENT_NOT_FOUND = pd(
    "http://librarysimplified.org/terms/problem/auth-document-not-found",
    400,
    title=_("Authentication document not found"),
    detail=_("You submitted an OPDS server, but I couldn't find an OPDS authentication document."),
)

INVALID_AUTH_DOCUMENT = pd(
    "http://librarysimplified.org/terms/problem/invalid-auth-document",
    400,
    title=_("Invalid auth document"),
    detail=_("The OPDS authentication document is not valid JSON."),
)
