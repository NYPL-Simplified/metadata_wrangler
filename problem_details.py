from core.util.problem_detail import ProblemDetail as pd
from core.problem_details import *
from flask_babel import lazy_gettext as _

NO_AUTH_URL = pd(
      "http://librarysimplified.org/terms/problem/no-auth-url",
      400,
      _("No Authentication URL provided"),
      _("You must provide a URL to a public key integration document to register a server."),
)

REMOTE_INTEGRATION_ERROR = pd(
      "http://librarysimplified.org/terms/problem/remote-integration-failed",
      502,
      _("Could not retrieve document"),
      _("The specified URL could not be retrieved."),
)

INVALID_INTEGRATION_DOCUMENT = pd(
    "http://librarysimplified.org/terms/problem/invalid-integration-document",
    400,
    title=_("Invalid integration document")
)
