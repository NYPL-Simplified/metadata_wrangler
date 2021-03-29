import contextlib
import logging
import pytest
import flask
from flask import Response
from werkzeug.exceptions import MethodNotAllowed

from core.app_server import ErrorHandler
from core.opds import VerboseAnnotator

from app import app
from controller import MetadataWrangler
from problem_details import INVALID_CREDENTIALS
import routes

from .test_controller import ControllerTest

class MockMetadataWrangler(object):
    """Pretends to be a MetadataWrangler with configured controllers."""

    AUTHENTICATED_CLIENT = "I'm an IntegrationClient"

    def __init__(self):
        self._cache = {}
        self.authenticated = False

    def __getattr__(self, controller_name):
        """Look up a MockController or create a new one."""
        return self._cache.setdefault(
            controller_name, MockController(controller_name)
        )

    def authenticated_client_from_request(self, _db, required=True):
        """Mock authenticated_client_from_request based on
        whether this method has .authenticated set.
        """
        if self.authenticated:
            return self.AUTHENTICATED_CLIENT
        else:
            if required:
                return INVALID_CREDENTIALS
            return None


class MockControllerMethod(object):
    """Pretends to be one of the methods of a controller class."""
    def __init__(self, controller, name):
        """Constructor.

        :param controller: A MockController.
        :param name: The name of this method.
        """
        self.controller = controller
        self.name = name
        self.callable_name = name

    def __call__(self, *args, **kwargs):
        """Simulate a successful method call.

        :return: A Response object, as required by Flask, with this
        method smuggled out as the 'method' attribute.
        """
        self.args = args
        self.kwargs = kwargs
        response = Response("I called %s" % repr(self), 200)
        response.method = self
        return response

    def __repr__(self):
        return "<MockControllerMethod %s.%s>" % (
            self.controller.name, self.name
        )

class MockController(MockControllerMethod):
    """Pretends to be a controller.

    A controller has methods, but it may also be called _as_ a method,
    so this class subclasses MockControllerMethod.
    """

    def __init__(self, name):
        """Constructor.

        :param name: The name of the controller.
        """
        self.name = name

        # If this controller were to be called as a method, the method
        # name would be __call__, not the name of the controller.
        self.callable_name = '__call__'

        self._cache = {}

    def __getattr__(self, method_name):
        """Locate a method of this controller as a MockControllerMethod."""
        return self._cache.setdefault(
            method_name, MockControllerMethod(self, method_name)
        )

    def __repr__(self):
        return "<MockControllerMethod %s>" % self.name


class RouteTest(ControllerTest):
    """Test what happens when an HTTP request is run through the
    routes we've registered with Flask.
    """

    def setup_method(self):
        super(RouteTest, self).setup_method()

        # Create a MockMetadataWrangler -- this is the one we'll be
        # using in the tests.
        self.mock_wrangler = MockMetadataWrangler()

        # Create a real MetadataWrangler -- we'll use this to check
        # whether the controllers we're accessing really exist.
        #
        # Unlike with the circulation manager, creating a
        # MetadataWrangler is really cheap and we can do a fresh one
        # for every test.
        self.real_wrangler = MetadataWrangler(self._db)

        # Swap out any existing MetadataWrangler in the Flask app for
        # our mock.  It'll be restored in teardown.
        self.old_wrangler = getattr(app, 'wrangler', None)
        routes.app.wrangler = self.mock_wrangler

        # Use the resolver to parse incoming URLs the same way the
        # real app will.
        self.resolver = routes.app.url_map.bind('', '/')

        # Set self.controller to a mocked version of the controller
        # whose routes are being tested in this class.
        controller_name = self.CONTROLLER_NAME
        self.controller = getattr(self.mock_wrangler, controller_name)

        # Make sure there's a controller by that name in the real
        # MetadataWrangler.
        self.real_controller = getattr(self.real_wrangler, controller_name)
        if not self.real_controller:
            raise Exception("No such controller: %s" % controller_name)

    def teardown_method(self):
        super(RouteTest, self).teardown_method()
        # Restore app.wrangler to its original state (possibly not yet
        # initialized)
        if self.old_wrangler:
            app.wrangler = self.old_wrangler
        else:
            del app.wrangler

    def request(self, url, method='GET'):
        """Simulate a request to a URL without triggering any code outside
        routes.py.
        """
        # Map an incoming URL to the name of a function within routes.py
        # and a set of arguments to the function.
        function_name, kwargs = self.resolver.match(url, method)

        # Locate the corresponding function in our mock app.
        mock_function = getattr(routes, function_name)

        # Call the function.
        with routes.app.test_request_context():
            return mock_function(**kwargs)

    def assert_request_calls(self, url, method, *args, **kwargs):
        """Make a request to the given `url` and assert that
        the given controller `method` was called with the
        given `args` and `kwargs`.
        """
        http_method = kwargs.pop('http_method', 'GET')
        response = self.request(url, http_method)
        assert response.method == method
        assert response.method.args == args
        assert response.method.kwargs == kwargs

        # Make sure the real controller has a method by the name of
        # the mock method that was called. We won't call it, because
        # it would slow down these tests dramatically, but we can make
        # sure it exists.
        if self.real_controller:
            real_method = getattr(self.real_controller, method.callable_name)

            # TODO: We could use inspect.getarcspec to verify that the
            # argument names line up with the variables passed in to
            # the mock method. This might remove the need to call the
            # mock method at all.

    def assert_authenticated_request_calls(self, url, method, *args, **kwargs):
        """First verify that an unauthenticated request fails. Then make an
        authenticated request to `url` and verify the results, as with
        assert_request_calls
        """
        http_method = kwargs.get('http_method', 'GET')
        body, status_code, headers = self.request(url, http_method)
        assert 401 == status_code

        # Set a variable so that our mocked
        # authenticated_client_from_request will succeed, and try
        # again.
        self.mock_wrangler.authenticated = True
        try:
            self.assert_request_calls(url, method, *args, **kwargs)
        finally:
            # Un-set authentication for the benefit of future
            # assertions in this test function.
            self.mock_wrangler.authenticated = False

    def assert_supported_methods(self, url, *methods):
        """Verify that the given HTTP `methods` are the only ones supported
        on the given `url`.
        """
        # The simplest way to do this seems to be to try each of the
        # other potential methods and verify that MethodNotAllowed is
        # raised each time.
        check = set(['GET', 'POST', 'PUT', 'DELETE']) - set(methods)

        # Treat HEAD specially. Any controller that supports GET
        # automatically supports HEAD. So we only assert that HEAD
        # fails if the method supports neither GET nor HEAD.
        if 'GET' not in methods and 'HEAD' not in methods:
            check.add('HEAD')
        for method in check:
            logging.debug("MethodNotAllowed should be raised on %s", method)
            pytest.raises(MethodNotAllowed, self.request, url, method)
            logging.debug("And it was.")


class TestIndex(RouteTest):
    """Test routes that end up in the IndexController."""

    CONTROLLER_NAME = "index"

    def test_index(self):
        for url in '/', '':
            self.assert_request_calls(url, self.controller.opds_catalog)


class TestHeartbeat(RouteTest):
    """Test routes that end up in the HeartbeatController."""

    CONTROLLER_NAME = "heartbeat"

    def test_heartbeat(self):
        self.assert_request_calls("/heartbeat", self.controller.heartbeat)


class TestCanonicalize(RouteTest):
    """Test routes that end up in the CanonicalizationController."""

    CONTROLLER_NAME = "canonicalization"

    def test_canonicalize(self):
        self.assert_request_calls(
            "/canonical-author-name", self.controller.canonicalize_author_name
        )


class TestURNLookup(RouteTest):
    """Test routes that end up in the URNLookupController."""

    CONTROLLER_NAME = "urn_lookup"

    def test_lookup_no_collection(self):
        self.assert_request_calls(
            "/lookup", self.controller.work_lookup,
            VerboseAnnotator,
            require_active_licensepool=False,
            metadata_identifier=None
        )

    def test_lookup_with_collection(self):
        self.assert_request_calls(
            "/<metadata_identifier>/lookup", self.controller.work_lookup,
            VerboseAnnotator,
            require_active_licensepool=False,
            metadata_identifier="<metadata_identifier>"
        )

    # TODO: We're running accepts_auth but we're only testing the case
    # where no auth is provided.

class TestCatalog(RouteTest):
    """Test routes that end up in the CatalogController."""

    CONTROLLER_NAME = "catalog"

    def test_add(self):
        url = "/<metadata_identifier>/add"
        self.assert_authenticated_request_calls(
            url, self.controller.add_items,
            metadata_identifier="<metadata_identifier>",
            http_method="POST"
        )
        self.assert_supported_methods(url, 'POST')

    def test_add_with_metadata(self):
        url = "/<metadata_identifier>/add_with_metadata"
        self.assert_authenticated_request_calls(
            url, self.controller.add_with_metadata,
            metadata_identifier="<metadata_identifier>",
            http_method="POST"
        )
        self.assert_supported_methods(url, 'POST')

    def test_metadata_needed(self):
        self.assert_authenticated_request_calls(
            "/<metadata_identifier>/metadata_needed",
            self.controller.metadata_needed_for,
            metadata_identifier="<metadata_identifier>",
        )

    def test_updates(self):
        self.assert_authenticated_request_calls(
            "/<metadata_identifier>/updates",
            self.controller.updates_feed,
            metadata_identifier="<metadata_identifier>",
        )

    def test_remove(self):
        url = "/<metadata_identifier>/remove"
        self.assert_authenticated_request_calls(
            url, self.controller.remove_items,
            metadata_identifier="<metadata_identifier>",
            http_method="POST"
        )
        self.assert_supported_methods(url, 'POST')


class TestIntegrationClient(RouteTest):
    """Test routes that end up in the IntegrationClientController."""

    CONTROLLER_NAME = "integration"

    def test_register(self):
        url = "/register"
        self.assert_request_calls(
            url, self.controller.register, http_method="POST"
        )
        self.assert_supported_methods(url, 'POST')
