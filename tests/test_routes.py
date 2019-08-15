import contextlib
import logging
from nose.tools import (
    assert_raises,
    eq_,
    set_trace,
)
import flask
from flask import Response
from werkzeug.exceptions import MethodNotAllowed

from core.app_server import ErrorHandler
from core.opds import VerboseAnnotator

from app import (
    app,
    MetadataWrangler,
)
import routes

from test_controller import ControllerTest

class MockMetadataWrangler(object):
    """Pretends to be a MetadataWrangler with configured controllers."""

    def __init__(self):
        self._cache = {}

    def __getattr__(self, controller_name):
        return self._cache.setdefault(
            controller_name, MockController(controller_name)
        )

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

    # The first time setup() is called, it will instantiate a real
    # MetadataWrangler object and store it here. We only do this
    # once because it takes about a second to instantiate this object.
    # Calling any of this object's methods could be problematic, since
    # it's probably left over from a previous test, but we won't be
    # calling any methods -- we just want to verify the _existence_,
    # in a real CirculationManager, of the methods called in
    # routes.py.
    REAL_CIRCULATION_MANAGER = None

    def setup(self):
        super(RouteTest, self).setup()

        # Create a MockMetadataWrangler -- this is the one we'll be
        # using in the tests.
        mock_wrangler = MockMetadataWrangler()

        # Swap out any existing MetadataWrangler in the Flask app for
        # our mock.  It'll be restored in teardown.
        self.old_wrangler = getattr(app, 'wrangler', None)
        routes.app.wrangler = mock_wrangler

        # Use the resolver to parse incoming URLs the same way the
        # real app will.
        self.resolver = routes.app.url_map.bind('', '/')

        # For convenience, set self.controller to a specific controller
        # whose routes are being tested.
        controller_name = getattr(self, 'CONTROLLER_NAME', None)
        if controller_name:
            self.controller = getattr(mock_wrangler, controller_name)

            # Make sure there's a controller by this name in the real
            # CirculationManager.
            real_wrangler = MetadataWrangler(self._db)
            self.real_controller = getattr(
                real_wrangler, controller_name
            )
        else:
            self.real_controller = None

        routes.app = app

    def teardown(self):
        super(RouteTest, self).teardown()
        # Restore app.wrangler to its original state (possibly not yet initialized)
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
        eq_(response.method, method)
        eq_(response.method.args, args)
        eq_(response.method.kwargs, kwargs)

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
            assert_raises(MethodNotAllowed, self.request, url, method)
            logging.debug("And it was.")


class TestIndex(RouteTest):

    CONTROLLER_NAME = "index"

    def test_index(self):
        for url in '/', '':
            self.assert_request_calls(url, self.controller.opds_catalog)


class TestHeartbeat(RouteTest):

    CONTROLLER_NAME = "heartbeat"

    def test_heartbeat(self):
        self.assert_request_calls("/heartbeat", self.controller.heartbeat)


class TestCanonicalize(RouteTest):

    CONTROLLER_NAME = "canonicalization"

    def test_canonicalize(self):
        self.assert_request_calls("/canonical-author-name", self.controller.canonicalize_author_name)


class TestURNLookup(RouteTest):

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

class TestCatalog(RouteTest):

    CONTROLLER_NAME = "catalog"

    def test_add(self):
        self.assert_request_calls(
            "/<metadata_identifier>/add", self.controller.add,
            metadata_identifier="<metadata_identifier>",
            http_method="POST"
        )
