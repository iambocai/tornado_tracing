'''RPC Tracing support.

Records timing information about rpcs and other operations for performance
profiling.  Currently just a wrapper around the Google App Engine appstats
module.
'''

import contextlib
import functools
import logging
import tornado.httpclient
import tornado.web
import tornado.wsgi
import warnings
import sys


with warnings.catch_warnings():
    warnings.simplefilter('ignore', DeprecationWarning)
    from tryfer.tracers import ZipkinTracer
from tornado.options import define, options
from tornado.stack_context import StackContext

# import libraries if trace enabled
# define("zipkin_trace", type=bool, default=True, help="Enable zipkin trace.(default: True)")
if options.zipkin_trace:
    import tornado.platform.twisted
    tornado.platform.twisted.install()
    from twisted.internet import reactor
    from tryfer.tracers import push_tracer, DebugTracer, EndAnnotationTracer, ZipkinTracer, RESTkinHTTPTracer
    from tryfer.trace import Trace, Annotation, Endpoint
    from tryfer.formatters import hex_str
    from twisted.web.client import Agent
    from scrivener import ScribeClient
    from twisted.internet.endpoints import clientFromString
    from twisted.internet.endpoints import TCP4ClientEndpoint

def save():
    '''Returns an object that can be passed to restore() to resume
    a suspended record.
    '''
    return recording.recorder

def restore(recorder):
    '''Reactivates a previously-saved recording context.'''
    recording.recorder = recorder


class RequestHandler(tornado.web.RequestHandler):
    '''RequestHandler subclass that establishes a recording context for each
    request.
    '''
    def __init__(self, *args, **kwargs):
        super(RequestHandler, self).__init__(*args, **kwargs)
        self.__recorder = None

    def _execute(self, transforms, *args, **kwargs):
        if options.enable_appstats:
            start_recording(tornado.wsgi.WSGIContainer.environ(self.request))
            recorder = save()
            @contextlib.contextmanager
        super(RequestHandler, self)._execute(transforms, *args, **kwargs)

    def finish(self, chunk=None):
        super(RecordingRequestHandler, self).finish(chunk)
        if options.enable_appstats:
            end_recording(self._status_code)

class FallbackHandler(tornado.web.FallbackHandler):
    '''FallbackHandler subclass that establishes a recording context for
    each request.
    '''
    def prepare(self):
        if options.enable_trace:
            push_tracer(EndAnnotationTracer(
                    RESTkinHTTPTracer(Agent(reactor),
                            trace_url='http://localhost:6956/v1.0/22/trace', 
                            max_traces=1,
                            max_idle_time=0)))
        
        super(FallbackHandler, self).prepare()

def _request_info(request):
    '''Returns a tuple (method, url) for use in recording traces.

    Accepts either a url or HTTPRequest object, like HTTPClient.fetch.
    '''
    if isinstance(request, tornado.httpclient.HTTPRequest):
        return (request.method, request.url)
    else:
        return ('GET', request)

class HTTPClient(tornado.httpclient.HTTPClient):
    def fetch(self, request, *args, **kwargs):
        method, url = _request_info(request)
        ZipkinTracer.record(Annotation.string('Url', url))
        ZipkinTracer.record(Annotation.client_send())
        response = super(HTTPClient, self).fetch(request, *args, **kwargs)
        ZipkinTracer.record(Annotation.client_recv())
        return response

class AsyncHTTPClient(tornado.httpclient.AsyncHTTPClient):
    def fetch(self, request, callback, *args, **kwargs):
        method, url = _request_info(request)
        ZipkinTracer.record(Annotation.string('Url', url))
        ZipkinTracer.record(Annotation.client_send())
        def wrapper(request, callback, response, *args):
            ZipkinTracer.record(Annotation.client_recv())
            callback(response)
        super(AsyncHTTPClient, self).fetch(
          request,
          functools.partial(wrapper, request, callback),
          *args, **kwargs)
