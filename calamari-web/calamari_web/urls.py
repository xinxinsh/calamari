from django.conf.urls import patterns, include, url

from settings import STATIC_ROOT, GRAPHITE_API_PREFIX, CONTENT_DIR

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()


urlpatterns = patterns(
    '',

    # These views are needed for the django-rest-framework debug interface
    # to be able to log in and out.  The URL path doesn't matter, rest_framework
    # finds the views by name.
    url(r'^api/rest_framework/', include('rest_framework.urls', namespace='rest_framework')),

    url(r'^$', 'calamari_web.views.home'),

    url(r'^api/v1/', include('calamari_rest.urls.v1')),
    url(r'^api/v2/', include('calamari_rest.urls.v2')),

    url(r'^bootstrap$', 'calamari_web.views.bootstrap', name='bootstrap'),

    url(r'^dashboard/(?P<path>.*)$', 'calamari_web.views.dashboard',
        {'document_root': '%s/dashboard/' % STATIC_ROOT},
        name='dashboard'),

    url(r'^render/?', include('graphite.render.urls')),
    url(r'^metrics/?', include('graphite.metrics.urls')),
    url(r'^%s/dashboard/?' % GRAPHITE_API_PREFIX.lstrip('/'), include('graphite.dashboard.urls')),

    # XXX this is a hack to make graphite visible where the 1.x GUI expects it,
    url(r'^graphite/render/?', include('graphite.render.urls')),
    url(r'^graphite/metrics/?', include('graphite.metrics.urls')),

    # XXX this is a hack to make graphite dashboard work in dev mode (full installation
    # serves this part with apache)
    url('^content/(?P<path>.*)$', 'django.views.static.serve', {'document_root': CONTENT_DIR}),

    # XXX this is a hack to serve apt repo in dev mode (Full installation serves this with apache)
    url(r'^static/precise/(?P<path>.*)$', 'django.views.static.serve',
        {'document_root': '%s/precise/' % STATIC_ROOT}),
    url(r'^static/trusty/(?P<path>.*)$', 'django.views.static.serve',
        {'document_root': '%s/trusty/' % STATIC_ROOT}),
    url(r'^static/wheezy/(?P<path>.*)$', 'django.views.static.serve',
        {'document_root': '%s/wheezy/' % STATIC_ROOT}),
    url(r'^static/el6/(?P<path>.*)$', 'django.views.static.serve',
        {'document_root': '%s/el6/' % STATIC_ROOT}),
    url(r'^static/rhel6/(?P<path>.*)$', 'django.views.static.serve',
        {'document_root': '%s/rhel6/' % STATIC_ROOT}),
    url(r'^static/rhel7/(?P<path>.*)$', 'django.views.static.serve',
        {'document_root': '%s/rhel7/' % STATIC_ROOT}),
)

UI_PATHS = ['login', 'admin', 'manage']

for path in UI_PATHS:
    urlpatterns = urlpatterns + [url(r'^{0}/(?P<path>.*)$'.format(path), 'calamari_web.views.serve_dir_or_index',
                                     {'document_root': '{0}/{1}/'.format(STATIC_ROOT, path)})]

handler500 = 'calamari_web.views.server_error'

# Graphite dashboard client code is not CSRF enabled, but we have
# global CSRF protection enabled.  Make exceptions for the views
# that the graphite dashboard wants to POST to.
from django.views.decorators.csrf import csrf_exempt

# By default graphite views are visible to anyone who asks:
# we only want to allow logged in users to access graphite
# API.
from django.contrib.auth.decorators import login_required


def patch_views(mod):
    for url_pattern in mod.urlpatterns:
        cb = url_pattern.callback
        url_pattern._callback = csrf_exempt(login_required(cb))


# Suppress warning from graphite's use of old django API
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning,
                        message="django.conf.urls.defaults is deprecated")

import graphite.metrics.urls
import graphite.dashboard.urls
patch_views(graphite.metrics.urls)
patch_views(graphite.dashboard.urls)
