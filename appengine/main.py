#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import logging
import wsgiref.handlers
import timeline

from google.appengine.ext import webapp
from google.appengine.ext.webapp import template

class MainPageHandler(webapp.RequestHandler):
  def get(self):
    results = timeline.Timeline.all().order('-submitted').fetch(limit=20)
    graphs = [{'id': tl.key().id(), 'url': timeline.generate_graph(tl, small=True)} for tl in results]
    template_file = os.path.join(os.path.dirname(__file__), 'main.html')
    self.response.out.write(template.render(template_file, {'graphs': graphs}))

class TimelineHandler(webapp.RequestHandler):
  def get(self, id):
    tl = timeline.Timeline.get_by_id(int(id))
    graph = timeline.generate_graph(tl)
    template_file = os.path.join(os.path.dirname(__file__), 'timeline.html')
    self.response.out.write(template.render(template_file, {'graph': graph, 'tl': tl}))

class TimelinesHandler(webapp.RequestHandler):
  def post(self):
    id = timeline.create(self.request)
    self.redirect("/timeline/" + str(id))

def main():
  application = webapp.WSGIApplication([
    (r"/", MainPageHandler),
    (r"/timelines", TimelinesHandler),
    (r"/timeline/([^/]+)", TimelineHandler),
  ], debug=True)
  wsgiref.handlers.CGIHandler().run(application)

if __name__ == '__main__':
  main()
