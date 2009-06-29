import logging
from datetime import date, datetime
from collections import defaultdict
from google.appengine.ext import db
from google.appengine.api import memcache
from pygooglechart import SparkLineChart, SimpleLineChart
from pygooglechart import Axis

class Timeline(db.Model):
  name = db.StringProperty()
  submitted = db.DateTimeProperty()
  
  start = db.DateTimeProperty()
  end = db.DateTimeProperty()
  
  mapcount = db.IntegerProperty()
  redcount = db.IntegerProperty()
    
  maps = db.TextProperty()
  reducers = db.TextProperty()
  shuffles = db.TextProperty()
  merges = db.TextProperty()
  waste = db.TextProperty()
  
  
  def duration(self):
    return self.end - self.start
  
PROP_LISTS = ('maps', 'reducers', 'shuffles', 'merges', 'waste')
  
def generate_graph(tl, small=False):
  
  sources = [tl.maps, tl.shuffles, tl.merges, tl.reducers, tl.waste]
  sources = [[int(c) for c in source.split(',')] for source in sources]
  sources.reverse()

  total = len(sources[0])
  sample = max(total/100, 1)
  actual = min(total, 100)

  # Set the vertical range from 0 to 50

  max_y = 0
  for i in xrange(total):
    max_y = max(max_y, sum(x[i] for x in sources))

  max_y += .05 * max_y
  max_y = round(max_y / 10.0) * 10

  width = 200 if small else 600
  height = 125 if small else 375
  if small:
    chart = SparkLineChart(width, height, y_range=[0, max_y])
  else:
    chart = SimpleLineChart(width, height, y_range=[0, max_y])

  # First value is the highest Y value. Two of them are needed to be
  # plottable.
  chart.add_data([max_y] * 2)

  # 3 sets of real data
  for i, source in enumerate(sources):
    data = [sum(x[t * sample] for x in sources[i:]) for t in xrange(actual)]
    chart.add_data(data)

  # Last value is the lowest in the Y axis.
  chart.add_data([0] * 2)

  chart.set_colours(['FFFFFF','FF3399','6666CC','669933','CC3333','33CCFF','FFFFFF'])

  for i in xrange(7):
    chart.set_line_style(i, 0)

  chart.add_fill_range('FF3399', 1, 2)
  chart.add_fill_range('6666CC', 2, 3)
  chart.add_fill_range('669933', 3, 4)
  chart.add_fill_range('CC3333', 4, 5)
  chart.add_fill_range('33CCFF', 5, 6)

  # Some axis data
  if not small:
    legend = ['','maps','shuffle','merge','reduce','waste','']
    legend.reverse()
    chart.set_legend(legend)
    chart.set_grid(0, 25)
    chart.set_axis_labels(Axis.LEFT, ['', int(max_y/4), int(max_y / 2), int(max_y*3/4), ''])
    chart.set_axis_labels(Axis.BOTTOM, ['', int(total/4), int(total/2), int(total*3/4), ''])

  try:
    return chart.get_url()
  except:
    return '/static/invalid_graph.png'

def create(request):
  values = {}
  for prop in PROP_LISTS:
    try:
      values[prop] = [int(count) for count in request.get(prop).split(',')]
    except:
      raise ValueError('Invalid count type. Only integers allowed')
  
  # all must be of same length
  lengths = set([len(v) for v in values.values()])
  if len(lengths) != 1 or list(lengths)[0] <= 0:
    raise ValueError('Invalid count array length')
    
  # store again
  values = dict([(k,",".join(str(c) for c in v)) for k,v in values.items()])
  
  values['start'] = datetime.fromtimestamp(int(request.get('start')))
  values['end'] = datetime.fromtimestamp(int(request.get('end')))
  
  values['mapcount'] = int(request.get('mapcount'))
  values['redcount'] = int(request.get('redcount'))
  
  timeline = Timeline(**values)
  timeline.submitted = datetime.today()
  timeline.put()
  return timeline.key().id()
  
  