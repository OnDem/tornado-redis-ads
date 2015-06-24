# -*- coding: utf-8 -*-
import tornado.ioloop
import tornado.web
from tornado.options import parse_command_line, define, options
import redis
import logging
from string import split
from random import randint

configFile = './config.csv'

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('main')

redisConnectionPool = redis.ConnectionPool(host='localhost', port=6379, db=12)
redisClient = redis.Redis(connection_pool=redisConnectionPool)
redisClient.flushdb()

def on_set(bannerUrl,paidShows):
  log.debug("set banner views: %s - %d " % (bannerUrl,paidShows))

with open(configFile) as f:
  settings = f.readlines()

for sLine in settings:
  settingsList = split(sLine,';')
  bannerUrl = settingsList.pop(0)
  paidShows = int(settingsList.pop(0))
  for bannerCategory in settingsList:
    redisClient.sadd(bannerCategory.strip(),"%s#%d" % (bannerUrl,paidShows))
    on_set(bannerUrl,paidShows)

class MainHandler(tornado.web.RequestHandler):
  def get(self):
    catsFromURI = self.get_query_arguments('category',[])
    catlen = len(catsFromURI)
    if catlen > 0:
      bannerCategory = catsFromURI.pop(randint(0,catlen-1))
    else:
      bannerCategory = 'tv'
    bannerUrl, paidShows = split(redisClient.srandmember(bannerCategory),'#')
    paidShows = int(paidShows)
    if redisClient.sismember(bannerCategory,"%s#%d" % (bannerUrl,paidShows)):
      redisClient.srem(bannerCategory,"%s#%d" % (bannerUrl,paidShows))
      paidShows = paidShows - 1
      if paidShows > 1:
        redisClient.sadd(bannerCategory,"%s#%d" % (bannerUrl,paidShows))
      on_set(bannerUrl,paidShows)
    self.set_header('Content-Type', 'text/html; charset=utf-8')
    self.render("banner.html", title="banner", bannerUrl=bannerUrl, paidShows=paidShows, cc=bannerCategory)

application = tornado.web.Application([
  (r"/", MainHandler),
])

if __name__ == "__main__":
  parse_command_line()
  application.listen(8888)
  tornado.ioloop.IOLoop.instance().start()
