#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'wxmimperio'

import tornado.httpserver
import tornado.websocket
import tornado.ioloop
import tornado.web
import pika
from redis import Redis
from threading import Thread


clients = []

def threaded_redis():
    redis = Redis(host="spark-master",port=6379)
    while True:
        res = redis.rpop("realtime_data_queue")
        if res == None:
            pass
        else:
            print res.decode("unicode-escape").encode("utf-8")
            for itm in clients:
                itm.write_message(res.decode("unicode-escape").encode("utf-8"))

class WSHandler(tornado.websocket.WebSocketHandler):

    def open(self):
        print 'new connection'
        #self.write_message("Hello ,i am open")
        clients.append(self)
        
    def on_message(self,message):
        print 'called'
        self.write_message()
            
    def on_close(self):
        print "WebSocket closed"
        clients.remove(self)

    def check_origin(self,origin):
    #    print origin
        return True


application = tornado.web.Application([
    (r'/ws',WSHandler),
])

if __name__ == "__main__":
    thread = Thread(target = threaded_redis)
    thread.start()
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(5050)
    tornado.ioloop.IOLoop.instance().start()
