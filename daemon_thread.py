import threading
import json
import socket


# Daemon thread  waiting for spark listener to connect
# For every connection, a new_thread will be created to
# do concrete work
class daemon_thread(threading.Thread):
    def __init__(self, redis_conn):
        super(daemon_thread, self).__init__()
        self.redis_conn = redis_conn
    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', 6666))
        s.listen(1)
        while True:
            print "waiting for connection..."
            conn, addr = s.accept()
            print "connection from Spark app"
            t = new_thread(conn, self.redis_conn)
            t.start()


# Thread created by daemon_thread to recv spark event
class new_thread(threading.Thread):
    def __init__(self, conn, redis_conn):
        super(new_thread, self).__init__()
        self.conn = conn
        self.redis_conn = redis_conn

    def run(self):
        appId = ''
        temp_ls = []
        count = 1
        exit_flag = False
        print "running..."
        while True:
            if exit_flag:
                print "disconn at", count
                break

            try:
                while True:
                    t = self.conn.recv(1)
                    if t == "":
                        exit_flag = True
                        print "exit", count
                        break
                    elif t != "{":
                        print "no data"
                    else:
                        break
                if exit_flag:
                    print "exit", count
                    break

                print "start recv"
                flag = 1
                s = '{'
                while flag != 0:
                    s_next = self.conn.recv(1)
                    if s_next == '':
                        exit_flag = True
                        print "exit", count
                        break
                    s += s_next
                    if s_next == '}':
                        flag -= 1
                    if s_next == '{':
                        flag += 1
                if exit_flag:
                    print "exit", count
                    break

                if flag == 0:
                    count = count + 1
                    print count
                    if appId:
                        self.redis_conn.lpush(appId, s)
                        e = json.loads(s)
                        if e['Event'] == "SparkListenerApplicationEnd":
                            print "app end"
                            self.redis_conn.lrem("run_apps", 1, appId)
                            self.redis_conn.lpush("fin_apps", appId)
                    else:
                        print s
                        e = json.loads(s)
                        if e['Event'] == "SparkListenerApplicationStart":
                            appId = e['App ID']
                            print appId
                            self.redis_conn.lpush("run_apps", appId)
                            for temp_e in temp_ls:
                                self.redis_conn.lpush(appId, temp_e)
                                self.redis_conn.lpush(appId, s)
                        else:
                            temp_ls.append(s)
            except socket.error as se:
                print se
                break
