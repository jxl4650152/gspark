# -*- coding: UTF-8 -*-
import eventlet
from eventlet.green import socket
from flask import *
from flask_socketio import *
import json
import random
import time
import threading
import struct
import Queue




app = Flask(__name__)
socketio = SocketIO(app)

historyJobs = []
runningJobs = []
CloudWidth = 280
rand = []
thread_running_flag = 0
job_rooms = set()
rooms_count = {}
q = Queue.Queue(10)


@app.route('/')
def hello_world():
    return render_template("introduction.html")


@app.route('/gspark/')
@app.route('/gspark/intro/')
def index():
    return render_template("introduction.html")


@app.route('/gspark/clusters/')
def clusters():
    return render_template("clusters.html")


@app.route('/gspark/jobs/')
def jobs():
    global historyJobs, runningJobs
    historyJobs = [Job(1, "Job15563"), Job(2, "Job16798")]
    runningJobs = [Job(1, "Job25863"), Job(2, "Job26998")]
    return render_template("jobs.html", HistoryJobs=historyJobs, RunningJobs=runningJobs)


@app.route('/gspark/jobs/runningjobs/<jobID>')
def runningjob(jobID):
    global historyJobs, runningJobs, CloudWidth
    job = getJob(jobID)
    return render_template("job.html", HistoryJobs=historyJobs, RunningJobs=runningJobs,
                           jobID=jobID,
                           job=job,
                           CloudWidth=CloudWidth)


@app.route('/gspark/jobs/historyjobs/<jobID>')
def historyjob(jobID):
    global historyJobs, runningJobs
    return render_template("job.html", HistoryJobs=historyJobs, RunningJobs=runningJobs, jobID=jobID)

@socketio.on('connect')
def connect_handler():

    # def random_event(io, sid):
    #     lock = threading.Lock()
    #     cond = threading.Condition(lock=lock)
    #
    #     with app.test_request_context():
    #         while True:
    #             cond.acquire()
    #             cond.wait(timeout=4)
    #             print "thread running", sid
    #             event1 = json.load(open(unicode("/home/LAB/jiaxl/gsparkdata-exch\\task_launch_to_siteDriver.json", "utf8")))
    #             event2 = json.load(open(unicode("/home/LAB/jiaxl/gsparkdata-exch\outter_shuffle.json", "utf8")))
    #             event3 = json.load(open(unicode("/home/LAB/jiaxl/gsparkdata-exch\\task_launch_to_executor.json", "utf8")))
    #             event4 = json.load(open(unicode("/home/LAB/jiaxl/gsparkdata-exch\inner_shuffle.json", "utf8")))
    #
    #             res1 = eventConvert(event1)
    #             res2 = eventConvert(event2)
    #             res3 = eventConvert(event3)
    #             res4 = eventConvert(event4)
    #
    #             event_list = [res1, res2, res3, res4]
    #
    #             num = random.randint(0, 3)
    #             res = event_list[num]
    #             print res
    #             io.emit(res["event"], res["data"], room=sid)
    #
    #             cond.wait(timeout=4)
    #             cond.release()
    print "Client  from ", request.remote_addr, " connected with sid ", request.sid


@socketio.on('recieved')
def connect_handler(data):
    print "recieved data:", data, type(data)
    global rand
    stage0 = "/home/LAB/jiaxl/gspark/data-exch/task_launch_to_siteDriver.json"
    stage1 = "/home/LAB/jiaxl/gspark/data-exch/task_launch_to_executor"
    stage2 = "/home/LAB/jiaxl/gspark/data-exch/inner_shuffle"
    stage3 = "/home/LAB/jiaxl/gspark/data-exch/outter_shuffle"
    event = {}

    if data["stage"] == 0:
        event = json.load(open(stage0))

    elif data["stage"] == 1:
        time.sleep(1)
        stage1 = stage1 + str(data["count"]) + ".json"
        event = json.load(open(stage1))

    elif data["stage"] == 2:
        time.sleep(1)
        while len(rand) < 7:
            i = random.randint(0, 5)
            if i not in rand:
                rand.append(i)
                stage2 = stage2 + str(i) + ".json"
                event = json.load(open(stage2))
                break


    elif data["stage"] == 3:
        time.sleep(2)
        stage3 = stage3 + str(data["count"]) + ".json"
        event = json.load(open(stage3))

    res = eventConvert(event)

    socketio.emit(res["event"], res["data"], room=request.sid)
    print "recieved reply to", request.sid
    print " "


@socketio.on('join')
def on_join(data):
    global thread_running_flag, job_rooms, rooms_count
    room = data['room']
    join_room(room)

    if room in rooms_count:
        rooms_count[room] += 1
    else:
        rooms_count[room] = 1
    print "client join room in thread", threading.currentThread()
    if not job_rooms:
        job_rooms.add(room)
        socketio.start_background_task(target=bg_thread)
        # socketio.start_background_task(target=get_data)
        event = threading.Event()
        event.clear()
        # consu_thread(event).start()
        produ_thread(event).start()
        thread_running_flag = 1
        # # thread.start_new_thread(get_data, args=())
        #
        # if room not in thread_running:
        #     thread_running.append(room)
        #     print "start sending thread..."
        #     my_pool.spawn(send_func, room)
        #     my_pool.spawn(get_data)
        #     my_pool.waitall()
        #             # thread.start_new_thread(send_func, args=(room,))


@socketio.on('disconnect')
def connect_handler():
    global rooms_count, job_rooms
    print "leaving..."
    room = rooms(sid=request.sid, namespace="/")[0]
    leave_room(room)
    rooms_count[room] -= 1
    if rooms_count[room] == 0:
        job_rooms.remove(room)



@socketio.on('close')
def connect_handler():
    print "close"


@socketio.on('ToSiteDriver')
def test_handler():
    #print request.sid
    event = json.load(open("/home/LAB/jiaxl/gspark/data-exch/task_launch_to_siteDriver.json"))
    res = eventConvert(event)
    socketio.emit(res["event"], res["data"], room=request.sid)

@socketio.on('OuterShuffle')
def test_handler():
    #print request.sid
    event = json.load(open("/home/LAB/jiaxl/gspark/data-exch/outter_shuffle.json"))
    res = eventConvert(event)
    socketio.emit(res["event"], res["data"], room=request.sid)

@socketio.on('ToExecutor')
def test_handler():
    #print request.sid
    event = json.load(open("/home/LAB/jiaxl/gspark/data-exch/task_launch_to_executor.json"))
    res = eventConvert(event)
    socketio.emit(res["event"], res["data"], room=request.sid)

@socketio.on('InnerShuffle')
def test_handler():
    #print request.sid
    event = json.load(open("/home/LAB/jiaxl/gspark/data-exch/inner_shuffle.json"))
    res = eventConvert(event)
    socketio.emit(res["event"], res["data"], room=request.sid)

def getJob(id):
    clu_list = json.load(open("/home/LAB/jiaxl/gspark/data-exch/clusters_defination.json"))
    job = Job(clouds=clu_list)
    return job


def eventConvert(event):
    res = {"event": "", "data": {}}
    if event["Event"] == "SparkListenerInnerShuffle":
        res["event"] = "EE"
        res["data"]["exe"] = event["to-host"] + "-" + event["to-component"]
        res["data"]["executors"] = []
        for host in event["from-hosts"]:
            for component in host["from-components"]:
                res["data"]["executors"].append([host["from-host"] + "-" + component["from-component"], len(component["blocks"])])


        return res

    if event["Event"] == "SparkListenerOutterShuffle":
        res["event"] = "SS"
        res["data"]["site"] = event["to-component"]
        res["data"]["site_array"] = []
        for host in event["from-hosts"]:
            for component in host["from-components"]:
                res["data"]["site_array"].append([component["from-component"], len(component["blocks"])])


        return res

    if event["Event"] == "SparkListenerTaskLaunchToExecutor":
        res["event"] = "SE"
        res["data"]["site"] = event["from-component"]
        res["data"]["executors"] = []
        for host in event["to-hosts"]:
            for component in host["to-components"]:
                res["data"]["executors"].append([host["to-host"] + "-" + component["to-component"], len(component["tasks"])])


        return res

    if event["Event"] == "SparkListenerTaskLaunchToSiteDriver":
        res["event"] = "GS"
        res["data"] = []

        for host in event["to-hosts"]:
            for component in host["to-components"]:
                res["data"].append([component["to-component"], len(component["tasks"])])


        return res


def send_func(room):
    # t = int(round(time.time()))
    # while True:
    #     now = int(round(time.time()))
    #     if (now - t) > 3:
    #         t = now

    print "send thread running, room", room
    while True:
        socketio.sleep(1)
        socketio.emit("hello", data, room=room)

    # while True:
    #     # q_event.wait()
    #
    #     socketio.emit("hello", "data", room=room)
    #     print "sending complete"
    #     # q_event.clear()
    #     # else:
    #     #     continue


# def get_data():
#     print "get data thread running in thread", threading.currentThread()
#     while True:
#         msg.append("hello")
#         socketio.sleep(0)


def bg_thread():
    while True:
        if q.empty():
            socketio.sleep(0)
            continue
        else:
            msg = q.get()
            socketio.emit("hello", msg, room="1")
            socketio.sleep(0)


class consu_thread(threading.Thread):
    def __init__(self, event):
        super(consu_thread, self).__init__()
        self.event = event

    def run(self):
        print "consumer thread in", threading.currentThread()
        print "socketio is", socketio
        while True:
            if q.empty():
                print "queue is empty"
                self.event.clear()
                self.event.wait()
            else:
                data = q.get()
                print "consumer get data", data
                send(data)



class produ_thread(threading.Thread):
    def __init__(self, event):
        super(produ_thread, self).__init__()
        self.event = event


    def run(self):
        self.conn()


    def conn(self):
        print "[%s] connecting to Spark" % self.__class__.__name__
        conn = socket.create_connection(("192.168.3.116", 2001), 1000)
        print "[%s] connected to Spark" % self.__class__.__name__

        while True:
            if not job_rooms:
                print "[%s] No receiver, stop connecting with Spark " % self.__class__.__name__
                break
            data_len = conn.recv(4)
            if data_len == "":
                break
            print repr(data_len)
            length = struct.unpack('!I', data_len)[0]
            print length
            data = conn.recv(length)
            print "[%s] recieved data %s" % (self.__class__.__name__, data)
            q.put(data)





def send(data):
    print "sending data..."
    socketio.emit("hello", data, room="1")

class Job:
    id = 0
    name = ""
    clouds = []
    progress = 40

    def __init__(self, id=0, name="", clouds=[]):
        self.id = id
        self.name = name
        self.clouds = clouds


test_data = [
        {"event": "GS", "data": ["cluster1-SiteDriver0", "cluster2-SiteDriver1",  "cluster3-SiteDriver2"]},
        {"event": "SS", "data": {"site":"cluster2-SiteDriver1", "site_array": ["cluster1-SiteDriver0", "cluster3-SiteDriver2"]}},
        {"event": "SS", "data": {"site":"cluster3-SiteDriver2", "site_array": ["cluster1-SiteDriver0", "cluster3-SiteDriver2","cluster4-SiteDriver3", "cluster6-SiteDriver5"]}},
        {"event": "GS", "data": ["cluster2-SiteDriver1", "cluster3-SiteDriver2"]},
        {"event": "SE", "data": {"site":"cluster1-SiteDriver0", "executors": ["cluster1-executor1","cluster1-executor2","cluster1-executor4"]}},
        {"event": "SE", "data": {"site":"cluster2-SiteDriver1", "executors": ["cluster2-executor1","cluster2-executor2","cluster2-executor3"]}},
        {"event": "SE", "data": {"site":"cluster3-SiteDriver2", "executors": ["cluster3-executor1","cluster3-executor2","cluster3-executor4","cluster3-executor8","cluster3-executor9"]}},
        {"event": "EE", "data": {"exe":"cluster1-executor2", "executors": ["cluster1-executor0","cluster1-executor1","cluster1-executor3","cluster1-executor4","cluster1-executor5","cluster1-executor6"]}},
        {"event": "EE", "data": {"exe":"cluster2-executor3", "executors": ["cluster2-executor0","cluster2-executor1","cluster2-executor2","cluster2-executor4"]}},
        {"event": "EE", "data": {"exe":"cluster3-executor6", "executors": ["cluster3-executor0","cluster3-executor4","cluster3-executor8","cluster3-executor7"]}},
        ]

test_data1 = [
        {"event": "EE", "data": {"exe":"cluster1-executor2", "executors": ["cluster1-executor0","cluster1-executor1","cluster1-executor3","cluster1-executor4","cluster1-executor5","cluster1-executor6"]}},
        {"event": "EE", "data": {"exe":"cluster2-executor3", "executors": ["cluster2-executor0","cluster2-executor1","cluster2-executor2","cluster2-executor4"]}},
        {"event": "EE", "data": {"exe":"cluster3-executor6", "executors": ["cluster3-executor0","cluster3-executor4","cluster3-executor8","cluster3-executor7"]}},
        ]








