# -*- coding: UTF-8 -*-

from flask import *
from flask_socketio import SocketIO
import json
import random
import time
import threading



app = Flask(__name__)
socketio = SocketIO(app)

historyJobs = []
runningJobs = []
CloudWidth = 280
rand = []


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

@socketio.on('disconnect')
def connect_handler():

    print "Client  from ", request.remote_addr, " disconnected with sid ", request.sid


@socketio.on('close')
def connect_handler():
    global rand
    rand = []


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





if __name__ == '__main__':

    socketio.run(app)

