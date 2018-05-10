# -*- coding: UTF-8 -*-
from daemon_thread import daemon_thread
from eventlet.semaphore import Semaphore
from flask import *
from flask_socketio import *
import json
import redis.connection


# flask version: 0.12.2
# when to change the flask version, structure of this file
# should be adapted correspondingly


app = Flask("gspark")
socketio = SocketIO(app)

# Dict to maintain living web client info
conn_list = {}

try:
    r = redis.StrictRedis(host="192.168.7.41", port=6380)
    daemon = daemon_thread(r)
    daemon.daemon = True
    daemon.start()
except Exception as e:
    print "Exception occured when to connect redis:", e



# web route handlers
@app.route('/')
@app.route('/gspark/')
@app.route('/gspark/intro/')
def index():
    return render_template("introduction.html")


@app.route('/gspark/clusters/')
def clusters():
    return render_template("clusters.html")


@app.route('/gspark/applications/')
def applications():
    running_apps = r.lrange("run_apps", 0, -1)
    history_apps = r.lrange("fin_apps", 0, -1)
    print running_apps
    return render_template("applications.html", r_apps=running_apps, h_apps=history_apps)


@app.route('/gspark/jobs/runningjobs/<jobID>')
def runningjob(jobID):
    return render_template("job2.html", jobID=jobID)


@app.route('/gspark/jobs/historyjobs/<jobID>')
def historyjob(jobID):
    return render_template("job2.html", jobID=jobID)


# WebSocket route handlers
# for every web client, two coroutines(play_thread, replay_thread)
# and some data structures are created
@socketio.on('join')
def on_join(data):
    print "new client connecting"
    sema1 = Semaphore(0)
    sema2 = Semaphore(0)
    conn_list[request.sid] = {}
    conn_list[request.sid]["job"] = data["job"]
    conn_list[request.sid]["curr_p"] = 1
    conn_list[request.sid]["client_stop_flag"] = False
    conn_list[request.sid]["play_stop_flag"] = False
    conn_list[request.sid]["exit_flag"] = False
    conn_list[request.sid]["play_sema"] = sema1
    conn_list[request.sid]["replay_sema"] = sema2

    pl = socketio.start_background_task(play_thread, request.sid)
    repl = socketio.start_background_task(replay_thread, request.sid)
    conn_list[request.sid]["pl"] = pl
    conn_list[request.sid]["repl"] = repl




@socketio.on('replay')
def replay_handler(data):
    conn_list[request.sid]["new_p"] = int(data["point"])
    conn_list[request.sid]["play_stop_flag"] = True


@socketio.on('stopped')
def stopped_handler(data):
    conn_list[request.sid]["client_stop_flag"] = True
    conn_list[request.sid]["curr_p"] = int(data)


# replay start event
@socketio.on('clearstopped')
def clearstopped_handler():
    print "recieve stop clear event"
    conn_list[request.sid]["client_stop_flag"] = False


@socketio.on('disconnect')
def connect_handler():

    if request.sid in conn_list.keys():
        conn_list[request.sid]["exit_flag"] = True
        socketio.sleep(0)
        conn_list[request.sid]["pl"].join()
        conn_list[request.sid]["repl"].join()
        socketio.sleep(0)
        del conn_list[request.sid]
        print "client leaving..."


@socketio.on('close')
def connect_handler():
    print "close"


# deprecated api, not used
@socketio.on('ToSiteDriver')
def test_handler():
    #print request.sid
    event = json.load(open("/home/LAB/jiaxl/gspark/data-exch/task_launch_to_siteDriver.json"))
    res = eventConvert(event)
    socketio.emit(res["event"], res["data"], room=request.sid)


# deprecated api, not used
@socketio.on('OuterShuffle')
def test_handler():
    #print request.sid
    event = json.load(open("/home/LAB/jiaxl/gspark/data-exch/outter_shuffle.json"))
    res = eventConvert(event)
    socketio.emit(res["event"], res["data"], room=request.sid)


# deprecated api, not used
@socketio.on('ToExecutor')
def test_handler():
    #print request.sid
    event = json.load(open("/home/LAB/jiaxl/gspark/data-exch/task_launch_to_executor.json"))
    res = eventConvert(event)
    socketio.emit(res["event"], res["data"], room=request.sid)


# deprecated api, not used
@socketio.on('InnerShuffle')
def test_handler():
    #print request.sid
    event = json.load(open("/home/LAB/jiaxl/gspark/data-exch/inner_shuffle.json"))
    res = eventConvert(event)
    socketio.emit(res["event"], res["data"], room=request.sid)


# deprecated api, not used
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




# coroutine(not thread) to handle web play
def play_thread(req_id):
    job = conn_list[req_id]["job"]
    i = conn_list[req_id]["curr_p"]
    listlen = r.llen(job)
    initialevent = '{"Event": "init", "data": %s}'% listlen
    initialres = '{"time": "0", "data": %s}' % initialevent
    socketio.emit("play", initialres, room=req_id)

    while True:
        socketio.sleep(0)
        if conn_list[req_id]["exit_flag"]:
            conn_list[req_id]["replay_sema"].release()
            print "play thread exit"
            break
        elif conn_list[req_id]["play_stop_flag"]:
            conn_list[req_id]["curr_p"] = i - 1
            conn_list[req_id]["replay_sema"].release()
            conn_list[req_id]["play_sema"].acquire()
            i = conn_list[req_id]["curr_p"]
        elif conn_list[req_id]["client_stop_flag"]:
            print "play thread stop at", conn_list[req_id]["curr_p"]
            i = conn_list[req_id]["curr_p"] + 1
            socketio.sleep(0.01)
        else:
            msg = r.lindex(job, -i)
            if not msg:
                socketio.sleep(2)
                print "null msg at ", i
                continue
            res = '{"time": "%s", "data": %s}' % (i, msg)
            print "play", i
            socketio.emit("play", res, room=req_id)
            i = i + 1
            socketio.sleep(0.07)


# coroutine(not thread) to handle event replay
def replay_thread(req_id):
    while True:
        print "Replay thread waiting..."
        socketio.sleep(0)
        conn_list[req_id]["replay_sema"].acquire()

        if conn_list[req_id]["exit_flag"]:
            conn_list[req_id]["play_sema"].release()
            print "replay thread exit"
            return
        else:
            job = conn_list[req_id]["job"]
            i = conn_list[req_id]["curr_p"]
            i_new = conn_list[req_id]["new_p"]
            if i_new > i:
                if i_new / 20 != i / 20:
                    socketio.emit("clear", room=req_id)
                    socketio.sleep(0)
                    keymsg = r.lindex(job + "-k", -(i_new / 20))
                    socketio.emit("keyframe", keymsg, room=req_id)
                    print "keyframe at", i_new/20
                    tmp_p = (i_new / 20) * 20 + 1
                    while tmp_p < i_new + 1:
                        print "make up", tmp_p, i_new
                        msg = r.lindex(job, -tmp_p)
                        socketio.emit("replay", msg, room=req_id)
                        tmp_p += 1
                        socketio.sleep(0)
                else:
                    s = i + 1
                    while s < i_new + 1:
                        msg = r.lindex(job, -s)
                        socketio.emit("replay", msg, room=req_id)
                        s = s + 1
                        socketio.sleep(0)
            elif i_new < i:
                socketio.emit("clear", room=req_id)
                socketio.sleep(0)
                keymsg = r.lindex(job + "-k", -(i_new / 20))
                socketio.emit("keyframe", keymsg, room=req_id)
                tmp_p = (i_new / 20) * 20 + 1
                while tmp_p < i_new + 1:
                    msg = r.lindex(job, -tmp_p)
                    socketio.emit("replay", msg, room=req_id)
                    tmp_p += 1
                    socketio.sleep(0)

            conn_list[req_id]["curr_p"] = i_new + 1
            socketio.sleep(0)
            conn_list[req_id]["play_stop_flag"] = False
            conn_list[req_id]["play_sema"].release()


if __name__ == "__main__":
    app.run("0.0.0.0", 2000)



