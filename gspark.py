from flask import *
from flask_socketio import SocketIO
import json
import random
import time

app = Flask(__name__)
socketio = SocketIO(app)



historyJobs = []
runningJobs = []
CloudWidth = 280


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

    print "Client connected..."
    print dir(request)
    print request.remote_addr





@socketio.on('test')
def test_handler():
    index = random.randrange(8)
    socketio.emit(test_data[index]["event"], test_data[index]["data"])






def getJob(id):
    clu_list = json.load(open("C:\Users\jthun\Downloads\clusters%s.json" % id))
    job = Job(clouds=clu_list)
    return job


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
        {"event": "GS", "data": ["cluster2-SiteDriver1", "cluster3-SiteDriver2"]},
        {"event": "SE", "data": {"site":"cluster1-SiteDriver0", "executors": ["cluster1-executor1","cluster1-executor2","cluster1-executor4"]}},
        {"event": "SE", "data": {"site":"cluster2-SiteDriver1", "executors": ["cluster2-executor1","cluster2-executor2","cluster2-executor3"]}},
        {"event": "SE", "data": {"site":"cluster3-SiteDriver2", "executors": ["cluster3-executor1","cluster3-executor2","cluster3-executor4","cluster3-executor8","cluster3-executor9"]}},
        {"event": "EE", "data": {"exe":"cluster1-executor2", "executors": ["cluster1-executor0","cluster1-executor1","cluster1-executor3","cluster1-executor4","cluster1-executor5","cluster1-executor6"]}},
        {"event": "EE", "data": {"exe":"cluster2-executor3", "executors": ["cluster2-executor0","cluster2-executor1","cluster2-executor2","cluster2-executor4"]}},
        {"event": "EE", "data": {"exe":"cluster3-executor6", "executors": ["cluster3-executor0","cluster3-executor4","cluster3-executor8","cluster3-executor7"]}},
        ]





if __name__ == '__main__':
    socketio.run(app)
