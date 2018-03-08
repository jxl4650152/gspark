from flask import *

import json

app = Flask(__name__)




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
    historyJobs = [Job(1, "first"), Job(2, "second")]
    runningJobs = [Job(1, "first"), Job(2, "second")]
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







if __name__ == '__main__':
    app.run()
