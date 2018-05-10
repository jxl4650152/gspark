import threading
import json
import socket


# class used by new_thread to accumulate infos for repaly
class key_frame_info:
    def __init__(self):
        self.executors = []
        self.driver = []
        self.jobs = []
        self.stages = []
        self.appId = ''

    def add_executor(self, id, host, fin_num, total):
        tmp_exec = {"id": id, "host": host, "fin_num": fin_num, "total_num": total}
        self.executors.append(tmp_exec)

    def update_executor(self, id, host, f, t):
        for e in self.executors:
            if e["id"] == id and e["host"] == host:
                e["fin_num"] += f
                e["total_num"] += t
                break

    def add_driver(self, cloud, host):
        tmp_driver = {"cloud": cloud, "host": host}
        self.driver.append(tmp_driver)

    def add_job(self, id, sub_time, stagelist, stagenum, com_time='-', cost='-', status=None):
        tmp_job = {"jobid": id, "subtime": sub_time, "comtime": com_time, "timecost": cost, "stagelist": stagelist, "stagenum": stagenum, "status": [0, 0]}
        self.jobs.append(tmp_job)

    def update_job(self, id, key, value):
        for j in self.jobs:
            if j["jobid"] == id:
                j[key] = value
                break

    def add_stage(self, id, stagename, tasknum, input='-', output='-', sread='-', swrite='-', cost='-', status=None):
        tmp_stage = {"stageid": id, "input": input, "output": output, "sread":sread, "swrite":swrite, "timecost": cost, "stagename": stagename, "tasknum": tasknum, "status": [0, 0] }
        self.stages.append(tmp_stage)

    def update_stage(self, id, c, r, type="update"):
        for stage in self.stages:
            if stage["stageid"] == id:
                if type == "update":
                    stage["status"][0] += c
                    stage["status"][1] += r
                    print stage["stageid"], id, stage["status"], c, r
                else:
                    stage[c] = r

    def toJson(self):
        keyframe = {"drivers": self.driver, "executors": self.executors, "jobs": self.jobs, "stages": self.stages}
        return json.dumps(keyframe)


def update_key_frame(keyframe, event):
    kf = keyframe
    if event["Event"] == "SparkListenerBlockManagerAdded":
        if event["Block Manager ID"]["Executor ID"] == "driver":
            kf.add_driver("cloud-1", event["Block Manager ID"]["Host"])

    elif event["Event"] == "SparkListenerExecutorAdded":
        kf.add_executor(event["Executor ID"], event["Executor Info"]["Host"], 0, 0)

    elif event["Event"] == "SparkListenerJobStart":
        kf.add_job(event["Job ID"], event["Submission Time"], [s["Stage ID"] for s in event["Stage Infos"]], len(event["Stage Infos"]))

    elif event["Event"] == "SparkListenerStageSubmitted":
        kf.add_stage(event["Stage Info"]["Stage ID"], event["Stage Info"]["Stage Name"], event["Stage Info"]["Number of Tasks"])
        for job in kf.jobs:
            if event["Stage Info"]["Stage ID"] in job["stagelist"]:
                job["status"][1] += 1
                break

    elif event["Event"] == "SparkListenerTaskStart":
        kf.update_executor(event["Task Info"]["Executor ID"], event["Task Info"]["Host"], 0, 1)
        kf.update_stage(event["Stage ID"], 0, 1)

    elif event["Event"] == "SparkListenerTaskEnd":
        kf.update_executor(event["Task Info"]["Executor ID"], event["Task Info"]["Host"], 1, -1)
        kf.update_stage(event["Stage ID"], 1, -1)

    elif event["Event"] == "SparkListenerStageCompleted":
        sid = event["Stage Info"]["Stage ID"]
        kf.update_stage(sid, "timecost", event["Stage Info"]["Completion Time"] - event["Stage Info"]["Submission Time"], type="add")
        for acu in event["Stage Info"]["Accumulables"]:
            if acu["Name"] == "internal.metrics.shuffle.read.remoteBytesRead":
                kf.update_stage(sid, "sread", acu["Value"], type="add")
            elif acu["Name"] == "internal.metrics.output.bytesWritten":
                kf.update_stage(sid, "output", acu["Value"], type="add")
            elif acu["Name"] == "internal.metrics.input.bytesRead":
                kf.update_stage(sid, "input", acu["Value"], type="add")
            elif acu["Name"] == "internal.metrics.shuffle.write.bytesWritten":
                kf.update_stage(sid, "swrite", acu["Value"], type="add")
        for job in kf.jobs:
            if sid in job["stagelist"]:
                job["status"][0] += 1
                job["status"][1] -= 1
                break

    elif event["Event"] == "SparkListenerJobEnd":
        for job in kf.jobs:
            if job["jobid"] == event["Job ID"]:
                job["comtime"] = event["Completion Time"]
                job["cost"] = event["Completion Time"] - job["subtime"]
                break


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
        self.keyframe = key_frame_info()

    def run(self):
        appId = ''
        temp_ls = []
        count = 0
        s = ''
        end_flag = 0
        while True:
            data = self.conn.recv(1024)

            # connection closed
            if not data:
                print count
                break

            for c in data:
                s += c
                if c == "{":
                    end_flag += 1
                if c == "}":
                    end_flag -= 1
                    if end_flag == 0:
                        count += 1
                        if appId:
                            self.redis_conn.lpush(appId, s)
                            e = json.loads(s)
                            update_key_frame(self.keyframe, e)
                            if count % 20 == 0 and count != 0:
                                kf = self.keyframe.toJson()
                                print kf
                                self.redis_conn.lpush(self.keyframe.appId + "-k", kf)
                            s = ''
                            if e['Event'] == "SparkListenerApplicationEnd":
                                print "app end"
                                self.redis_conn.lrem("run_apps", 1, appId)
                                self.redis_conn.lpush("fin_apps", appId)
                                self.conn.close()
                                return
                        else:
                            print s
                            e = json.loads(s)
                            update_key_frame(self.keyframe, e)
                            if e['Event'] == "SparkListenerApplicationStart":
                                appId = e['App ID']
                                self.keyframe.appId = e['App ID']
                                self.redis_conn.lpush("run_apps", appId)
                                for temp_e in temp_ls:
                                    self.redis_conn.lpush(appId, temp_e)
                                self.redis_conn.lpush(appId, s)
                                s = ''

                            else:
                                temp_ls.append(s)
                                s = ''
