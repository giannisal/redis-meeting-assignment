

import redis
from datetime import datetime
from faker import Faker
import time

fake = Faker()

#The function will be used to decode bytestrings, to regular bytestrings
#The applied methid changes both keys and values simultaneously
def bytedictdecoder(bytedict):
    #for i in bytedict:
    #    bytedict[i] = bytedict[i].decode('utf-8')
    stringdict  = { y.decode('utf-8'): bytedict.get(y).decode('utf-8') for y in bytedict.keys() }
    return stringdict
def bytedecoder(bytestring):
    normstring = bytestring.decode('utf-8')
    return normstring

def bytelistdecoder(bytelist):
    normlist = []
    for i in bytelist:
        normlist.append(i.decode('utf-8'))
    return normlist


def usersetter(r, userid, name, age, gender, email):
#  r = redis.Redis(host='127.0.0.1', port=6379, db = 0)
  usdict = {"userid": userid, "name": name, "age": age, "gender": gender, "email": email}
  usname = "user:"+str(userid)
  r.hmset(usname, usdict)

def usergetter(r, userid, field="all"):
    fieldlist = ['name', 'age', 'gender', 'email']
    usname = "user:"+str(userid)
    if field=="all":
        result = bytedictdecoder(r.hgetall(usname))
    elif (field in fieldlist):
        result = bytedecoder(r.hget(usname, field))
    else:
        result = "You need to choose a valid user field (name, age, gender, email), or all. Please try again."
    return result


def meetingsetter(r, meetingid, title, description, ispublic, audience = None):
#  r = redis.Redis(host='127.0.0.1', port=6379, db = 0)
  meetingdict = {"meetingid": meetingid, "title": title, "description": description, "ispublic": ispublic, "audience": audience}
  #We set value 0 as public meetings, and value 1 as non-ispublic
  if ((ispublic==0)&(audience!=None)):
      print("You cannot set a public meeting with specific audience list")
  elif ((ispublic==1)&(audience==None)):
      print("You need to provide an audience list for non-public meetings")
  else:
      meetingname = "meeting:"+str(meetingid)
      r.hmset(meetingname, meetingdict)

def meetinggetter(r, meetingid, field="all"):
    fieldlist = ['title', 'description', 'ispublic', 'audience']
    meetingname = "meeting:"+str(meetingid)
    if field=="all":
        result = bytedictdecoder(r.hgetall(meetingname))
    elif (field in fieldlist):
        result = bytedecoder(r.hget(meetingname, field))
    else:
        result = "You need to choose a valid user field (name, age, gender, email), or all. Please try again."
    return result

#Since more than one instances exist for the same meeting, but orderid is unique, this is the one that will be included in the mi name
def misetter(r, meetingid, orderid, fromdatetime, todatetime):
#  r = redis.Redis(host='127.0.0.1', port=6379, db = 0)
  midict = {"meetingid": meetingid, "orderid": orderid, "fromdatetime": fromdatetime, "todatetime": todatetime}
  miname = "mi:"+str(orderid)
  r.hmset(miname, midict)

def migetter(r, orderid, field="all"):
    fieldlist = ['meetingid', 'orderid', 'fromdatetime', 'todatetime']
    miname = "mi:"+str(orderid)
    if field=="all":
        result = bytedictdecoder(r.hgetall(miname))
    elif (field in fieldlist):
        result = bytedecoder(r.hget(miname, field))
    else:
        result = "You need to choose a valid user field (name, age, gender, email), or all. Please try again."
    return result


#There is no use in manu"all"y inserting data to eventlog, they should be auto-generated. So a setter is ommited

def eventsetter(r, eventid, userid, eventtype):
      eventdict = {"eventid": eventid, "userid": userid, "eventtype": eventtype, "timestamp": time.time()}
      eventname = "event:"+str(eventid)
      r.hmset(eventname, eventdict)

def eventgetter(r, eventid, field="all"):
    fieldlist = ['userid', 'eventtype', 'timestamp']
    eventname = "event:"+str(userid)
    if field=="all":
        result = bytedictdecoder(r.hgetall(eventname))
    elif (field in fieldlist):
        result = bytedecoder(r.hget(eventname, field))
    else:
        result = "You need to choose a valid user field (name, age, gender, email), or all. Please try again."
    return result



#We begin implementing the requsted functions


#Activate a given meeting instance
def activate(r, orderid):
    #check if meeting instance actually exists
    miname = "mi:"+str(orderid)
    if (r.exists(miname)):
        r.sadd("active", miname)
        print("Meeting activated")
    else:
        print("Requested instance does not exist")

def join(r, orderid, userid):
    username = "user:"+str(userid)
    miname = "mi:"+str(orderid)
    #We get some of our data using the redis object, before editing values within the pipeline
    #An alternative way would be to use the watch function, within the pipeline
    #That way, until we use the multi function, we don't execute atomically
    isamember = r.sismember("active", miname)
    userexists = r.exists(username)
    meetingid = migetter(r, orderid,"meetingid")
    #get the privacy of the meeting
    priv = meetinggetter(r, meetingid, "ispublic")
    belong = 0
    useremail = usergetter(r, userid, "email")
    #Checking that the meeting is active (& exists) and the user exists
    if (isamember & userexists):
        if int(priv)==1:
            if (useremail in meetinggetter(r, meetingid, "audience").split(" ")):
                belong = 1
        if ((belong == 1) | (priv == 0)):
            with r.pipeline() as pipe:
                memberlist = str(orderid)+":joined"
                membertime = str(orderid)+":time:joined"
                pipe.sadd(memberlist, userid)
                mymap = {userid : time.time()}
                pipe.zadd(membertime,mymap)
                eventid = "eventlog:"+str(r.get("eventcounter"))
                eventsetter(pipe,eventid, userid, "joined")
                pipe.incr("eventcounter")
                pipe.execute()
            print("Query executed successfully\n\n")
        else:
            print("Error: The user is not a member of the meeting audience\n")
    else:
        print("Error: Your meeting instance is not active, or you are not logged in as a valid user\n")

def leave(r, orderid, userid):
    #checking for active meeting instance and joined user
    miname = "mi:"+str(orderid)
    isamember = r.sismember("active", miname)
    hasjoined = r.sismember(orderid+":joined", userid)
    eventcounter = r.get("eventcounter")
    if(isamember & hasjoined):
        with r.pipeline() as pipe:
            memberlist = str(orderid)+":joined"
            membertimed = str(orderid)+":time:joined"
            pipe.srem(memberlist, userid)
            pipe.zrem(membertimed, userid)
            eventid = "eventlog:"+bytedecoder(eventcounter)
            eventsetter(pipe,eventid, userid, "left")
            pipe.incr("eventcounter")
            pipe.execute()
        print("Query executed successfully")
    else:
        print("Error: The meeting is not active, or the user has not joined")

def showjoined(r, orderid):
    myset = r.smembers(orderid+":joined")
    myset = list(myset)
    myset = bytelistdecoder(myset)
    return myset



def showactive (r):
    members = r.smembers("active")
    members = bytelistdecoder(list(members))
    return members

def meetend(r, orderid):
    isamember = r.smembers("active")
    isamember = list(isamember)
    isamember = bytelistdecoder(isamember)
    eventcounter = r.get("eventcounter")
    miname = "mi:"+str(orderid)
    if (miname in isamember):
        joinedlist = r.smembers(orderid+":joined")
        miname = "mi:"+orderid
        with r.pipeline() as pipe:
            for i in joinedlist:
                memberlist = str(orderid)+":joined"
                pipe.srem(memberlist, i)

                eventid = "eventlog:"+bytedecoder(eventcounter)
                eventsetter(pipe,eventid, i, "left")
                pipe.incr("eventcounter")
                pipe.delete(miname)
                pipe.execute()
                print("users removed, and meeting ended successfully")
    else:
        print("The meeting is not active, or the user has not joined")


def messagepost(r, orderid, userid, message):
    isamember = showactive(r)
    miname = "mi:"+str(orderid)
    if(miname in isamember):
        joinedlist = showjoined(r, orderid)
        if(userid in joinedlist):
            eventcounter = r.get("eventcounter")
            eventid = "eventlog:"+bytedecoder(eventcounter)
            with r.pipeline() as pipe:
                totalrecord = orderid+":messages"
                userrecord = orderid+":"+userid+":messages"
                pipe.rpush(totalrecord, message)
                pipe.rpush(userrecord, message)
                eventid = "eventlog:"+bytedecoder(eventcounter)
                eventsetter(pipe, eventid, userid, "posted")
                pipe.incr("eventcounter")
                pipe.execute()
                print("user: "+str(userid)+"sent a message: " +message)

    else:
        print("The user is not an active member of the meeting")
def mimessages(r, orderid):
#   Assumes active meeting
    messagesid = orderid+":messages"
    mimessages = r.lrange(messagesid, 0, -1)
    for i in mimessages:
        print(bytedecoder(i))

def participantstime(r, orderid):
    miname = str(orderid)+":time:joined"
    return r.zrange(miname, 0, -1, withscores = True)


def usermessages(r, orderid, userid):
# Assumes active meeting, but checks for joined userid
    joinedlist = showjoined(r, orderid)
    if userid in joinedlist:
        usermessagesid = orderid+":"+userid+":messages"
        messagelist = r.lrange(usermessagesid, 0, -1)
        messagelist = bytelistdecoder(messagelist)
        result = "user: "+ userid+ " has sent the below messages. \n\n"+str(messagelist)
        return result
