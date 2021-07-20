import redis
from datetime import datetime
from faker import Faker
import random
import redisfunctions as red

fake = Faker()
Faker.seed(190)
random.seed(0)

r = redis.Redis(host='127.0.0.1', port=6379, db = 0)

#a functionality selector, to be used each time we end a function
def funsel():
    value = int(input("\n\n\nPlease enter your selected functionality:\n" +
                    "0-9: According function\n"+
                    "Press -1 to exit \n\n"+
                    "Proposed functions sequence: 0,1,3,4,6,7,8,9,2,5\n\n\n"))
    return value

value = funsel()
usercounter = 1
meetingcounter = 1
ordercounter = 1
#we run flushall to have a clean data import to an empty db, and proof of script's functionality
#we also avoid any issue with the counters, since we initialize them each time to 1
r.flushall()
sex = ['male','female']
#importing 10 users
for i in range(0,10):
    userid = "us"+str(usercounter)
    name = fake.name_nonbinary()
    age = random.randint(18,60)
    gender = random.choice(sex)
    email = fake.free_email()
    usercounter +=1
    red.usersetter(r, userid, name, age, gender, email)

#importing 4 meetings
for i in range(0,4):
    meetingid = "me"+str(meetingcounter)
    title = fake.text(max_nb_chars=20)
    description = fake.text(max_nb_chars=100)
    ispublic = random.randint(0,1)
    audience = ""
    if ispublic==1:
        for i in range (0,3):
            #we add three mmebers to the audience.
            #we ensure that the email is coming from a valid user

            randuser = "us"+str(random.randint(1,usercounter-1))
            randemail = red.usergetter(r, randuser, 'email')
            audience = audience + " " + randemail
    #change the ispublic parameter value to test the exceptions in the meetingsetter method
    meetingcounter += 1
    red.meetingsetter(r, meetingid, title, description, ispublic, audience)


#importing 6 meeting instances
for i in range(0,6):
    meetingid = "me"+str(random.randint(1,meetingcounter-1))
    orderid = "or"+str(ordercounter)
    #creating a starting datetime within the year
    fromdatetime = fake.date_time_this_year()
    #creating a random datetime after the starting datetime
    #We set ending date to isoformat, in order to be read as a string from the redis client
    todatetime = fake.date_time_between_dates(datetime_start=fromdatetime).isoformat()
    fromdatetime = fromdatetime.isoformat()
    red.misetter(r, meetingid, orderid, fromdatetime, todatetime)
    ordercounter+=1
r.set("eventcounter", 1)



while (value !=-1):
    if value ==0:
        #Uncomment the next line to get random meeting instance.
        #randmi = "or"+str(random.randint(1,ordercounter -1))
        randmi = "or"+str(3)
        red.activate(r, randmi)
        randmi = "or"+str(4)
        red.activate(r, randmi)
        print("Done\n")
        value = funsel()
    elif value ==1:
        #Based on the data created so far (replicable), the first example should work
        orderid = "or3"
        userid = "us2"
        red.join(r, orderid, userid)
        print("First join ending")
        #The following example should not work, since user us1 is not in an active instance
        orderid = "or3"
        userid = "us1"
        red.join(r, orderid, userid)
        print("Second join ending")
        value = funsel()

    elif value==2:
        #We are expecting the first attempt to succeed, and the second one to fail, since the user will no longer be joined
        print("First pass, expected to succeed\n")
        orderid = "or3"
        userid = "us2"
        red.leave(r, orderid, userid)
        print("Second pass, expected to throw error\n")
        orderid = "or3"
        userid = "us2"
        red.leave(r, orderid, userid)

        value = funsel()

    elif value==3:
        orderid = "or3"
        print("The joined members of meeting instance: "+ str(orderid))
        print(red.showjoined(r, orderid))

        value = funsel()

    elif value==4:
        print("The active meetings are the following: ")
        print(red.showactive(r))

        value = funsel()

    elif value==5:
        orderid = "or3"
        red.meetend(r, orderid)

        value = funsel()
    elif value ==6:
        for i in range(0,2):
            message = fake.text(max_nb_chars=20)
            userid = "us2"
            orderid = "or3"
            red.messagepost(r, orderid, userid, message)
        for i in range(0,2):
            message = fake.text(max_nb_chars=20)
            userid = "us4"
            orderid = "or3"
            red.messagepost(r, orderid, userid, message)

        value = funsel()

    elif value==7:
        orderid = "or3"
        red.mimessages(r, orderid)

        value = funsel()
    elif value==8:
        orderid = "or3"
        print(red.participantstime(r, orderid))

        value = funsel()
    elif value==9:
        orderid = "or3"
        userid = "us2"
        print(red.usermessages(r, orderid, userid))

        value = funsel()
