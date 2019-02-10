import socket
import json
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import sys
import pickledb
import pickle

servers = [3000 ,3001,3002,3003]
host = '127.0.0.1'

def serverStart(host:str,port:int):

    # Create new socket that will be passed to reactor later.
    portSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Make the port non-blocking and start it listening.
    portSocket.setblocking(False)
    portSocket.bind((host, port))
    reactor.adoptDatagramPort(portSocket.fileno(), socket.AF_INET, Server( int(port)))

class Server(DatagramProtocol):

    def __init__(self,port:int):
        self.port = port
        self.db = pickledb.load('./assignment3_{' + str(port) + '}.db', True)
        self.db.deldb()
        self.resetState()
        self.quorum = len(servers) - 3
        self.processedId = {}


    def resetState(self):

        self.dict1 = {}
        self.Adict = {}
        if (self.port == 3000):
            self.state = "REC"
        else:
            self.state = "WAIT"
        self.dict1['dict2'] = {}
        self.Adict['Adict2'] = {}
        self.flag = False
        self.flag1 = False
        self.key = None



    def datagramReceived(self, data, addr):

        # Main Logic
        #  In Rec --> only 3000 will send to others
        dataDict = pickle.loads(data)
        # print("received %r from Client %s" % (dataDict, addr))
        if dataDict['id'] in self.processedId:
            return
            #print("Ignore already processed")
        elif self.state == "REC" and str(addr[1])==str(2000):
            self.key = dataDict
            print("received %r from Client %s" % (dataDict, addr))
            self.state = "VOTING"
            # % s % d" % (self.host, self.port))
            self.sendNeighbors(data)
        elif(self.state == "WAIT" and str(addr[1])==str(3000)):
            print("received %r from %s" % (dataDict, addr))
            self.state = "VOTING"
            self.key = dataDict
            print("Current state : %s , %r from %s" % (self.state, dataDict,addr))
            self.storeHash(data, addr)
            self.sendNeighbors(data)
        elif(self.state == "VOTING" and self.flag==False and dataDict == self.key):
            print("Current state : %s , %r from %s" % (self.state,dataDict, addr))
            self.storeHash(data, addr)
            self.ratify(data)
        elif(self.state == "ACCEPT" and self.flag1==False and dataDict == self.key):
            print("Current state : %s , %r from %s" % (self.state,dataDict, addr))
            self.storeHash2(data,addr)
            self.ratify2(data)
        else:
            print()
            #print("Extra Message Ignore received %r from Client %s" % (dataDict, addr))


    # Send msgs to all its neighbours except self
    def sendNeighbors(self,data:str):
        for each in servers:
            if each != self.port:
                self.transport.write(data,(host,each))

    # store the VOTING msges
    def storeHash(self,data,addr):
        #  key : data ; value contains hashmap (key: server ;value = count)
        if data in self.dict1:
            if addr in self.dict1[data]:
                self.dict1[data][addr] = self.dict1[data][addr] + 1
            else:
                self.dict1[data][addr] = 1
        else:
            self.dict1[data] = {}
            self.dict1[data][addr]=1
        # print("dict count "+str(len(self.dict1[data])))

    # store the ACCEPT msges
    def storeHash2(self,data,addr):
        #  key : data ; value contains hashmap (key: server ;value = count)
        if data in self.Adict:
            if addr in self.Adict[data]:
                self.Adict[data][addr] = self.Adict[data][addr] + 1
            else:
                self.Adict[data][addr] = 1
        else:
            self.Adict[data] = {}
            self.Adict[data][addr]=1
        # print("dict count "+str(len(self.Adict[data])))

    # check majority VOTING msgs
    def ratify(self,data):
        if(len(self.dict1[data]) > self.quorum and self.flag == False):
            self.flag = True
            self.state = "ACCEPT"
            self.sendNeighbors(data)

    # check majority ACCEPT msgs
    def ratify2(self,data):
        if(len(self.Adict[data]) > self.quorum and self.flag1 == False):
            self.flag1 = True
            self.state = "CONFIRM"
            self.confirm(data)


    # set msg to db
    def confirm(self,data):
        # list = data.split(':')
        print("Current State : CONFIRM")
        dataDict = pickle.loads(data)
        name = dataDict['key']
        amountold = dataDict['value']
        amountLi = amountold.split('$')
        amount = int(amountLi[1])
        # if present , update it by adding prevamount and new amount and setting to db
        if(self.db.exists(name)):
            prevAmount = self.db.get(name)
            prevamountLi = prevAmount.split('$')
            preamount = int(prevamountLi[1])
            newAmount =  preamount + amount
            newAmount1 = '$'+str(newAmount)
            self.db.rem(name)
            self.db.set(name, newAmount1)

        else:
            # if not present in db make a new entry
            amountNew = '$'+str(amount)
            self.db.set(name,amountNew)

        self.processedId[dataDict['id']]=dataDict['id']
        # print all DB entries
        list = self.db.getall()
        print("DB ENTRY ")
        for each in list:
            print(each + " : " + self.db.get(each))
        print(" ================= ")

        self.sendReplyClient()

    def sendReplyClient(self):
        if(self.port == 3000):
            self.transport.write(("Reply message").encode(), (host, 2000))
        self.db.dump()
        self.resetState()




if __name__ == '__main__':
    port = sys.argv[1]
    serverStart(host,int(port))
    reactor.run()
    print("finished")