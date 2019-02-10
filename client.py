from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import sys
import time
import  pickle


messages = ['foo:$10',
            'bar:$30',
            'foo:$20',
            'bar:$20',
            'foo:$30',
            'bar:$10']


class Client(DatagramProtocol):
    def __init__(self,host:str,port:int):
        self.host = host
        self.port = port


    def startProtocol(self):
        self.transport.connect(self.host, self.port)
        print("Sending to primary server %s %d" % (self.host, self.port))
        self.sendMessages()

    # send all msgs to server one by one
    def sendMessages(self):
        i = 10
        for each in messages:
            li = each.split(':')
            eachMessage = {'key':li[0],'value' :li[1],'id':i}
            i = i+1
            data = pickle.dumps(eachMessage)
            self.transport.write(data)
            time.sleep(1)

    def datagramReceived(self, data, addr):
        data = data.decode()
        print("Received %r from Primary Server %s" % (data, addr))

    # Possibly invoked if there is no server listening on the
    # address to which we are sending.
    def connectionRefused(self):
        print("No one listening")

# 0 means any port, we don't care in this case

if __name__ == '__main__':
    # primary server details
    host = "127.0.0.1"
    port = 3000
    reactor.listenUDP(2000, Client(host,port))
    reactor.run()