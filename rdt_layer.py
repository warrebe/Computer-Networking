# Author: Benjamin Warren
# Date: 02/26/2022
# Description: Creating a reliable data transfer algorithm/protocol for communication between a client and server

# Citation for the following program:
      # Date: 02/25/2022
      # Based on: Epic Networks Lab on Youtube (Summary of textbook section 3.4)
      # Source URL: https://www.youtube.com/watch?v=j93DZaMMjfg&t=683s


from segment import Segment


# #################################################################################################################### #
# RDTLayer                                                                                                             #
#                                                                                                                      #
# Description:                                                                                                         #
# The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable         #
# channel.                                                                                                             #
#                                                                                                                      #
#                                                                                                                      #
# Notes:                                                                                                               #
# This file is meant to be changed.                                                                                    #
#                                                                                                                      #
#                                                                                                                      #
# #################################################################################################################### #


class RDTLayer(object):
    # ################################################################################################################ #
    # Class Scope Variables                                                                                            #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    DATA_LENGTH = 4 # in characters                     # The length of the string data that will be sent per packet...
    FLOW_CONTROL_WIN_SIZE = 15 # in characters          # Receive window size for flow-control
    sendChannel = None
    receiveChannel = None
    dataToSend = ''
    currentIteration = 0                                # Use this for segment 'timeouts'
    # Add items as needed

    # ################################################################################################################ #
    # __init__()                                                                                                       #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def __init__(self, host):
        self.clientServer = host
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0
        self.dataReceived = []
        if host == 1:
            self.lastSeqReceived = 0
        else:
            self.lastSeqReceived = 1
        self.wait = False
        self.allReceived =False
        self.countSegmentTimeouts = 0
        self.segmentsSent = []
        self.lastSeqSent = 1
        self.timeout = 0
        # Add items as needed

    # ################################################################################################################ #
    # setSendChannel()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable sending lower-layer channel                                                 #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setSendChannel(self, channel):
        self.sendChannel = channel

    # ################################################################################################################ #
    # setReceiveChannel()                                                                                              #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable receiving lower-layer channel                                               #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setReceiveChannel(self, channel):
        self.receiveChannel = channel

    # ################################################################################################################ #
    # setDataToSend()                                                                                                  #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the string data to send                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setDataToSend(self,data):
        self.dataToSend = data

    # ################################################################################################################ #
    # getDataReceived()                                                                                                #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to get the currently received and buffered string data, in order                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def getDataReceived(self):
        # ############################################################################################################ #
        # Identify the data that has been received...
        dataToPrint = ""
        for seg in self.dataReceived:
           dataToPrint += seg.payload

        # ############################################################################################################ #
        return dataToPrint

    # ################################################################################################################ #
    # processData()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # "timeslice". Called by main once per iteration                                                                   #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processData(self):
        self.currentIteration += 1
        if not self.allReceived:
            if self.clientServer == 0:
                self.processReceiveAndSendRespond()
                self.processSend()
            if self.clientServer == 1:
                self.processReceiveAndSendRespond()

    # ################################################################################################################ #
    # processSend()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment sending tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processSend(self):
        if not self.wait:
            if self.clientServer == 0:
                self.lastSeqSent = self.lastSeqReceived
                while len(self.sendChannel.sendQueue) * self.DATA_LENGTH < self.FLOW_CONTROL_WIN_SIZE:
                    segmentSend = Segment()

                    # ############################################################################################################ #

                    # You should pipeline segments to fit the flow-control window
                    # The flow-control window is the constant RDTLayer.FLOW_CONTROL_WIN_SIZE
                    # The maximum data that you can send in a segment is RDTLayer.DATA_LENGTH
                    # These constants are given in # characters

                    # Somewhere in here you will be creating data segments to send.
                    # The data is just part of the entire string that you are trying to send.
                    # The seqnum is the sequence number for the segment (in character number, not bytes)


                    seqnum = self.lastSeqReceived
                    data = self.dataToSend[(seqnum - 1) * self.DATA_LENGTH:(seqnum * self.DATA_LENGTH)]
                    if data == "":
                        return


                    # ############################################################################################################ #
                    # Display sending segment
                    segmentSend.setData(seqnum,data)
                    self.lastSeqReceived += 1

                    # Use the unreliable sendChannel to send the segment
                    self.sendChannel.send(segmentSend)
                    self.wait = True

    # ################################################################################################################ #
    # processReceive()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment receive tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processReceiveAndSendRespond(self):
        if not self.receiveChannel.receiveQueue:
            if self.currentIteration != 1:
                segmentAck = Segment()
                segmentAck.setAck(0)
                self.sendChannel.send(segmentAck)
            return
        else:
            self.wait = False
        if self.clientServer == 0:
            segmentAck = Segment()                  # Segment acknowledging packet(s) received

            # This call returns a list of incoming segments (see Segment class)...
            listIncomingSegments = self.receiveChannel.receive()

            # ############################################################################################################ #
            # What segments have been received?
            # How will you get them back in order?
            # This is where a majority of your logic will be implemented
            for seg in listIncomingSegments:
                if seg.acknum == -1:
                    self.lastSeqReceived = seg.seqnum
                    return
                else:
                    acknum = 1

            # Display response segment
            segmentAck.setAck(acknum)

            # Use the unreliable sendChannel to send the ack packet
            self.sendChannel.send(segmentAck)
        if self.clientServer == 1:
            segmentAck = Segment()                  # Segment acknowledging packet(s) received

            # This call returns a list of incoming segments (see Segment class)...
            listIncomingSegments = self.receiveChannel.receive()
            if len(listIncomingSegments) == 1:
                if listIncomingSegments[0].acknum == 1:
                    segmentAck = Segment()
                    segmentAck.setData(self.lastSeqReceived, "")
                    self.sendChannel.send(segmentAck)
                    return

            # ############################################################################################################ #
            # What segments have been received?
            # How will you get them back in order?
            # This is where a majority of your logic will be implemented
            listIncomingSegments.sort(key=lambda x: x.seqnum)

            # ############################################################################################################ #
            # How do you respond to what you have received?
            # How can you tell data segments apart from ack segemnts?
            problem = []
            for seg in listIncomingSegments:
                if seg.acknum == 0:
                    self.sendChannel.send(self.lastSeqSent)
                    return
                elif seg.acknum == -1:
                    if seg.checksum == seg.calc_checksum(seg.to_string()):
                        if seg.seqnum != self.lastSeqReceived + 1:
                            segmentAck.setData(self.lastSeqReceived + 1, "")  # NACK
                            self.lastSeqSent = segmentAck
                            self.sendChannel.send(segmentAck)
                            return
                        else:
                            self.dataReceived.append(seg)
                            self.lastSeqReceived += 1
                    else:
                        problem.append(seg)
                        break

            # Somewhere in here you will be setting the contents of the ack segments to send.
            # The goal is to employ cumulative ack, just like TCP does...
            if not problem:
                acknum = 1
                # ######################################################################################################
                # Display response segment
                segmentAck.setAck(acknum)
            else:
                segmentAck.setData(problem[0].seqnum, "") #NACK

            # Use the unreliable sendChannel to send the ack packet
            self.lastSeqSent = segmentAck
            self.sendChannel.send(segmentAck)

