import argparse
from collections import deque

parser = argparse.ArgumentParser(description='Message Generator', fromfile_prefix_chars='@')
parser.add_argument("stock_file", help="List stock symbol for the Matching Engine")
parser.add_argument("event_file", help="List of messages with time to send to Matching Engine")
parser.add_argument("model_seq_file", help="Model sequence of message type to send to Matching Engine")
parser.add_argument("output_test_file", help="List of messages to send to Matching Engine")
parser.add_argument("output_seeded_file", help="List of seeded orders messages to send to Matching Engine")

args = parser.parse_args()

###################################################################
# Data Definition section
###################################################################
#
# MAIN_INSTRUMENT Table 
class Stock(object):
    def __init__(self, stock_number=None, symbol=None, instrument_id=None):
        self.stock_number = int(stock_number)
        self.symbol = symbol
        self.instrument_id = instrument_id
#
# Template of Message definition groups
class GroupOrderAttr(object):
    def __init__(self, group_code=None, trading_account=None, msg_type=None, price=None, time_in_force=None):
        self.group_code = group_code
        self.trading_account = trading_account
        self.msg_type = msg_type
        self.price = price
        self.time_in_force = time_in_force
#
# Event of messages to send to ME
class Event(object):
    def __init__(self, seq=None, mtime=None, stock_number=None):
        self.seq = int(seq)
        self.mtime = int(mtime)
        self.stock_number = int(stock_number)

#
# Test Messages that can be used for sending to ME and seeded orders 
class Message(GroupOrderAttr):
    def __init__(self, type=None, sender=None, seq=None, mtime=None, side=None, qty=None, stock=None, group_attr=None):
        super().__init__(group_attr.group_code, group_attr.trading_account, group_attr.msg_type, group_attr.price, group_attr.time_in_force)
        self.sender = sender
        self.seq = abs(seq)
        self.mtime = mtime
        self.side = side
        self.qty = qty
        self.stock = stock
        if ( self.side == 'B' ):
            if ( self.price == "on_nbbo" ):
                self.real_price = "50.00"
            elif ( self.price == "behine_nbbo"):
                self.real_price = "49.99"
            else:
                self.real_price = "50.01"
        else:
            if ( self.price == "on_nbbo" ):
                self.real_price = "50.01"
            elif ( self.price == "behine_nbbo"):
                self.real_price = "50.02"
            else:
                self.real_price = "50.00"
        if (type == "seeded"):
            self.clordid = str(self.sender) + ":" + self.side + ":" + self.trading_account + ":Seed:" + str(self.seq).zfill(8)
        elif (type == "test"):
            self.clordid = str(self.sender) + ":" + self.side + ":" + self.trading_account + ":" + str(self.seq).zfill(8)
        else:
            self.clordid = str(self.sender) + ":" + self.side + ":" + self.trading_account + ":Match:" + str(abs(self.seq)).zfill(8)

        self.orig_clordid = None

    def SetOrigClOrdId(self, orig_clordid ):
        self.orig_clordid = orig_clordid
        
    def GetRestingLookupKey(self):
        if ( self.side == 'B' and self.price == "marketable"):
            return self.trading_account + ":" + self.side + ":" + self.stock.symbol + ":" + "50.00"
        elif ( self.side == 'S' and self.price == "marketable"):
            return self.trading_account + ":" + self.side + ":" + self.stock.symbol + ":" + "50.01"
        else:       
            return self.trading_account + ":" + self.side + ":" + self.stock.symbol + ":" + self.real_price

    def CreateSeededOrder(self):
        seeded_order = None
        if (self.msg_type == "cxl" ):
            if ( self.price == "on_nbbo"):
                seeded_order = Message("seeded", 1, self.seq * -1, 0, self.side, 100, self.stock, groups['A'])            
            elif ( self.price == "behine_nbbo"):           
                seeded_order = Message("seeded", 1, self.seq * -1, 0, self.side, 100, self.stock, groups['B'])            
            self.orig_clordid = seeded_order.clordid
        elif (self.msg_type == "cxlrpl" ):
             #
             # We will change qty from 100 to 200
            if ( self.price == "on_nbbo"):
                seeded_order = Message("seeded", 1, self.seq * -1, 0, self.side, 200, self.stock, groups['A'])            
            elif ( self.price == "behine_nbbo"):           
                seeded_order = Message("seeded", 1, self.seq * -1, 0, self.side, 200, self.stock, groups['B'])
            else:
                seeded_order = Message("seeded", 1, self.seq * -1, 0, self.side, 100, self.stock, groups['A'])
            self.orig_clordid = seeded_order.clordid
        return seeded_order

    def GetContraSide(self):
        if ( self.side == 'B' ):
            return 'S'
        else:
            return 'B'
        
        
###################################################################
# Global Data Section
###################################################################
stocks = {} #stock table to translate stock number to stock symbol
model_seq = {} #Modeling Sequence of messages
groups = {} #List of message group templates

events_list = [] # list of event that will be sent to ME
test_messages = [] #List of actual test messages to ME by time

chx_book = {}   # key is Trading account + side + symbol + price
                # the value is list of clordid

seeded_orders = [] # keep the list of seeded orders
resting_match_orders = {} #List of resting order for mmarketable order

###################################################################
# Functions Section
###################################################################
def LoadStockTable():
    try:
        stock_file = open(args.stock_file, "r")
    except IOError:
        print( args.stock_file, " File does not exist")

    for line in stock_file:
       (key, symbol, instrument_id) = line.split(",")
       stocks[int(key)] = Stock( int(key), symbol, int(instrument_id) )

    stock_file.close() 

    print("total stock: ", len(stocks) )
    # for i in stocks:
    #     print(i, "stock:", stocks[i].symbol, " instrument id: ", stocks[i].instrument_id )

def LoadModelSeq():
    try:
        model_seq_file = open( args.model_seq_file, "r")
    except IOError:
        print( args.model_seq_file, " File does not exist")
    for line in model_seq_file:
       (seq, code) = line.split(",")
       model_seq[int(seq.lstrip())] = code.rstrip().lstrip()

    model_seq_file.close()
    print("total modeling sequence: ", len(model_seq) )
    #for i in model_seq:
    #    print(i, "group code:", model_seq[i])

def LoadGroupTemplate():
    groups['A'] = GroupOrderAttr("A", "LMM", "new", "behine_nbbo", "DAY" ) 
    groups['B'] = GroupOrderAttr("B", "LMM", "new", "on_nbbo", "DAY" ) 
    groups['C'] = GroupOrderAttr("C", "LMM", "new", "marketable", "DAY" ) 

    groups['D'] = GroupOrderAttr("D", "LMM", "cxl", "behine_nbbo", "DAY" ) 
    groups['E'] = GroupOrderAttr("E", "LMM", "cxl", "on_nbbo", "DAY" ) 

    groups['F'] = GroupOrderAttr("F", "LMM", "cxlrpl", "behine_nbbo", "DAY" ) 
    groups['G'] = GroupOrderAttr("G", "LMM", "cxlrpl", "on_nbbo", "DAY" ) 
    groups['H'] = GroupOrderAttr("H", "LMM", "cxlrpl", "marketable", "DAY" ) 

    groups['I'] = GroupOrderAttr("I", "DAST", "new", "behine_nbbo", "DAY" ) 
    groups['J'] = GroupOrderAttr("J", "DAST", "new", "on_nbbo", "DAY" ) 
    groups['K'] = GroupOrderAttr("K", "DAST", "new", "marketable", "DAY" ) 

    groups['L'] = GroupOrderAttr("L", "DAST", "new", "behine_nbbo", "IOC" ) 
    groups['M'] = GroupOrderAttr("M", "DAST", "new", "on_nbbo", "IOC" ) 
    groups['N'] = GroupOrderAttr("N", "DAST", "new", "marketable", "IOC" ) 

    groups['O'] = GroupOrderAttr("D", "DAST", "cxl", "behine_nbbo", "DAY" ) 
    groups['P'] = GroupOrderAttr("E", "DAST", "cxl", "on_nbbo", "DAY" ) 

    groups['Q'] = GroupOrderAttr("Q", "DAST", "cxlrpl", "behine_nbbo", "DAY" ) 
    groups['R'] = GroupOrderAttr("R", "DAST", "cxlrpl", "on_nbbo", "DAY" ) 
    groups['S'] = GroupOrderAttr("S", "DAST", "cxlrpl", "marketable", "DAY" ) 

    groups['T'] = GroupOrderAttr("T", "DAST", "cxlrpl", "behine_nbbo", "IOC" ) 
    groups['U'] = GroupOrderAttr("U", "DAST", "cxlrpl", "on_nbbo", "IOC" ) 
    groups['V'] = GroupOrderAttr("V", "DAST", "cxlrpl", "marketable", "IOC" ) 

    # for i in groups:
    #     print( "group code: ", groups[i].group_code, " acct type", groups[i].trading_account, " price", groups[i].price)

def LoadEventTable():
    try:
        event_file = open(args.event_file, "r")
    except IOError:
        print(args.event_file, " Event File does not exist")

    for line in event_file:
       (seq, mtime, stock_number) = line.split(",")
       events_list.append( Event(seq, mtime, stock_number.rstrip() ))

    print("Total events: ", len(events_list))

def PrepareCHXBook():
    # Create empty book for every key
    for i in stocks: 
        key = "LMM:B:" + stocks[i].symbol + ":50.00"
        chx_book[key] = deque()
        key = "LMM:B:" + stocks[i].symbol + ":49.99"
        chx_book[key] = deque()
        key = "LMM:S:" + stocks[i].symbol + ":50.01"
        chx_book[key] = deque()
        key = "LMM:S:" + stocks[i].symbol + ":50.02"
        chx_book[key] = deque()
        
        key = "DAST:B:" + stocks[i].symbol + ":50.00"
        chx_book[key] = deque()
        key = "DAST:B:" + stocks[i].symbol + ":49.99"
        chx_book[key] = deque()
        key = "DAST:S:" + stocks[i].symbol + ":50.01"
        chx_book[key] = deque()
        key = "DAST:S:" + stocks[i].symbol + ":50.02"
        chx_book[key] = deque()

def CreateTestMessage():
    max_model = len(model_seq) 
    count = 0
    for event in events_list:
        model_idx = count % max_model
        group = groups[model_seq[model_idx]]
        side_idx = count % 2
        side = None
        if ( side_idx == 0):
            side = "B"
        else:
            side = "S"    
        msg = Message("test", 1, event.seq, event.mtime, side, 100, stocks[event.stock_number], group) 

        #
        # Add clordid in chx_book if the order is resting
        if ( msg.time_in_force != "IOC" and msg.price != "marketable" ):
            if ( msg.msg_type == "new" or msg.msg_type == "cxlrpl" ):
                chx_book[msg.GetRestingLookupKey()].append( msg.clordid )
        #
        # Populate orig_clordid
        if ( msg.msg_type == "cxl" or msg.msg_type == "cxlrpl" ):
            #
            # Find resting order in CHX book. If not found then create seeded order
            if ( len(chx_book[msg.GetRestingLookupKey()]) > 0):
                #
                # Get resting order in the book
                msg.SetOrigClOrdId(chx_book[msg.GetRestingLookupKey()].pop())
            else:
                #
                # We have to create seeded order for this message
                seeded_order = msg.CreateSeededOrder()
                if ( seeded_order != None ):
                    seeded_orders.append( seeded_order )
        #
        # Add qty of resting order for marketable
        if ( (msg.msg_type == "new" or msg.msg_type == "cxlrpl") and msg.price == "marketable" ):
            lookup_key = msg.stock.symbol + ":" + msg.GetContraSide()
            if ( lookup_key in resting_match_orders ):
                resting_match_orders[lookup_key].qty = resting_match_orders[lookup_key].qty + 100
            else:
                resting_match_orders[lookup_key] = Message("match", 1, 0, 0, msg.GetContraSide(), 100, msg.stock, groups['B'])
                
        test_messages.append( msg )
        count = count + 1

def FormatMessageForFile( msg ):
    if ( msg.msg_type == "cxl" or msg.msg_type == "cxlrpl" ):
        return "{0},{1},{2},{3},{4},{5},{6},{7},{8}".format( msg.seq, msg.mtime, msg.msg_type, msg.stock.symbol, msg.real_price,
                msg.time_in_force, msg.trading_account, msg.clordid, msg.orig_clordid)
    else:
        return "{0},{1},{2},{3},{4},{5},{6},{7}".format( msg.seq, msg.mtime, msg.msg_type, msg.stock.symbol, msg.real_price,
                msg.time_in_force, msg.trading_account, msg.clordid)

###################################################################
# Start Processing
###################################################################
def main():
    LoadStockTable()
    LoadModelSeq()
    LoadGroupTemplate()
    LoadEventTable()
    PrepareCHXBook()
    CreateTestMessage()

    print("=== Test Messages " )
    for msg in test_messages:
        print( FormatMessageForFile(msg) )

    print("==== CHX Book " )
    for i in chx_book:
        if ( len(chx_book[i]) ):
            print ("   Key:" + i + " size: " + str(len(chx_book[i])) )
    print( "==== Seeded Orders" )
    try:
        output_seeded_file = open(args.output_seeded_file, "w")
    except IOError:
        print( args.output_seeded_file, " could not open for write")

    seeded_count = 0
    for r in resting_match_orders:
        resting_match_orders[r].seq = seeded_count
        resting_match_orders[r].mtime = seeded_count * 100
        print( FormatMessageForFile(resting_match_orders[r]) )
        output_seeded_file.writelines( FormatMessageForFile(resting_match_orders[r]) )
        seeded_count = seeded_count + 1
    for s in seeded_orders:
        s.seq = seeded_count
        s.mtime = seeded_count * 100
        print( FormatMessageForFile(s) )
        output_seeded_file.writelines(FormatMessageForFile(s) )
        seeded_count = seeded_count + 1

###################################################################
# Starting point
###################################################################
main()

