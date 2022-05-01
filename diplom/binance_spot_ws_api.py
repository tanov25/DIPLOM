import os
import websocket 
import simplejson as json
from pathlib import Path
from datetime import datetime


#make static or class
#balancing algo (n particion problem)
#this one is static and should be abstract and shiet

def return_time_mask(freq='D'):
    if freq == 'D':
        return '%Y-%m-%d'
    elif freq == 'H':
        return '%Y-%m-%d-%H'
    else:
        return '%Y-%m-%d'

def save_data(data, type, exch_name, symbol, timestamp, freq, csv_or_json='json'):
    #print(data)
    str_to_write = ""
    if csv_or_json == 'json':
        str_to_write = json.dumps(data)
    else:
        for _, val in data.items():
            str_to_write = str_to_write + str(val) + ";"
        str_to_write = str_to_write[:-1] + '\n'

    timestamp_ = datetime.utcfromtimestamp(float(timestamp/1000.0))
     
    my_file = Path(f"/workspace/main/python_scripts/DIPLOM/save_test_data/{symbol}_{exch_name}_" + timestamp_.strftime(return_time_mask()) + ".txt")
    
    if os.path.isfile(my_file):
        with open(my_file,'a+') as f:
            f.write(str_to_write)
        f.close()
    else:
        with open(my_file,'a+') as f:
            header = ""
            for key, _ in data.items():
                header = header + str(key) + ";"
            header = header[:-1] + '\n'
            print(header)
            f.write(header)
            f.write(str_to_write)
        f.close()
    pass

class Binance():
    name = "BINANCE"
    types = {'l2_updates':'depth@100ms', 'raw_trades':'trade', 'agg_trades':'aggTrade'}
    endpoint = "wss://stream.binance.com:9443/ws"

    def __init__(self) -> None:
        pass
    
    @classmethod
    def get_all_symbols(cls):
        return ['btcusdt', 'ethusdt']

    @classmethod
    def subscribe_message(cls, type_, symbols):
        msg = dict()
        msg['method'] = "SUBSCRIBE"
        subs = list()
        for symbol in symbols:
            sub = symbol + "@" + cls.types[type_]
            subs.append(sub)
        msg['params'] = subs
        msg['id'] = 1
        print(msg)
        return json.dumps(msg)
    
    def agg_trade_callback(cls, msg):
        """
        
        """
        pass

    def depth_update_callback(cls, msg):
        """
        {
  "e": "depthUpdate", // Event type
  "E": 123456789,     // Event time
  "s": "BNBBTC",      // Symbol
  "U": 157,           // First update ID in event
  "u": 160,           // Final update ID in event
  "b": [              // Bids to be updated
    [
      "0.0024",       // Price level to be updated
      "10"            // Quantity
    ]
  ],
  "a": [              // Asks to be updated
    [
      "0.0026",       // Price level to be updated
      "100"           // Quantity
    ]
  ]
}
        """
        book_raw = json.loads(msg)

        

    def partial_depth_update_callback(cls, msg):
        pass

    @classmethod
    def raw_trade_callback(cls, trade_raw):
        """
        {
    "e": "trade",     // Event type
    "E": 123456789,   // Event time
    "s": "BNBBTC",    // Symbol
    "t": 12345,       // Trade ID
    "p": "0.001",     // Price
    "q": "100",       // Quantity
    "b": 88,          // Buyer order ID
    "a": 50,          // Seller order ID
    "T": 123456785,   // Trade time
    "m": true,        // Is the buyer the market maker?
    "M": true         // Ignore
    }
        
        """
        #print(trade_raw)
        #print(type(trade_raw))
        #del trade_raw['E']
        del trade_raw['M']
        del trade_raw['e']
        if trade_raw['m'] == "true":
            pass
        save_data(trade_raw, 'raw_trade', Binance.name, trade_raw['s'], trade_raw['E'], 'D', csv_or_json='csv')
        

    def callback(type='raw_trades'):
        return Binance.raw_trade_callback
    
#what kind of data do i need to pass in subprocess
"""
adress, proxy, subscription message, callback, symbols_parsed_conveted&original, pong sending policy, always reconnect, data validation (ids continuation and shiet) 
"""

def test_inner_function(address, message, callback, proxy=None):

    def on_open(ws):
        ws.send(message)
        ws.sock.recv()

    def on_message(ws, msg):
        callback(json.loads(msg))

    ws = websocket.WebSocketApp(address, on_open=on_open, on_message=on_message)
    ws.run_forever()
    #sleep
    #ask a server to connect



if __name__ == "__main__":
    type_data = 'raw_trades'
    symbols = Binance.get_all_symbols()
    message = Binance.subscribe_message(type_data, symbols)

    #this is done in subprocess
    test_inner_function(Binance.endpoint, message, callback=Binance.callback(type_data))
    #also a websocket server is started to listen to all the disconnected baybes (it also pings google)