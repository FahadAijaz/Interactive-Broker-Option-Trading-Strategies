from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.scanner import ScannerSubscription
import pandas as pd
import datetime


class TestApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self,self)
        self.scanner_data_master = pd.DataFrame({})
        self.stocks = []
        self.i = 0

    def error(self, reqId, errorCode, errorString):
        print("Error", reqId, " ", errorCode, " ", errorString)

    def contractDetails(self, reqId, contractDetails):
        print(contractDetails.__str__())

    def historicalData(self, reqId: int, bar):
        historical_data_dict = {'con_id': reqId, 'date': bar.date, 'open': bar.open, 'high': bar.high,
                                'low': bar.low, 'close': bar.close, 'volume': bar.volume, 'average': bar.average,
                                'barcount': bar.barCount}
        historical_data = pd.DataFrame([historical_data_dict], columns= historical_data_dict.keys())

        result = pd.merge(self.market_data_master,historical_data, how='left', on= 'con_id')
        print(result)




    def scannerData(self, reqId, rank, contractDetails, distance, benchmark, projection, legsStr):
        super().scannerData(reqId, rank, contractDetails, distance, benchmark, projection, legsStr)

        scanner_data_dict = {'con_id': contractDetails.contract.conId, 'rank': rank, 'symbol': contractDetails.contract.symbol,
                        'sec_type': contractDetails.contract.secType, 'sec_currency': contractDetails.contract.currency,
                        'sec_exchange': contractDetails.contract.exchange}

        scanner_data = pd.DataFrame([scanner_data_dict], columns=scanner_data_dict.keys())
        self.scanner_data_master = self.scanner_data_master.append(scanner_data)

        # print("ScannerData. ReqId:", reqId, "Rank:", rank, "Symbol:", contractDetails.contract.symbol,
        # "SecType:", contractDetails.contract.secType,
        # "Currency:", contractDetails.contract.currency,
        # "Distance:", distance, "Benchmark:", benchmark,
        # "Projection:", projection, "Legs String:", legsStr)

        contract = contractDetails.contract
        queryTime = (datetime.datetime.today()).strftime("%Y%m%d %H:%M:%S")
        self.reqHistoricalData(contractDetails.contract.conId, contract, queryTime,
                              "1 D", "1 day", "OPTION_IMPLIED_VOLATILITY", 1, 1, False, [])
        
    def scannerDataEnd(self, reqId):
        print(self.scanner_data_master)

    def scannerParameters(self, xml):
        super().scannerParameters(xml)
        open('log/scanner.xml', 'w').write(xml)
        print("Scanner Parameters received")

    # 
    # def error_handler(msg):
    #     print("Server Error: %s" % msg)
    # 
    # def reply_handler(msg):
    #     print("Server Response: %s, %s" % (msg.typeName, msg))
    # 

def main():
    
    app = TestApp()
    
    app.connect("127.0.0.1", 7496, 0)

    # app.register(error_handler, 'Error')
    # app.registerAll(reply_handler)

    contract = Contract()    
    contract.symbol = "GOOG"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    contract.primaryExchange = "NASDAQ"

    scanner = ScannerSubscription()
    scanner.instrument = "STK"
    scanner.locationCode = "STK.US.MAJOR"
    scanner.scanCode = "HIGH_OPT_IMP_VOLAT"
    scanner.marketCapAbove = 5000000000
    scanner.averageOptionVolumeAbove = 50000

    # tagvalues = []
    # tagvalues.append(TagValue("usdMarketCapAbove", "50000000"))
    # tagvalues.append(TagValue("AverageOptionVolumeAbove", "50000"))

    app.reqScannerSubscription(7001, scanner, [], [])

    app.run()


if __name__ == "__main__":
    main()