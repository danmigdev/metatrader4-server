//+------------------------------------------------------------------+
//|                                            ZeroMQ_Server_EA.mq5  |
//|  An endpoint for remote control of MetaTrader 5 via ZeroMQ       |
//|  EA version - can be saved in templates for auto-restart         |
//+------------------------------------------------------------------+
#property description   "An endpoint for remote control of MetaTrader 5 via ZeroMQ sockets."
#property copyright     "Copyright 2020-2025, CoeJoder - MQL5 Port"
#property link          "https://github.com/CoeJoder/metatrader4-server"
#property version       "3.0"

// see: https://github.com/dingmaotu/mql-zmq
#include <Zmq/Zmq.mqh>
// see: https://www.mql5.com/en/code/13663
#include <JAson.mqh>
#include <Trade/Trade.mqh>

// MQL4 compatibility - define missing error code
#define ERR_HISTORY_WILL_UPDATED 4066

// input parameters
input string SCRIPT_NAME = "ZeroMQ_Server_EA";
input string ADDRESS = "tcp://*:28282";
input int REQUEST_POLLING_INTERVAL = 10;
input int RESPONSE_TIMEOUT = 60000 * 5;  // 5 minutes for large data requests
input int MIN_POINT_DISTANCE = 3;
input bool VERBOSE = false;

// Heartbeat global variable name - updated after each successful response
const string HEARTBEAT_GV_NAME = "ZeroMQ_Server_LastResponse";

// response message keys
const string KEY_RESPONSE = "response";
const string KEY_ERROR_CODE = "error_code";
const string KEY_ERROR_CODE_DESCRIPTION = "error_code_description";
const string KEY_ERROR_MESSAGE = "error_message";
const string KEY_WARNING = "warning";

// types of requests
enum RequestAction {
    GET_ACCOUNT_INFO,
    GET_ACCOUNT_INFO_INTEGER,
    GET_ACCOUNT_INFO_DOUBLE,
    GET_SYMBOL_INFO,
    GET_SYMBOL_MARKET_INFO,
    GET_SYMBOL_INFO_INTEGER,
    GET_SYMBOL_INFO_DOUBLE,
    GET_SYMBOL_INFO_STRING,
    GET_SYMBOL_TICK,
    GET_ORDER,
    GET_ORDERS,
    GET_HISTORICAL_ORDERS,
    GET_SYMBOLS,
    GET_OHLCV,
    GET_SIGNALS,
    GET_SIGNAL_INFO,
    DO_ORDER_SEND,
    DO_ORDER_CLOSE,
    DO_ORDER_DELETE,
    DO_ORDER_MODIFY,
    RUN_INDICATOR
};

// types of indicators (prefixed with IND_ to avoid conflicts with MQL5 built-in functions)
enum Indicator {
    IND_iAC, IND_iAD, IND_iADX, IND_iAlligator, IND_iAO, IND_iATR, IND_iBearsPower,
    IND_iBands, IND_iBandsOnArray, IND_iBullsPower, IND_iCCI, IND_iCCIOnArray,
    IND_iCustom, IND_iDeMarker, IND_iEnvelopes, IND_iEnvelopesOnArray, IND_iForce,
    IND_iFractals, IND_iGator, IND_iIchimoku, IND_iBWMFI, IND_iMomentum,
    IND_iMomentumOnArray, IND_iMFI, IND_iMA, IND_iMAOnArray, IND_iOsMA, IND_iMACD,
    IND_iOBV, IND_iSAR, IND_iRSI, IND_iRSIOnArray, IND_iRVI, IND_iStdDev,
    IND_iStdDevOnArray, IND_iStochastic, IND_iWPR
};

// ZeroMQ sockets
Context* context = NULL;
Socket* socket = NULL;

// Trade object for trading operations
CTrade trade;

// Error recovery
int consecutiveErrors = 0;
int maxConsecutiveErrors = 10;

//+------------------------------------------------------------------+
//| Expert initialization function                                     |
//+------------------------------------------------------------------+
int OnInit() {
    // Initialize heartbeat on startup
    UpdateLastResponseTime();
    Print("ZeroMQ Server EA v3.0 - Watchdog heartbeat enabled");

    // Initialize ZeroMQ
    Print("Initializing ZeroMQ context and socket...");
    context = new Context(SCRIPT_NAME);
    context.setBlocky(false);
    socket = new Socket(context, ZMQ_REP);
    socket.setSendHighWaterMark(1);
    socket.setReceiveHighWaterMark(1);
    socket.setSendTimeout(RESPONSE_TIMEOUT);

    if (!socket.bind(ADDRESS)) {
        Alert(StringFormat("Failed to bind socket on %s: %s", ADDRESS, Zmq::errorMessage(Zmq::errorNumber())));
        return INIT_FAILED;
    }

    Print(StringFormat("Listening for requests on %s", ADDRESS));

    // Set timer for polling (milliseconds to seconds conversion)
    EventSetMillisecondTimer(REQUEST_POLLING_INTERVAL);

    return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                   |
//+------------------------------------------------------------------+
void OnDeinit(const int reason) {
    EventKillTimer();
    Cleanup();
    Print("ZeroMQ Server EA stopped. Reason: ", reason);
}

//+------------------------------------------------------------------+
//| Timer function - polls for ZeroMQ messages                         |
//+------------------------------------------------------------------+
void OnTimer() {
    if (!_runMainLoop()) {
        consecutiveErrors++;
        Print(StringFormat("Socket error (%d/%d). Attempting recovery...",
              consecutiveErrors, maxConsecutiveErrors));

        if (consecutiveErrors >= maxConsecutiveErrors) {
            Print("Too many consecutive errors. Restarting socket...");
            if (_restartSocket()) {
                Print("Socket restarted successfully.");
                consecutiveErrors = 0;
            }
        }
    } else {
        consecutiveErrors = 0;
    }
}

//+------------------------------------------------------------------+
//| Tick function (not used but can process during ticks too)          |
//+------------------------------------------------------------------+
void OnTick() {
    // Optional: process messages on tick as well for faster response
    // _runMainLoop();
}

// Update last response timestamp - called after each successful response sent
void UpdateLastResponseTime() {
    GlobalVariableSet(HEARTBEAT_GV_NAME, (double)TimeCurrent());
}

// Cleanup function for EA termination
void Cleanup() {
    // Clear heartbeat on exit
    GlobalVariableDel(HEARTBEAT_GV_NAME);

    if (context != NULL) {
        Print("Cleaning up ZeroMQ resources...");

        if (socket != NULL) {
            socket.unbind(ADDRESS);
            delete socket;
            socket = NULL;
        }

        context.destroy(0);
        delete context;
        context = NULL;
    }
}

// Returns true if main loop cycle was successful, false if error occurred
bool _runMainLoop() {
    if (socket == NULL || context == NULL) return false;

    PollItem poller[1];
    socket.fillPollItem(poller[0], ZMQ_POLLIN);
    ZmqMsg inMessage;

    int pollResult = Socket::poll(poller, 1);  // Short poll, timer handles interval
    if (pollResult == -1) {
        int errNo = Zmq::errorNumber();
        if (errNo != 11 && errNo != 4) {
            Print("Poll error: " + Zmq::errorMessage(errNo) + " (errno: " + IntegerToString(errNo) + ")");
            return false;
        }
        return true;
    }

    if (poller[0].hasInput()) {
        if (_socketReceive(inMessage, true)) {
            if (inMessage.size() > 0) {
                string dataStr = inMessage.getData();
                Trace("Received request: " + dataStr);
                _processRequest(dataStr);
            }
            else {
                sendError("Request was empty.");
            }
        } else {
            return false;
        }
    }

    return true;
}

// Restart the ZeroMQ socket
bool _restartSocket() {
    if (socket != NULL) {
        socket.unbind(ADDRESS);
        delete socket;
        socket = NULL;
    }

    Sleep(500);

    socket = new Socket(context, ZMQ_REP);
    socket.setSendHighWaterMark(1);
    socket.setReceiveHighWaterMark(1);
    socket.setSendTimeout(RESPONSE_TIMEOUT);

    if (!socket.bind(ADDRESS)) {
        Print(StringFormat("Failed to rebind socket on %s: %s", ADDRESS, Zmq::errorMessage(Zmq::errorNumber())));
        return false;
    }

    Print(StringFormat("Socket rebound successfully on %s", ADDRESS));
    return true;
}

bool _socketReceive(ZmqMsg& msg, bool nowait=false) {
    if (!socket.recv(msg, nowait)) {
        Print("Failed to receive request.");
        return false;
    }
    return true;
}

bool _socketSend(string response=NULL, bool nowait=false) {
    if ((response == NULL && !socket.send(nowait)) || (response != NULL && !socket.send(response, nowait))) {
        Alert("Critical error!  Failed to send response to client: " + Zmq::errorMessage(Zmq::errorNumber()));
        return false;
    }
    UpdateLastResponseTime();
    return true;
}

void _processRequest(string dataStr) {
    CJAVal req;
    if (!req.Deserialize(dataStr)) {
        sendError("Failed to parse request.");
        return;
    }
    string actionStr = req["action"].ToStr();
    if (actionStr == "") {
        sendError("No request action specified.");
        return;
    }

    RequestAction action = (RequestAction)-1;
    switch(StringToEnum(actionStr, action)) {
        case GET_ACCOUNT_INFO: Get_AccountInfo(); break;
        case GET_ACCOUNT_INFO_INTEGER: Get_AccountInfoInteger(req); break;
        case GET_ACCOUNT_INFO_DOUBLE: Get_AccountInfoDouble(req); break;
        case GET_SYMBOL_INFO: Get_SymbolInfo(req); break;
        case GET_SYMBOL_MARKET_INFO: Get_SymbolMarketInfo(req); break;
        case GET_SYMBOL_INFO_INTEGER: Get_SymbolInfoInteger(req); break;
        case GET_SYMBOL_INFO_DOUBLE: Get_SymbolInfoDouble(req); break;
        case GET_SYMBOL_INFO_STRING: Get_SymbolInfoString(req); break;
        case GET_SYMBOL_TICK: Get_SymbolTick(req); break;
        case GET_ORDER: Get_Order(req); break;
        case GET_ORDERS: Get_Orders(); break;
        case GET_HISTORICAL_ORDERS: Get_HistoricalOrders(); break;
        case GET_SYMBOLS: Get_Symbols(); break;
        case GET_OHLCV: Get_OHLCV(req); break;
        case GET_SIGNALS: Get_Signals(); break;
        case GET_SIGNAL_INFO: Get_SignalInfo(req); break;
        case DO_ORDER_SEND: Do_OrderSend(req); break;
        case DO_ORDER_MODIFY: Do_OrderModify(req); break;
        case DO_ORDER_CLOSE: Do_OrderClose(req); break;
        case DO_ORDER_DELETE: Do_OrderDelete(req); break;
        case RUN_INDICATOR: Run_Indicator(req); break;
        default:
            sendError(StringFormat("Unrecognized requested action (%s).", actionStr));
            break;
    }
}

void _serializeAndSendResponse(CJAVal& resp) {
    string strResp = resp.Serialize();
    if (_socketSend(strResp)) {
        Trace("Sent response: " + strResp);
    }
}

void sendResponse(CJAVal& data, string warning=NULL) {
    CJAVal resp;
    resp[KEY_RESPONSE].Set(data);
    if (warning != NULL) resp[KEY_WARNING] = warning;
    _serializeAndSendResponse(resp);
}

void sendResponse(string val, string warning=NULL) {
    CJAVal resp;
    resp[KEY_RESPONSE] = val;
    if (warning != NULL) resp[KEY_WARNING] = warning;
    _serializeAndSendResponse(resp);
}

void sendResponse(double val, string warning=NULL) {
    CJAVal resp;
    resp[KEY_RESPONSE] = val;
    if (warning != NULL) resp[KEY_WARNING] = warning;
    _serializeAndSendResponse(resp);
}

void sendResponse(long val, string warning=NULL) {
    CJAVal resp;
    resp[KEY_RESPONSE] = val;
    if (warning != NULL) resp[KEY_WARNING] = warning;
    _serializeAndSendResponse(resp);
}

void sendError(int code, string msg) {
    CJAVal resp;
    resp[KEY_ERROR_CODE] = code;
    resp[KEY_ERROR_CODE_DESCRIPTION] = CustomErrorDescription(code);
    resp[KEY_ERROR_MESSAGE] = msg;
    _serializeAndSendResponse(resp);
}

void sendError(int code) {
    CJAVal resp;
    resp[KEY_ERROR_CODE] = code;
    resp[KEY_ERROR_CODE_DESCRIPTION] = CustomErrorDescription(code);
    _serializeAndSendResponse(resp);
}

void sendError(string msg) {
    CJAVal resp;
    resp[KEY_ERROR_MESSAGE] = msg;
    _serializeAndSendResponse(resp);
}

void sendErrorMissingParam(string paramName) {
    sendError(StringFormat("Missing \"%s\" param.", paramName));
}

bool assertParamExists(CJAVal& req, string paramName) {
    if (IsNullOrMissing(req, paramName)) {
        sendErrorMissingParam(paramName);
        return false;
    }
    return true;
}

bool assertParamArrayExistsAndNotEmpty(CJAVal& req, string paramName) {
    if (!assertParamExists(req, paramName)) return false;
    CJAVal* param = req[paramName];
    if (param.m_type != jtARRAY) {
        sendError(StringFormat("Param \"%s\" is not an array.", paramName));
        return false;
    }
    if (param.Size() == 0) {
        sendError(StringFormat("Param \"%s[]\" is empty.", paramName));
        return false;
    }
    return true;
}

void sendOrder(ulong ticket, string warning=NULL) {
    if (PositionSelectByTicket(ticket)) {
        CJAVal order;
        _getSelectedPosition(order);
        sendResponse(order, warning);
        return;
    }
    else if (OrderSelect(ticket)) {
        CJAVal order;
        _getSelectedOrder(order);
        sendResponse(order, warning);
        return;
    }
    else if (HistoryOrderSelect(ticket)) {
        CJAVal order;
        _getSelectedHistoryOrder(order);
        sendResponse(order, warning);
        return;
    }
    else {
        sendError(StringFormat("Order/Position # %d is not found.", ticket));
    }
}

void Get_AccountInfo() {
    CJAVal account_info;
    account_info["login"] = AccountInfoInteger(ACCOUNT_LOGIN);
    account_info["trade_mode"] = AccountInfoInteger(ACCOUNT_TRADE_MODE);
    account_info["name"] = AccountInfoString(ACCOUNT_NAME);
    account_info["server"] = AccountInfoString(ACCOUNT_SERVER);
    account_info["currency"] = AccountInfoString(ACCOUNT_CURRENCY);
    account_info["company"] = AccountInfoString(ACCOUNT_COMPANY);
    sendResponse(account_info);
}

void Get_AccountInfoInteger(CJAVal& req) {
    if (!IsNullOrMissing(req, "property_name")) {
        string propertyName = req["property_name"].ToStr();
        ENUM_ACCOUNT_INFO_INTEGER action = StringToEnum(propertyName, (ENUM_ACCOUNT_INFO_INTEGER)-1);
        if (action == -1) {
            sendError(StringFormat("Unrecognized account integer property: %s", propertyName));
        } else {
            sendResponse(AccountInfoInteger(action));
        }
    }
    else if (!IsNullOrMissing(req, "property_id")) {
        sendResponse(AccountInfoInteger((ENUM_ACCOUNT_INFO_INTEGER)req["property_id"].ToInt()));
    }
    else {
        sendError("Must include either \"property_name\" or \"property_id\" param.");
    }
}

void Get_AccountInfoDouble(CJAVal& req) {
    if (!IsNullOrMissing(req, "property_name")) {
        string propertyName = req["property_name"].ToStr();
        ENUM_ACCOUNT_INFO_DOUBLE action = StringToEnum(propertyName, (ENUM_ACCOUNT_INFO_DOUBLE)-1);
        if (action == -1) {
            sendError(StringFormat("Unrecognized account double property: %s", propertyName));
        } else {
            sendResponse(AccountInfoDouble(action));
        }
    }
    else if (!IsNullOrMissing(req, "property_id")) {
        sendResponse(AccountInfoDouble((ENUM_ACCOUNT_INFO_DOUBLE)req["property_id"].ToInt()));
    }
    else {
        sendError("Must include either \"property_name\" or \"property_id\" param.");
    }
}

void Get_SymbolInfo(CJAVal& req) {
    if (!assertParamArrayExistsAndNotEmpty(req, "names")) return;
    CJAVal* names = req["names"];
    CJAVal symbols;
    for (int i = 0; i < names.Size(); i++) {
        string name = names[i].ToStr();
        if (!SymbolSelect(name, true)) {
            sendError(GetLastError(), name);
            return;
        }
        CJAVal symbol;
        symbol["name"] = name;
        symbol["point"] = SymbolInfoDouble(name, SYMBOL_POINT);
        symbol["digits"] = SymbolInfoInteger(name, SYMBOL_DIGITS);
        symbol["volume_min"] = SymbolInfoDouble(name, SYMBOL_VOLUME_MIN);
        symbol["volume_step"] = SymbolInfoDouble(name, SYMBOL_VOLUME_STEP);
        symbol["volume_max"] = SymbolInfoDouble(name, SYMBOL_VOLUME_MAX);
        symbol["trade_contract_size"] = SymbolInfoDouble(name, SYMBOL_TRADE_CONTRACT_SIZE);
        symbol["trade_tick_value"] = SymbolInfoDouble(name, SYMBOL_TRADE_TICK_VALUE);
        symbol["trade_tick_size"] = SymbolInfoDouble(name, SYMBOL_TRADE_TICK_SIZE);
        symbol["trade_stops_level"] = SymbolInfoInteger(name, SYMBOL_TRADE_STOPS_LEVEL);
        symbol["trade_freeze_level"] = SymbolInfoInteger(name, SYMBOL_TRADE_FREEZE_LEVEL);
        symbols[name].Set(symbol);
    }
    sendResponse(symbols);
}

void Get_SymbolMarketInfo(CJAVal& req) {
    if (!assertParamExists(req, "symbol") || !assertParamExists(req, "property")) return;
    string symbol = req["symbol"].ToStr();
    string strProperty = req["property"].ToStr();
    if (!SymbolSelect(symbol, true)) {
        sendError(GetLastError(), symbol);
        return;
    }

    ENUM_SYMBOL_INFO_INTEGER intProp = StringToEnum(strProperty, (ENUM_SYMBOL_INFO_INTEGER)-1);
    if (intProp != -1) { sendResponse(SymbolInfoInteger(symbol, intProp)); return; }

    ENUM_SYMBOL_INFO_DOUBLE dblProp = StringToEnum(strProperty, (ENUM_SYMBOL_INFO_DOUBLE)-1);
    if (dblProp != -1) { sendResponse(SymbolInfoDouble(symbol, dblProp)); return; }

    ENUM_SYMBOL_INFO_STRING strProp = StringToEnum(strProperty, (ENUM_SYMBOL_INFO_STRING)-1);
    if (strProp != -1) { sendResponse(SymbolInfoString(symbol, strProp)); return; }

    sendError(StringFormat("Unrecognized market info property: %s", strProperty));
}

void Get_SymbolInfoInteger(CJAVal& req) {
    if (!assertParamExists(req, "symbol")) return;
    string symbol = req["symbol"].ToStr();
    if (!IsNullOrMissing(req, "property_name")) {
        ENUM_SYMBOL_INFO_INTEGER action = StringToEnum(req["property_name"].ToStr(), (ENUM_SYMBOL_INFO_INTEGER)-1);
        if (action == -1) sendError(StringFormat("Unrecognized symbol integer property: %s", req["property_name"].ToStr()));
        else sendResponse(SymbolInfoInteger(symbol, action));
    }
    else if (!IsNullOrMissing(req, "property_id")) {
        sendResponse(SymbolInfoInteger(symbol, (ENUM_SYMBOL_INFO_INTEGER)req["property_id"].ToInt()));
    }
    else sendError("Must include either \"property_name\" or \"property_id\" param.");
}

void Get_SymbolInfoDouble(CJAVal& req) {
    if (!assertParamExists(req, "symbol")) return;
    string symbol = req["symbol"].ToStr();
    if (!IsNullOrMissing(req, "property_name")) {
        ENUM_SYMBOL_INFO_DOUBLE action = StringToEnum(req["property_name"].ToStr(), (ENUM_SYMBOL_INFO_DOUBLE)-1);
        if (action == -1) sendError(StringFormat("Unrecognized symbol double property: %s", req["property_name"].ToStr()));
        else sendResponse(SymbolInfoDouble(symbol, action));
    }
    else if (!IsNullOrMissing(req, "property_id")) {
        sendResponse(SymbolInfoDouble(symbol, (ENUM_SYMBOL_INFO_DOUBLE)req["property_id"].ToInt()));
    }
    else sendError("Must include either \"property_name\" or \"property_id\" param.");
}

void Get_SymbolInfoString(CJAVal& req) {
    if (!assertParamExists(req, "symbol")) return;
    string symbol = req["symbol"].ToStr();
    if (!IsNullOrMissing(req, "property_name")) {
        ENUM_SYMBOL_INFO_STRING action = StringToEnum(req["property_name"].ToStr(), (ENUM_SYMBOL_INFO_STRING)-1);
        if (action == -1) sendError(StringFormat("Unrecognized symbol string property: %s", req["property_name"].ToStr()));
        else sendResponse(SymbolInfoString(symbol, action));
    }
    else if (!IsNullOrMissing(req, "property_id")) {
        sendResponse(SymbolInfoString(symbol, (ENUM_SYMBOL_INFO_STRING)req["property_id"].ToInt()));
    }
    else sendError("Must include either \"property_name\" or \"property_id\" param.");
}

void Get_SymbolTick(CJAVal& req) {
    if (!assertParamExists(req, "symbol")) return;
    string symbol = req["symbol"].ToStr();
    if (!SymbolSelect(symbol, true)) {
        sendError(GetLastError(), symbol);
        return;
    }
    MqlTick lastTick;
    if(SymbolInfoTick(symbol, lastTick)) {
        CJAVal tick;
        tick["time"] = (long)lastTick.time;
        tick["bid"] = lastTick.bid;
        tick["ask"] = lastTick.ask;
        tick["last"] = lastTick.last;
        tick["volume"] = (long)lastTick.volume;
        sendResponse(tick);
    } else {
        sendError(GetLastError());
    }
}

void Get_Order(CJAVal& req) {
    if (!assertParamExists(req, "ticket")) return;
    sendOrder((ulong)req["ticket"].ToInt());
}

void Get_Orders() {
    CJAVal orders;
    int total = PositionsTotal();
    for(int i = 0; i < total; i++) {
        ulong ticket = PositionGetTicket(i);
        if(ticket > 0 && PositionSelectByTicket(ticket)) {
            CJAVal curOrder;
            _getSelectedPosition(curOrder);
            orders.Add(curOrder);
        }
    }
    total = OrdersTotal();
    for(int i = 0; i < total; i++) {
        ulong ticket = OrderGetTicket(i);
        if(ticket > 0 && OrderSelect(ticket)) {
            CJAVal curOrder;
            _getSelectedOrder(curOrder);
            orders.Add(curOrder);
        }
    }
    sendResponse(orders);
}

void Get_HistoricalOrders() {
    datetime from = TimeCurrent() - 90*24*60*60;
    datetime to = TimeCurrent();
    if(!HistorySelect(from, to)) {
        sendError(GetLastError(), "Failed to select history");
        return;
    }
    CJAVal deals;
    int total = HistoryDealsTotal();
    for(int i = 0; i < total; i++) {
        ulong ticket = HistoryDealGetTicket(i);
        if(ticket > 0 && HistoryDealGetInteger(ticket, DEAL_ENTRY) == DEAL_ENTRY_OUT) {
            CJAVal curDeal;
            _getSelectedHistoryDeal(ticket, curDeal);
            deals.Add(curDeal);
        }
    }
    sendResponse(deals);
}

void _getSelectedPosition(CJAVal& order) {
    order["ticket"] = PositionGetInteger(POSITION_TICKET);
    order["magic_number"] = PositionGetInteger(POSITION_MAGIC);
    order["symbol"] = PositionGetString(POSITION_SYMBOL);
    order["order_type"] = PositionGetInteger(POSITION_TYPE);
    order["lots"] = PositionGetDouble(POSITION_VOLUME);
    order["open_price"] = PositionGetDouble(POSITION_PRICE_OPEN);
    order["close_price"] = PositionGetDouble(POSITION_PRICE_CURRENT);
    order["open_time"] = TimeToString((datetime)PositionGetInteger(POSITION_TIME), TIME_DATE|TIME_SECONDS);
    order["close_time"] = "";
    order["expiration"] = "";
    order["sl"] = PositionGetDouble(POSITION_SL);
    order["tp"] = PositionGetDouble(POSITION_TP);
    order["profit"] = PositionGetDouble(POSITION_PROFIT);
    order["commission"] = 0.0;
    order["swap"] = PositionGetDouble(POSITION_SWAP);
    order["comment"] = PositionGetString(POSITION_COMMENT);
}

void _getSelectedOrder(CJAVal& order) {
    order["ticket"] = OrderGetInteger(ORDER_TICKET);
    order["magic_number"] = OrderGetInteger(ORDER_MAGIC);
    order["symbol"] = OrderGetString(ORDER_SYMBOL);
    order["order_type"] = OrderGetInteger(ORDER_TYPE);
    order["lots"] = OrderGetDouble(ORDER_VOLUME_CURRENT);
    order["open_price"] = OrderGetDouble(ORDER_PRICE_OPEN);
    order["close_price"] = 0.0;
    order["open_time"] = TimeToString((datetime)OrderGetInteger(ORDER_TIME_SETUP), TIME_DATE|TIME_SECONDS);
    order["close_time"] = "";
    order["expiration"] = TimeToString((datetime)OrderGetInteger(ORDER_TIME_EXPIRATION), TIME_DATE|TIME_SECONDS);
    order["sl"] = OrderGetDouble(ORDER_SL);
    order["tp"] = OrderGetDouble(ORDER_TP);
    order["profit"] = 0.0;
    order["commission"] = 0.0;
    order["swap"] = 0.0;
    order["comment"] = OrderGetString(ORDER_COMMENT);
}

void _getSelectedHistoryDeal(ulong ticket, CJAVal& deal) {
    deal["ticket"] = HistoryDealGetInteger(ticket, DEAL_TICKET);
    deal["magic_number"] = HistoryDealGetInteger(ticket, DEAL_MAGIC);
    deal["symbol"] = HistoryDealGetString(ticket, DEAL_SYMBOL);
    deal["order_type"] = HistoryDealGetInteger(ticket, DEAL_TYPE);
    deal["lots"] = HistoryDealGetDouble(ticket, DEAL_VOLUME);
    deal["open_price"] = HistoryDealGetDouble(ticket, DEAL_PRICE);
    deal["close_price"] = HistoryDealGetDouble(ticket, DEAL_PRICE);
    deal["open_time"] = TimeToString((datetime)HistoryDealGetInteger(ticket, DEAL_TIME), TIME_DATE|TIME_SECONDS);
    deal["close_time"] = TimeToString((datetime)HistoryDealGetInteger(ticket, DEAL_TIME), TIME_DATE|TIME_SECONDS);
    deal["expiration"] = "";
    deal["sl"] = 0.0;
    deal["tp"] = 0.0;
    deal["profit"] = HistoryDealGetDouble(ticket, DEAL_PROFIT);
    deal["commission"] = HistoryDealGetDouble(ticket, DEAL_COMMISSION);
    deal["swap"] = HistoryDealGetDouble(ticket, DEAL_SWAP);
    deal["comment"] = HistoryDealGetString(ticket, DEAL_COMMENT);
}

void _getSelectedHistoryOrder(CJAVal& order) {
    order["ticket"] = HistoryOrderGetInteger((ulong)0, ORDER_TICKET);
    order["magic_number"] = HistoryOrderGetInteger((ulong)0, ORDER_MAGIC);
    order["symbol"] = HistoryOrderGetString((ulong)0, ORDER_SYMBOL);
    order["order_type"] = HistoryOrderGetInteger((ulong)0, ORDER_TYPE);
    order["lots"] = HistoryOrderGetDouble((ulong)0, ORDER_VOLUME_INITIAL);
    order["open_price"] = HistoryOrderGetDouble((ulong)0, ORDER_PRICE_OPEN);
    order["close_price"] = HistoryOrderGetDouble((ulong)0, ORDER_PRICE_CURRENT);
    order["open_time"] = TimeToString((datetime)HistoryOrderGetInteger((ulong)0, ORDER_TIME_SETUP), TIME_DATE|TIME_SECONDS);
    order["close_time"] = TimeToString((datetime)HistoryOrderGetInteger((ulong)0, ORDER_TIME_DONE), TIME_DATE|TIME_SECONDS);
    order["expiration"] = TimeToString((datetime)HistoryOrderGetInteger((ulong)0, ORDER_TIME_EXPIRATION), TIME_DATE|TIME_SECONDS);
    order["sl"] = HistoryOrderGetDouble((ulong)0, ORDER_SL);
    order["tp"] = HistoryOrderGetDouble((ulong)0, ORDER_TP);
    order["profit"] = 0.0;
    order["commission"] = 0.0;
    order["swap"] = 0.0;
    order["comment"] = HistoryOrderGetString((ulong)0, ORDER_COMMENT);
}

void Get_Symbols() {
    CJAVal symbols;
    int count = SymbolsTotal(false);
    for (int i = 0; i < count; i++) {
        symbols.Add(SymbolName(i, false));
    }
    sendResponse(symbols);
}

void Get_OHLCV(CJAVal& req) {
    if (!assertParamExists(req, "symbol") || !assertParamExists(req, "timeframe")
            || !assertParamExists(req, "limit") || !assertParamExists(req, "timeout")) return;

    string symbol = req["symbol"].ToStr();
    ENUM_TIMEFRAMES timeframe = (ENUM_TIMEFRAMES)req["timeframe"].ToInt();
    int limit = (int)req["limit"].ToInt();
    long timeout = req["timeout"].ToInt();
    int offset = GetDefault(req, "offset", 0);

    if (limit <= 0 || offset < 0 || timeout <= 0) {
        sendError("Invalid parameters");
        return;
    }

    if (!SymbolSelect(symbol, true)) {
        sendError("Symbol selection failed: " + symbol);
        return;
    }

    MqlRates rates[];
    ArraySetAsSeries(rates, false);
    int delay = 100;
    long maxTries = timeout / delay;
    int numResults = -1;

    for (int attempt = 0; attempt < maxTries && numResults == -1; attempt++) {
        numResults = CopyRates(symbol, timeframe, offset, limit, rates);
        if (numResults == -1) {
            int err = GetLastError();
            if (err == ERR_HISTORY_WILL_UPDATED || err == 4066) Sleep(delay);
            else { sendError("CopyRates failed: " + IntegerToString(err)); return; }
        }
    }

    if (numResults <= 0) {
        sendError("No data available");
        return;
    }

    CJAVal ohlcv;
    for (int i = 0; i < numResults; i++) {
        CJAVal curBar;
        curBar["time"] = (long)rates[i].time;
        curBar["open"] = rates[i].open;
        curBar["high"] = rates[i].high;
        curBar["low"] = rates[i].low;
        curBar["close"] = rates[i].close;
        curBar["tick_volume"] = (long)rates[i].tick_volume;
        curBar["real_volume"] = (long)rates[i].real_volume;
        curBar["spread"] = rates[i].spread;
        ohlcv.Add(curBar);
    }
    sendResponse(ohlcv);
}

void Get_Signals() {
    CJAVal signals;
    int total = SignalBaseTotal();
    for (int i = 0; i < total; i++) {
        if (SignalBaseSelect(i)) signals.Add(SignalBaseGetString(SIGNAL_BASE_NAME));
    }
    sendResponse(signals);
}

void Get_SignalInfo(CJAVal& req) {
    if (!assertParamArrayExistsAndNotEmpty(req, "names")) return;
    CJAVal* reqNames = req["names"];
    CJAVal signals;
    int total = SignalBaseTotal();
    for (int i = 0; i < total; i++) {
        if (SignalBaseSelect(i)) {
            string name = SignalBaseGetString(SIGNAL_BASE_NAME);
            if (ArrayEraseElement(reqNames.m_e, name)) {
                CJAVal signal;
                signal["author_login"] = SignalBaseGetString(SIGNAL_BASE_AUTHOR_LOGIN);
                signal["broker"] = SignalBaseGetString(SIGNAL_BASE_BROKER);
                signal["broker_server"] = SignalBaseGetString(SIGNAL_BASE_BROKER_SERVER);
                signal["name"] = name;
                signal["currency"] = SignalBaseGetString(SIGNAL_BASE_CURRENCY);
                signal["date_published"] = SignalBaseGetInteger(SIGNAL_BASE_DATE_PUBLISHED);
                signal["date_started"] = SignalBaseGetInteger(SIGNAL_BASE_DATE_STARTED);
                signal["id"] = SignalBaseGetInteger(SIGNAL_BASE_ID);
                signal["leverage"] = SignalBaseGetInteger(SIGNAL_BASE_LEVERAGE);
                signal["pips"] = SignalBaseGetInteger(SIGNAL_BASE_PIPS);
                signal["rating"] = SignalBaseGetInteger(SIGNAL_BASE_RATING);
                signal["subscribers"] = SignalBaseGetInteger(SIGNAL_BASE_SUBSCRIBERS);
                signal["trades"] = SignalBaseGetInteger(SIGNAL_BASE_TRADES);
                signal["trade_mode"] = SignalBaseGetInteger(SIGNAL_BASE_TRADE_MODE);
                signal["balance"] = SignalBaseGetDouble(SIGNAL_BASE_BALANCE);
                signal["equity"] = SignalBaseGetDouble(SIGNAL_BASE_EQUITY);
                signal["gain"] = SignalBaseGetDouble(SIGNAL_BASE_GAIN);
                signal["max_drawdown"] = SignalBaseGetDouble(SIGNAL_BASE_MAX_DRAWDOWN);
                signal["price"] = SignalBaseGetDouble(SIGNAL_BASE_PRICE);
                signal["roi"] = SignalBaseGetDouble(SIGNAL_BASE_ROI);
                signals[name].Set(signal);
            }
        }
    }
    if (reqNames.Size() == 0) sendResponse(signals);
    else sendError(StringFormat("Signals not found: %s", reqNames.Serialize()));
}

void Do_OrderSend(CJAVal& req) {
    if (!assertParamExists(req, "symbol") || !assertParamExists(req, "order_type")
            || !assertParamExists(req, "lots") || !assertParamExists(req, "comment")) return;

    if (!IsNullOrMissing(req, "sl") && !IsNullOrMissing(req, "sl_points")) {
        sendError("Stop-loss cannot be both relative and absolute.");
        return;
    }
    if (!IsNullOrMissing(req, "tp") && !IsNullOrMissing(req, "tp_points")) {
        sendError("Take-profit cannot be both relative and absolute.");
        return;
    }

    string symbol = req["symbol"].ToStr();
    int orderType = (int)req["order_type"].ToInt();
    double lots = req["lots"].ToDbl();
    string comment = req["comment"].ToStr();

    if (!SymbolSelect(symbol, true)) { sendError(GetLastError(), symbol); return; }
    if (!IsValidTradeOperation(orderType)) { sendError("Invalid trade operation"); return; }

    ENUM_ORDER_TYPE mql5OrderType = ConvertOrderType(orderType);
    bool isPending = (mql5OrderType >= ORDER_TYPE_BUY_LIMIT);

    if (isPending && IsNullOrMissing(req, "price")) {
        sendError("Pending order requires price parameter.");
        return;
    }

    double price = GetDefault(req, "price", DefaultOpenPrice(symbol, mql5OrderType));
    int slippage = GetDefault(req, "slippage", DefaultSlippage(symbol));
    double stopLoss = GetDefault(req, "sl", 0.0);
    double takeProfit = GetDefault(req, "tp", 0.0);
    int slPoints = GetDefault(req, "sl_points", 0);
    int tpPoints = GetDefault(req, "tp_points", 0);
    int magicNumber = GetDefault(req, "magic_number", 0);

    if (slPoints > 0) stopLoss = CalculateSL(symbol, mql5OrderType, price, slPoints);
    if (tpPoints > 0) takeProfit = CalculateTP(symbol, mql5OrderType, price, tpPoints);

    lots = NormalizeLots(symbol, lots);
    price = NormalizePrice(symbol, price);
    stopLoss = NormalizePrice(symbol, stopLoss);
    takeProfit = NormalizePrice(symbol, takeProfit);

    trade.SetDeviationInPoints(slippage);
    trade.SetExpertMagicNumber(magicNumber);
    trade.SetTypeFilling(ORDER_FILLING_FOK);

    bool result = false;
    if (mql5OrderType == ORDER_TYPE_BUY) result = trade.Buy(lots, symbol, 0, stopLoss, takeProfit, comment);
    else if (mql5OrderType == ORDER_TYPE_SELL) result = trade.Sell(lots, symbol, 0, stopLoss, takeProfit, comment);
    else result = trade.OrderOpen(symbol, mql5OrderType, lots, 0, price, stopLoss, takeProfit, ORDER_TIME_GTC, 0, comment);

    if (!result) {
        sendError(trade.ResultRetcode(), "Failed: " + trade.ResultRetcodeDescription());
        return;
    }

    ulong ticket = trade.ResultOrder();
    if (ticket == 0) ticket = trade.ResultDeal();
    if (ticket > 0) sendOrder(ticket);
    else sendError("Order created but ticket not found");
}

void Do_OrderModify(CJAVal& req) {
    if (!assertParamExists(req, "ticket")) return;
    ulong ticket = (ulong)req["ticket"].ToInt();

    if (!IsNullOrMissing(req, "sl") && !IsNullOrMissing(req, "sl_points")) {
        sendError("Stop-loss cannot be both relative and absolute.");
        return;
    }
    if (!IsNullOrMissing(req, "tp") && !IsNullOrMissing(req, "tp_points")) {
        sendError("Take-profit cannot be both relative and absolute.");
        return;
    }

    bool isPosition = PositionSelectByTicket(ticket);
    bool isOrder = !isPosition && OrderSelect(ticket);

    if (!isPosition && !isOrder) {
        sendError(StringFormat("Order/Position # %d not found.", ticket));
        return;
    }

    string symbol = isPosition ? PositionGetString(POSITION_SYMBOL) : OrderGetString(ORDER_SYMBOL);
    double stopLoss = 0, takeProfit = 0, newPrice = 0;

    if (isPosition) {
        stopLoss = GetDefault(req, "sl", PositionGetDouble(POSITION_SL));
        takeProfit = GetDefault(req, "tp", PositionGetDouble(POSITION_TP));
        int slPoints = GetDefault(req, "sl_points", 0);
        int tpPoints = GetDefault(req, "tp_points", 0);
        if (slPoints > 0) {
            ENUM_POSITION_TYPE posType = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
            stopLoss = CalculateSL(symbol, posType == POSITION_TYPE_BUY ? ORDER_TYPE_BUY : ORDER_TYPE_SELL,
                                  PositionGetDouble(POSITION_PRICE_OPEN), slPoints);
        }
        if (tpPoints > 0) {
            ENUM_POSITION_TYPE posType = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
            takeProfit = CalculateTP(symbol, posType == POSITION_TYPE_BUY ? ORDER_TYPE_BUY : ORDER_TYPE_SELL,
                                    PositionGetDouble(POSITION_PRICE_OPEN), tpPoints);
        }
        stopLoss = NormalizePrice(symbol, stopLoss);
        takeProfit = NormalizePrice(symbol, takeProfit);
        if (!trade.PositionModify(ticket, stopLoss, takeProfit)) {
            sendError(trade.ResultRetcode(), "Failed to modify position");
            return;
        }
    } else {
        newPrice = GetDefault(req, "price", OrderGetDouble(ORDER_PRICE_OPEN));
        stopLoss = GetDefault(req, "sl", OrderGetDouble(ORDER_SL));
        takeProfit = GetDefault(req, "tp", OrderGetDouble(ORDER_TP));
        int slPoints = GetDefault(req, "sl_points", 0);
        int tpPoints = GetDefault(req, "tp_points", 0);
        if (slPoints > 0) stopLoss = CalculateSL(symbol, (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE), newPrice, slPoints);
        if (tpPoints > 0) takeProfit = CalculateTP(symbol, (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE), newPrice, tpPoints);
        newPrice = NormalizePrice(symbol, newPrice);
        stopLoss = NormalizePrice(symbol, stopLoss);
        takeProfit = NormalizePrice(symbol, takeProfit);
        if (!trade.OrderModify(ticket, newPrice, stopLoss, takeProfit, ORDER_TIME_GTC, 0)) {
            sendError(trade.ResultRetcode(), "Failed to modify order");
            return;
        }
    }
    sendOrder(ticket);
}

void Do_OrderClose(CJAVal& req) {
    if (!assertParamExists(req, "ticket")) return;
    ulong ticket = (ulong)req["ticket"].ToInt();
    if (!PositionSelectByTicket(ticket)) {
        sendError(StringFormat("Position # %d not found.", ticket));
        return;
    }
    double lots = GetDefault(req, "lots", PositionGetDouble(POSITION_VOLUME));
    int slippage = GetDefault(req, "slippage", DefaultSlippage(PositionGetString(POSITION_SYMBOL)));
    lots = NormalizeLots(PositionGetString(POSITION_SYMBOL), lots);
    trade.SetDeviationInPoints(slippage);
    if (!trade.PositionClose(ticket, slippage)) {
        sendError(trade.ResultRetcode(), "Failed to close position");
        return;
    }
    sendResponse(StringFormat("Closed position # %d", ticket));
}

void Do_OrderDelete(CJAVal& req) {
    if (!assertParamExists(req, "ticket")) return;
    ulong ticket = (ulong)req["ticket"].ToInt();
    bool closeIfOpened = GetDefault(req, "close_if_opened", false);

    if (PositionSelectByTicket(ticket)) {
        if (closeIfOpened) { Do_OrderClose(req); return; }
        else { sendError("Ticket is a position. Use close_if_opened=true."); return; }
    }

    if (!OrderSelect(ticket)) {
        sendError(StringFormat("Order # %d not found.", ticket));
        return;
    }
    if (!trade.OrderDelete(ticket)) {
        sendError(trade.ResultRetcode(), "Failed to delete order");
        return;
    }
    sendResponse(StringFormat("Deleted order # %d", ticket));
}

void Run_Indicator(CJAVal& req) {
    if (!assertParamExists(req, "indicator") || !assertParamExists(req, "argv") || !assertParamExists(req, "timeout")) return;

    string strIndicator = req["indicator"].ToStr();
    CJAVal argv = req["argv"];
    long timeout = req["timeout"].ToInt();

    Indicator indicator = StringToEnum(strIndicator, (Indicator)-1);
    if (indicator == -1) {
        sendError("Indicator not recognized: " + strIndicator);
        return;
    }

    double results[];
    ArrayResize(results, 1);
    int delay = 100;
    long maxTries = timeout / delay;
    bool isDone = false;

    for (int tryNum = 0; tryNum < maxTries && !isDone; tryNum++) {
        int retVal = _runIndicator(indicator, argv, results);
        if (retVal == -1) { sendError("Indicator not recognized"); return; }
        int err = GetLastError();
        if (err == ERR_HISTORY_WILL_UPDATED || err == 4066) Sleep(delay);
        else if (err != 0) { sendError(err); return; }
        else isDone = true;
    }

    if (!isDone) { sendError("Timeout waiting for indicator data"); return; }

    if (ArraySize(results) == 1) sendResponse(results[0]);
    else {
        CJAVal arr;
        for (int i = 0; i < ArraySize(results); i++) arr.Add(results[i]);
        sendResponse(arr);
    }
}

int _runIndicator(Indicator indicator, CJAVal& argv, double& results[]) {
    string symbol = argv[0].ToStr();
    ENUM_TIMEFRAMES period = (ENUM_TIMEFRAMES)argv[1].ToInt();
    int shift = (int)argv[2].ToInt();
    int handle = -1;
    double buffer[];
    ArraySetAsSeries(buffer, true);

    switch(indicator) {
        case IND_iAC:
            handle = iAC(symbol, period);
            break;
        case IND_iAD:
            handle = iAD(symbol, period, VOLUME_TICK);
            break;
        case IND_iATR:
            handle = iATR(symbol, period, (int)argv[2].ToInt());
            break;
        case IND_iRSI:
            handle = iRSI(symbol, period, (int)argv[2].ToInt(), (ENUM_APPLIED_PRICE)argv[3].ToInt());
            break;
        case IND_iMA:
            handle = iMA(symbol, period, (int)argv[2].ToInt(), (int)argv[3].ToInt(),
                        (ENUM_MA_METHOD)argv[4].ToInt(), (ENUM_APPLIED_PRICE)argv[5].ToInt());
            break;
        default:
            return -1;
    }

    if (handle != INVALID_HANDLE && CopyBuffer(handle, 0, shift, 1, buffer) > 0) {
        results[0] = buffer[0];
        IndicatorRelease(handle);
        return 1;
    }
    if (handle != INVALID_HANDLE) IndicatorRelease(handle);
    return -1;
}

bool IsValidTradeOperation(int orderType) { return (orderType >= 0 && orderType <= 5); }

ENUM_ORDER_TYPE ConvertOrderType(int mql4OrderType) {
    switch(mql4OrderType) {
        case 0: return ORDER_TYPE_BUY;
        case 1: return ORDER_TYPE_SELL;
        case 2: return ORDER_TYPE_BUY_LIMIT;
        case 3: return ORDER_TYPE_SELL_LIMIT;
        case 4: return ORDER_TYPE_BUY_STOP;
        case 5: return ORDER_TYPE_SELL_STOP;
        default: return ORDER_TYPE_BUY;
    }
}

double DefaultOpenPrice(string symbol, ENUM_ORDER_TYPE orderType) {
    return NormalizePrice(symbol, (orderType == ORDER_TYPE_BUY || orderType == ORDER_TYPE_BUY_LIMIT || orderType == ORDER_TYPE_BUY_STOP) ?
            SymbolInfoDouble(symbol, SYMBOL_ASK) : SymbolInfoDouble(symbol, SYMBOL_BID));
}

double NormalizePrice(string symbol, double price) {
    if (price == 0) return 0;
    double tickSize = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_SIZE);
    return MathRound(price / tickSize) * tickSize;
}

int DefaultSlippage(string symbol) {
    double tickSize = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_SIZE);
    double spread = MathAbs(SymbolInfoDouble(symbol, SYMBOL_ASK) - SymbolInfoDouble(symbol, SYMBOL_BID));
    return (int)(2.0 * spread / tickSize);
}

double NormalizeLots(string symbol, double lots) {
    double lotStep = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
    double minLot = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
    return MathMax(MathRound(lots / lotStep) * lotStep, minLot);
}

double CalculateSL(string symbol, ENUM_ORDER_TYPE orderType, double price, int points) {
    double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
    return (orderType == ORDER_TYPE_BUY || orderType == ORDER_TYPE_BUY_LIMIT || orderType == ORDER_TYPE_BUY_STOP) ?
           price - points * point : price + points * point;
}

double CalculateTP(string symbol, ENUM_ORDER_TYPE orderType, double price, int points) {
    double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
    return (orderType == ORDER_TYPE_BUY || orderType == ORDER_TYPE_BUY_LIMIT || orderType == ORDER_TYPE_BUY_STOP) ?
           price + points * point : price - points * point;
}

string CustomErrorDescription(int errorCode) {
    switch(errorCode) {
        case 0: return "Success";
        case 4001: return "Wrong function pointer";
        case 4002: return "Array index out of range";
        case 4066: return "History data updating";
        default: return "Error " + IntegerToString(errorCode);
    }
}

template<typename T> T StringToEnum(string str, T enumType) {
    for (int i = 0; i < 256; i++) {
        if (str == EnumToString(enumType = (T)i)) return enumType;
    }
    return (T)-1;
}

template <typename T, typename E> bool ArrayEraseElement(T& arr[], E element) {
    for (int i = 0; i < ArraySize(arr); ++i) {
        if (arr[i] == element) {
            for(int j = i; j < ArraySize(arr) - 1; ++j) arr[j] = arr[j + 1];
            ArrayResize(arr, ArraySize(arr) - 1);
            return true;
        }
    }
    return false;
}

void Trace(string msg) { if (VERBOSE) Print(msg); }

bool IsNullOrMissing(CJAVal& obj, string key) {
    return !obj.HasKey(key) || obj[key].m_type == jtNULL;
}

bool GetDefault(CJAVal& obj, string key, bool defaultVal) {
    return IsNullOrMissing(obj, key) ? defaultVal : obj[key].ToBool();
}

int GetDefault(CJAVal& obj, string key, int defaultVal) {
    return IsNullOrMissing(obj, key) ? defaultVal : (int)obj[key].ToInt();
}

long GetDefault(CJAVal& obj, string key, long defaultVal) {
    return IsNullOrMissing(obj, key) ? defaultVal : obj[key].ToInt();
}

double GetDefault(CJAVal& obj, string key, double defaultVal) {
    return IsNullOrMissing(obj, key) ? defaultVal : obj[key].ToDbl();
}

string GetDefault(CJAVal& obj, string key, string defaultVal) {
    return IsNullOrMissing(obj, key) ? defaultVal : obj[key].ToStr();
}
//+------------------------------------------------------------------+
