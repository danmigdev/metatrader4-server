//+------------------------------------------------------------------+
//|                                              ZeroMQ_Monitor.mq5  |
//|                        Copyright 2025, danmig                     |
//|                    Watchdog EA for ZeroMQ_Server script           |
//+------------------------------------------------------------------+
#property copyright "Copyright 2025, danmig"
#property link      ""
#property version   "1.4"
#property description "Monitors ZeroMQ_Server via GlobalVariable heartbeat and restarts if unresponsive"

// Input parameters
input bool   ENABLED = true;               // Enable monitor (false = disabled without removing)
input int    TIMEOUT_SECONDS = 30;         // Timeout in seconds (default 30 seconds)
input int    CHECK_INTERVAL_SECONDS = 15;  // Check interval in seconds
input string TEMPLATE_NAME = "ZeroMQ_Server_EA";  // Template name to apply for restart
input string SERVER_CHART_SYMBOL = "";     // Symbol of chart running ZeroMQ_Server (empty = same as this chart)
input long   SERVER_CHART_ID = 0;          // Specific chart ID for ZeroMQ_Server (0 = auto-detect from logs)

// Heartbeat global variable name (must match ZeroMQ_Server.mq5)
const string HEARTBEAT_GV_NAME = "ZeroMQ_Server_LastResponse";

// Chart ID where ZeroMQ_Server runs
long serverChartId = 0;

//+------------------------------------------------------------------+
//| Expert initialization function                                     |
//+------------------------------------------------------------------+
int OnInit() {
    // Check if monitor is disabled
    if (!ENABLED) {
        Print("ZeroMQ Monitor v1.4 - DISABLED");
        return(INIT_SUCCEEDED);
    }

    // Find or validate server chart
    if (!FindServerChart()) {
        Print("[ERROR] Could not find server chart. Check SERVER_CHART_SYMBOL parameter.");
        return(INIT_FAILED);
    }

    // Set timer for periodic checks
    EventSetTimer(CHECK_INTERVAL_SECONDS);

    Print("ZeroMQ Monitor v1.4 initialized");
    Print("  - Timeout: ", TIMEOUT_SECONDS, " seconds");
    Print("  - Check interval: ", CHECK_INTERVAL_SECONDS, " seconds");
    Print("  - Restart template: ", TEMPLATE_NAME);
    Print("  - Server chart ID: ", serverChartId, " (", ChartSymbol(serverChartId), ")");

    // Initial check
    CheckServerStatus();

    return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Find the chart where ZeroMQ_Server should run                      |
//+------------------------------------------------------------------+
bool FindServerChart() {
    // If specific chart ID is provided, use it directly
    if (SERVER_CHART_ID > 0) {
        // Validate that the chart exists
        if (ChartSymbol(SERVER_CHART_ID) != "") {
            serverChartId = SERVER_CHART_ID;
            Print("[OK] Using specified server chart ID: ", serverChartId, " (", ChartSymbol(serverChartId), ")");
            return true;
        } else {
            Print("[ERROR] Specified SERVER_CHART_ID ", SERVER_CHART_ID, " does not exist!");
            return false;
        }
    }

    // Auto-detect: find a different chart with target symbol
    string targetSymbol = SERVER_CHART_SYMBOL;
    if (targetSymbol == "") {
        targetSymbol = Symbol();
    }

    Print("[DEBUG] Auto-detecting chart with symbol: ", targetSymbol);
    Print("[DEBUG] This chart ID: ", ChartID());

    long chartId = ChartFirst();
    long thisChartId = ChartID();

    while (chartId >= 0) {
        Print("[DEBUG] Found chart ", chartId, " symbol=", ChartSymbol(chartId));

        if (chartId != thisChartId && ChartSymbol(chartId) == targetSymbol) {
            serverChartId = chartId;
            Print("[DEBUG] Auto-selected server chart: ", serverChartId);
            Print("[INFO] To use a specific chart, set SERVER_CHART_ID = ", serverChartId);
            return true;
        }
        chartId = ChartNext(chartId);
    }

    // If no other chart found, warn but allow using same chart
    Print("[WARNING] No separate chart found for ", targetSymbol, ". Using same chart (Monitor will be removed on restart).");
    serverChartId = thisChartId;
    return true;
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                   |
//+------------------------------------------------------------------+
void OnDeinit(const int reason) {
    EventKillTimer();
    Print("ZeroMQ Monitor stopped");
}

//+------------------------------------------------------------------+
//| Timer function - called every CHECK_INTERVAL_SECONDS               |
//+------------------------------------------------------------------+
void OnTimer() {
    if (!ENABLED) return;
    CheckServerStatus();
}

//+------------------------------------------------------------------+
//| Check if ZeroMQ Server is responsive                               |
//| Reads heartbeat from GlobalVariable updated after each response    |
//+------------------------------------------------------------------+
void CheckServerStatus() {
    // Check if heartbeat global variable exists
    if (!GlobalVariableCheck(HEARTBEAT_GV_NAME)) {
        Print("[WARNING] ZeroMQ Server heartbeat not found - server may not be running");
        RestartServer();
        return;
    }

    // Get last successful response timestamp
    datetime lastResponse = (datetime)GlobalVariableGet(HEARTBEAT_GV_NAME);
    datetime now = TimeCurrent();
    int elapsedSeconds = (int)(now - lastResponse);

    // Check if timeout exceeded
    if (elapsedSeconds > TIMEOUT_SECONDS) {
        Print("[WARNING] ZeroMQ Server no successful response for ", elapsedSeconds, " seconds (timeout: ", TIMEOUT_SECONDS, ")");
        Print("  - Last response: ", TimeToString(lastResponse, TIME_DATE|TIME_SECONDS));
        Print("  - Current time: ", TimeToString(now, TIME_DATE|TIME_SECONDS));
        RestartServer();
    } else {
        // Server is responsive
        Print("[OK] ZeroMQ Server OK - last response ", elapsedSeconds, " seconds ago");
    }
}

//+------------------------------------------------------------------+
//| Restart the ZeroMQ Server by applying template                     |
//+------------------------------------------------------------------+
void RestartServer() {
    Print("[ACTION] Attempting to restart ZeroMQ Server on chart ", serverChartId, "...");

    // Clear the old heartbeat to avoid immediate re-trigger
    GlobalVariableDel(HEARTBEAT_GV_NAME);

    // Apply template to the SERVER chart (not this chart)
    // The template must be saved with the script already running
    string templateFile = TEMPLATE_NAME + ".tpl";

    if (ChartApplyTemplate(serverChartId, templateFile)) {
        Print("[OK] Template '", templateFile, "' applied to chart ", serverChartId);
        Print("[INFO] ZeroMQ Server should restart automatically");
    } else {
        int err = GetLastError();
        Print("[ERROR] Failed to apply template '", templateFile, "' to chart ", serverChartId, ". Error: ", err);
        Print("[INFO] Please ensure template exists in MQL5/Profiles/Templates/");

        // Alternative: Alert user to manually restart
        Alert("ZeroMQ Server is unresponsive! Please restart manually.");
    }
}

//+------------------------------------------------------------------+
//| Tick function (not used but required)                              |
//+------------------------------------------------------------------+
void OnTick() {
    // Not used - we rely on timer events
}
//+------------------------------------------------------------------+
