# ZeroMQ Server and Monitor for MetaTrader 5

This folder contains two Expert Advisors (EA) for managing ZeroMQ communication with MetaTrader 5.

## Files

| File | Description |
|------|-------------|
| `ZeroMQ_Server_EA.mq5` | ZeroMQ server to receive remote commands |
| `ZeroMQ_Monitor_EA.mq5` | Watchdog to monitor and restart the server |

## Installation

1. Copy the `.mq5` files to the `MQL5/Experts/` folder of your MT5 terminal
2. Copy the `Include/Zmq/` folder to `MQL5/Include/`
3. Compile both EAs in MetaEditor

## ZeroMQ_Server_EA Configuration

### Input Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SCRIPT_NAME` | ZeroMQ_Server_EA | Server identifier name |
| `ADDRESS` | tcp://*:28282 | Listening address and port |
| `REQUEST_POLLING_INTERVAL` | 10 | Polling interval in milliseconds |
| `RESPONSE_TIMEOUT` | 300000 | Response timeout (5 minutes) |
| `MIN_POINT_DISTANCE` | 3 | Minimum distance in points |
| `VERBOSE` | false | Enable detailed logging |

### Setup

1. Open a chart (e.g., EURUSD)
2. Drag `ZeroMQ_Server_EA` onto the chart
3. Enable "Allow DLL imports" in options
4. Click OK

### Creating a Template for Auto-Restart

1. Configure the server EA on the chart
2. Go to `Chart > Templates > Save Template`
3. Save as `ZeroMQ_Server_EA.tpl`

## ZeroMQ_Monitor_EA Configuration

The Monitor EA checks that the Server is active and automatically restarts it if unresponsive.

### Input Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ENABLED` | true | Enable/disable the monitor |
| `TIMEOUT_SECONDS` | 30 | Timeout before restart |
| `CHECK_INTERVAL_SECONDS` | 15 | Check interval in seconds |
| `TEMPLATE_NAME` | ZeroMQ_Server_EA | Template name for restart |
| `SERVER_CHART_SYMBOL` | "" | Server chart symbol (empty = same symbol) |
| `SERVER_CHART_ID` | 0 | Specific chart ID (0 = auto-detect) |

### Recommended Setup (Two Charts)

Using two separate charts allows the Monitor to restart the Server without removing itself.

#### Step 1: Setup the Server Chart

1. Open MetaTrader 5
2. Go to `File > New Chart > EURUSD` (or any symbol)
3. In the Navigator panel (Ctrl+N), expand `Expert Advisors`
4. Drag `ZeroMQ_Server_EA` onto the chart
5. In the popup dialog:
   - Go to `Dependencies` tab
   - Check "Allow DLL imports"
   - Click OK
6. Verify in the Experts tab (Ctrl+T) that the server started:
   ```
   ZeroMQ Server EA v3.0 - Watchdog heartbeat enabled
   Listening for requests on tcp://*:28282
   ```

#### Step 2: Create the Restart Template

1. With the Server EA running on the chart
2. Go to `Chart > Templates > Save Template...`
3. Save as exactly: `ZeroMQ_Server_EA.tpl`
4. The template is saved in `MQL5/Profiles/Templates/`

#### Step 3: Find the Server Chart ID

The Chart ID is a unique number that identifies each chart window. You need this to tell the Monitor which chart to restart.

**Method 1 - From Server Logs:**
1. Look at the Experts tab when the Server starts
2. Find the log line containing the chart ID:
   ```
   [DEBUG] This chart ID: 132456789012345678
   ```

**Method 2 - From Monitor Auto-Detection:**
1. Start the Monitor EA (next step)
2. Look at the Experts tab for:
   ```
   [DEBUG] Found chart 132456789012345678 symbol=EURUSD
   [INFO] To use a specific chart, set SERVER_CHART_ID = 132456789012345678
   ```
3. Copy this number for the next step

**Method 3 - Using a Script:**
1. Create a new script in MetaEditor with:
   ```mql5
   void OnStart() {
       Print("This chart ID: ", ChartID());
   }
   ```
2. Run it on the Server chart
3. Check the Experts tab for the ID

#### Step 4: Setup the Monitor Chart

1. Go to `File > New Chart > EURUSD` (same symbol as Server)
2. You now have two EURUSD charts open
3. Drag `ZeroMQ_Monitor_EA` onto the NEW chart (not the Server chart)
4. In the popup dialog:
   - Set `SERVER_CHART_ID` = the ID from Step 3 (e.g., `132456789012345678`)
   - Adjust `TIMEOUT_SECONDS` if needed (default 30)
   - Click OK
5. Verify in Experts tab:
   ```
   ZeroMQ Monitor v1.4 initialized
     - Server chart ID: 132456789012345678 (EURUSD)
   ```

#### Step 5: Test the Setup

1. Stop the Server EA manually (right-click > Remove EA)
2. Wait for `TIMEOUT_SECONDS` (default 30 seconds)
3. The Monitor should detect the timeout and restart:
   ```
   [WARNING] ZeroMQ Server heartbeat not found - server may not be running
   [ACTION] Attempting to restart ZeroMQ Server...
   [OK] Template 'ZeroMQ_Server_EA.tpl' applied
   ```

### Temporarily Disable

To disable the monitor without removing it:
1. Right-click on EA > Properties
2. Set `ENABLED = false`
3. Click OK

## How It Works

```
+-------------------+        +-------------------+
|  ZeroMQ_Server_EA |        | ZeroMQ_Monitor_EA |
+-------------------+        +-------------------+
         |                            |
         v                            v
  Updates GlobalVariable       Reads GlobalVariable
  "ZeroMQ_Server_LastResponse" every CHECK_INTERVAL
         |                            |
         |                   If timeout > TIMEOUT_SECONDS
         |                            |
         |                   Applies template to
         |                   restart the server
         v                            v
+-------------------------------------------+
|           GlobalVariables MT5             |
+-------------------------------------------+
```

## Troubleshooting

### Server won't start
- Verify "Allow DLL imports" is enabled
- Check that `libzmq.dll` is in the `Libraries/` folder
- Check logs in the "Experts" tab

### Monitor can't find the server
- Set `SERVER_CHART_ID` manually
- Verify both EAs are on the same symbol

### Auto-restart doesn't work
- Verify the template `ZeroMQ_Server_EA.tpl` exists
- Template must be in `MQL5/Profiles/Templates/`

## Java Connection

```java
try (MT5Client client = new MT5Client("tcp://127.0.0.1:28282")) {
    Account account = client.getAccount();
    System.out.println("Balance: " + account.getBalance());
}
```

## Requirements

- MetaTrader 5 Build 3000+
- mql-zmq library (branch `fix/mql5_compatibility`)
- Java 21+ for the client
