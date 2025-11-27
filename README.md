# MetaTrader 4/5 Server
Provides a remote interface and high-level API for MetaTrader 4 and MetaTrader 5 via ZeroMQ sockets.\
See [API](docs/api.md) for supported operations.

![Diagram 1](diagram_1.png)

## Supported Platforms

| Platform | Status | Directory |
|----------|--------|-----------|
| MetaTrader 4 | Stable | [metatrader4/](metatrader4/) |
| MetaTrader 5 | Stable | [metatrader5/](metatrader5/) |

## MetaTrader 4 Installation

1. Copy [metatrader4](metatrader4) into your MetaTrader 4 profile directory, merging the
folder contents.
1. Copy [mql-zmq/Include/](https://github.com/danmigdev/mql-zmq/tree/fix/mql5_compatibility/Include) into the `MQL4/Include` subdirectory of your MetaTrader 4 profile directory, merging the
folder contents.
1. Copy [mql-zmq/Library/MT4/](https://github.com/danmigdev/mql-zmq/tree/fix/mql5_compatibility/Library/MT4) into the `MQL4/Libraries` subdirectory of your MetaTrader 4 profile directory, merging the
folder contents.
1. Launch MetaEditor, open `MQL4/Scripts/ZeroMQ_Server.mq4` in your MetaTrader 4 profile directory and compile it.
1. Start MetaTrader 4 and add the `ZeroMQ Server` script to any chart (chart symbol does not matter). The server begins
listening for client requests and responds synchronously.

## MetaTrader 5 Installation

1. Copy [metatrader5/MQL5](metatrader5/MQL5) into your MetaTrader 5 profile directory, merging the folder contents.
1. Copy [mql-zmq/Include/](https://github.com/danmigdev/mql-zmq/tree/fix/mql5_compatibility/Include) into the `MQL5/Include` subdirectory of your MetaTrader 5 profile directory, merging the folder contents.
1. Copy [mql-zmq/Library/MT5/](https://github.com/danmigdev/mql-zmq/tree/fix/mql5_compatibility/Library/MT5) into the `MQL5/Libraries` subdirectory of your MetaTrader 5 profile directory, merging the folder contents.
1. Launch MetaEditor, open and compile:
   - `MQL5/Experts/ZeroMQ_Server_EA.mq5`
   - `MQL5/Experts/ZeroMQ_Monitor_EA.mq5` (optional watchdog)
1. Start MetaTrader 5 and add `ZeroMQ_Server_EA` to any chart.

> **Note:** For MT5 detailed setup instructions, see [metatrader5/MQL5/Experts/README.md](metatrader5/MQL5/Experts/README.md)

### MT5 Features

- **Expert Advisor**: Runs as EA instead of script, supports templates for auto-restart
- **Watchdog Monitor**: `ZeroMQ_Monitor_EA` monitors server health and auto-restarts if unresponsive
- **Long Ticket Support**: MT5 uses `long` (64-bit) ticket numbers

### MT5 Library Compatibility

MT5 requires the `fix/mql5_compatibility` branch of mql-zmq to fix `char[]/uchar[]` type conversion errors:

```
https://github.com/danmigdev/mql-zmq/tree/fix/mql5_compatibility
```

## Configuration
The default listening port is `TCP/28282` but is configurable in the script/EA parameters popup in the MetaTrader terminal,
along with other parameters such as socket timeouts. If Windows Defender Firewall is running, you must forward the port.

## Usage
Typical client usage:

1. Create a ZeroMQ context and a `REQ` socket with appropriate send/receive timeouts and options.
1. Connect the `REQ` socket to the server's `REP` socket.
1. Perform the following sequence any number of times:
    1. Construct an [API](docs/api.md) request.
    1. Send the request to the `REQ` socket.
    1. Receive a JSON-formatted string response from the `REQ` socket.
1. Close the socket connection and destroy the ZeroMQ context.

It is recommended to use one of the following client libraries to abstract away these details:

| Client | MT4 | MT5 | Repository |
|--------|-----|-----|------------|
| Python | Yes | Yes | [metatrader4-client-python](https://github.com/CoeJoder/metatrader4-client-python) |
| Java   | Yes | Yes | [metatrader4-client-java](https://github.com/danmigdev/metatrader4-client-java) |

### Java Client Example

```java
// MT4 Client
try (MT4Client client = new MT4Client("tcp://127.0.0.1:28282")) {
    Account account = client.getAccount();
    System.out.println("Balance: " + account.getBalance());
}

// MT5 Client (supports long ticket numbers)
try (MT5Client client = new MT5Client("tcp://127.0.0.1:28282")) {
    List<MT5Order> orders = client.getOrders();
    for (MT5Order order : orders) {
        System.out.println("Ticket: " + order.getTicket()); // long type
    }
}
```

## Limitations
The `REQ-REP` socket connection enforces a strict request-response cycle and may deadlock if connection is lost.
The client libraries use `ZMQ_REQ_RELAXED` and `ZMQ_REQ_CORRELATE` socket options to prevent this in most cases.
If a response is dropped, the client may want to catch the exception and check whether or not the operation was
successful.

## Development
If you want to make changes to the server implementation, it's useful to setup a [local dev environment](docs/dev.md).
