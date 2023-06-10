# ethutils
Ethereum utility codes

## EthLogSubscriber

```
ctx = EthLogSubscriber(wss_provider_url, address, event_abi, callback, fromDate=datetime.datetime, dateTolerance=60)
ctx.start()
ctx.stop()
```

Neither web3.js nor web3.py have a convenient way to get historical logs and
then stay subscribed to async capture new ones.  At least web3.js has an
implementation of `eth_subscribe`; web3.py doesn't even have that.
`EthLogSubscriber` combines historial lookup and ongoing subscription into a
tidy package that hides thread, async, and ethereum encode/decode details.
The built-in `main()` offers a example of how to use it.  Features:

 *  Logic properly sets up async notification *before* looking up historical
    information.   This ensures seamless transition from vending historic logs
    to real-time subscription without danger of creating a gap between
    processing the last historic log and the setup of the websocket listener.
 *  An event ABI is used to facilitate decoding the pile of bytes in the
    `data` portion of the log into a well-structured dictionary of names and
    real types.
 *  The web3 JSON-RPC has no facility to lookup a block by timestamp.  As this
    is a frequent use case, it is implemented in EthLogSubscriber.  The closest
    block *after* the given `fromDate` will be used as a starting point.
 *  Nothing happens until `start()` is called.   This permits separation of the
    locus of initialization from the logic where the callback will be active.
 *  Subscription can be formally shut down with the `stop()` method. 