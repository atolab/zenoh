## 0.2.6 (2019-05-14)

- Zenoh Router Lib : Applications can run the zenoh router as a library and interract with it through the same API as client applications.

## 0.2.5 (2019-04-18)

- Management : zenoh routers answer to management queries.
- Bug fixes in the discovery engine.

## 0.2.4 (2019-03-22)

- Data context : possibility to attach context (timestamp, data kind and data encoding) to data samples.
- Batched stream : possibility to send data samples in batches to improve performances.
- API changes : zenoh engine is the first argument of functions rather than last argument.
- Performances improvements

## 0.2.3 (2019-02-22)

- API changes : 
    - New lquery function with automatic (reception side) replies consolidation.
    - New squery function with replies provided through a Lwt_stream.
- Performances improvements

## 0.2.2 (2019-02-11)

- Fix bug when multiple storage declarations in the same client
- Fix bugs in high throughput situations

## 0.2.1 (2019-01-25)

- Query improvement : destinations options

## 0.2.0 (2019-01-11)

First public release.

- Pub/Sub protocol
- Query/Reply protocol
- TCP transport 
- Single spanning tree routing