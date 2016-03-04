<p align="center">
  <a href="https://github.com/tile38/draft"><img 
    src="https://raw.githubusercontent.com/tile38/something/master/doc/logo1500.png" 
    width="200" height="200" border="0" alt="Tile38"></a>
</p>

Tile38 is an open source (MIT licensed), in-memory geolocation data store, spatial index, and realtime geofence. It supports a variety of object types including lat/lon points, bounding boxes, XYZ tiles, Geohashes, and GeoJSON. 

## Features

- Spatial index with [search](#searching) methods such as Nearby, Within, and Intersects.
- Realtime [geofencing](#geofencing).
- Variety of client protocols, including [http](#http) (curl), [websockets](#websockets), [telnet](#telnet), and a [native interface](#native-interface).
- Server responses are in json.
- Full [command line interface](#cli).
- Leader / follower [replication](#replication).
- Simliar feel and syntax style to the fantastic [Redis](http://redis.io) api.
- Written 100% in [Go](https://golang.org).
- Very high performance.

## Components
- `tile38-server ` - The server
- `tile38-cli    ` - Command line interface tool

## Building Tile38
Tile38 can be compiled and used on Linux, OSX, Windows, FreeBSD, and probably others since the codebase is 100% Go. We support both 32 bit and 64 bit systems. [Go 1.6+](https://golang.org/dl/) must be installed on the build machine.

To build everything simply:
```
$ make
```

To test:
```
$ make test
```

## Running 
For command line options invoke:
```
$ ./tile38-server -h
```

To run a single server:

```
$ ./tile38-server

# The tile38 shell connects to localhost:9851
$ ./tile38-cli
> help
```

## <a name="cli"></a>Playing with Tile38

Basic operations:
```
$ ./tile38-cli

# add a couple of points named 'truck1' and 'truck2' to a collection named 'fleet'.
> set fleet truck1 point 33.5123 -112.2693   # on the Loop 101 in Phoenix
> set fleet truck2 point 33.4626 -112.1695   # on the I-10 in Phoenix

# search the 'fleet' collection.
> scan fleet                                 # returns both trucks in 'fleet'
> nearby fleet point 33.462 -112.268 6000    # search 6 kilometers around a point. returns one truck.

# crud operations
> get fleet truck1                           # returns 'truck1'
> del fleet truck2                           # deletes 'truck2'
> drop fleet                                 # removes all 
```

## Fields
Fields are extra data that belongs to an object. A field is always a double precision floating point. There is no limit to the number of fields that an object can have. 

To set a field when setting an object:
```
> set fleet truck1 field speed 90 point 33.5123 -112.2693             
> set fleet truck1 field speed 90 field age 21 point 33.5123 -112.2693
```

To set a field when an object already exists:
```
> fset fleet truck1 speed 90
```

## Searching

<img src="https://raw.githubusercontent.com/tile38/something/master/doc/search-within.png" width="200" height="200" border="0" alt="Search Within" align="left">
#### Within 
WITHIN searches a collection for objects that are fully contained inside a specified bounding area.
<BR CLEAR="ALL">

<img src="https://raw.githubusercontent.com/tile38/something/master/doc/search-intersects.png" width="200" height="200" border="0" alt="Search Intersects" align="left">
#### Intersects
INTERSECTS searches a collection for objects that intersect a specified bounding area.
<BR CLEAR="ALL">

<img src="https://raw.githubusercontent.com/tile38/something/master/doc/search-nearby.png" width="200" height="200" border="0" alt="Search Nearby" align="left">
#### Nearby
NEARBY searches a collection for objects that intersect a specified radius.
<BR CLEAR="ALL">


### Search options
**SPARSE** - This option will distribute the results of a search evenly across the requested area.  
This is very helpful for example; when you have many (perhaps millions) of objects and do not want them all clustered together on a map. Sparse will limit the number of objects returned and provide them evenly distributed so that your map looks clean.<br><br>
You can choose a value between 1 and 8. The value 1 will result in no more than 4 items. The value 8 will result in no more than 65536. *1=4, 2=16, 3=64, 4=256, 5=1024, 6=4098, 7=16384, 8=65536.*<br><br>
<table>
<td>No Sparsing<img src="https://raw.githubusercontent.com/tile38/something/master/doc/sparse-none.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 1<img src="https://raw.githubusercontent.com/tile38/something/master/doc/sparse-1.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 2<img src="https://raw.githubusercontent.com/tile38/something/master/doc/sparse-2.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 3<img src="https://raw.githubusercontent.com/tile38/something/master/doc/sparse-3.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 4<img src="https://raw.githubusercontent.com/tile38/something/master/doc/sparse-4.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 5<img src="https://raw.githubusercontent.com/tile38/something/master/doc/sparse-5.png" width="100" height="100" border="0" alt="Search Within"></td>
<td>Sparse 6<img src="https://raw.githubusercontent.com/tile38/something/master/doc/sparse-6.png" width="100" height="100" border="0" alt="Search Within"></td>
</table>
*Please note that higher the sparse value, the slower the performance. Also, LIMIT and CURSOR are not available when using SPARSE.* 

**WHERE** - This option allows for filtering out results based on [field](#fields) values. For example<br>```nearby fleet where speed 70 +inf point 33.462 -112.268 6000``` will return only the objects in the 'fleet' collection that are within the 6 km radius **and** have a field named `speed` that is greater than `70`. <br><br>Multiple WHEREs are concatenated as **and** clauses. ```WHERE speed 70 +inf WHERE age -inf 24``` would be interpreted as *speed is over 70 <b>and</b> age is less than 24.*<br><br>The default value for a field is always `0`. Thus if you do a WHERE on the field `speed` and an object does not have that field set, the server will pretend that the object does and that the value is Zero.

**MATCH** - MATCH is similar to WHERE expect that it works on the object id instead of fields.<br>```nearby fleet match truck* point 33.462 -112.268 6000``` will return only the objects in the 'fleet' collection that are within the 6 km radius **and** have an object id that starts with `truck`. There can be multiple MATCH options in a single search. The MATCH value is a simple [glob pattern](https://en.wikipedia.org/wiki/Glob_(programming)).

**CURSOR** - CURSOR is used to iterate though many objects from the search results. An iteration begins when the CURSOR is set to Zero or not included with the request, and completes when the cursor returned by the server is Zero.

**NOFIELDS** - NOFIELDS tells the server that you do not want field values returned with the search results.

**LIMIT** - LIMIT can be used to limit the number of objects returned for a single search request.


## Geofencing

<img src="https://raw.githubusercontent.com/tile38/something/master/doc/geofence.gif" width="200" height="200" border="0" alt="Geofence animation" align="left">
A [geofence](https://en.wikipedia.org/wiki/Geo-fence) is a virtual boundary that can detect when an object enters or exits the area. This boundary can be a radius, bounding box, or a polygon. Tile38 can turn any standard search into a geofence monitor by adding by the FENCE keyword to the search. 
<br clear="all">

A simple example:
```
> nearby fleet fence point 33.462 -112.268 6000
```
This command opens a geofence that monitors the 'fleet' collection. The server will respond with:
```
{"ok":true,"live":true}
```
And the connection will be kept open. If any object enters or exits the 6 km radius around `33.462,-112.268` the server will respond in realtime with a message such as:

```
{"command":"set","detect":"enter","id":"truck02","object":{"type":"Point","coordinates":[-112.2695,33.4626]}}
```

The server will notify the client if the `command` is `del | set | drop`. 

- `del` is when an object has been deleted from the collection that is being fenced.
- `drop` is when the entire collection is dropped.
- `set` is when an object has been added or updated, and when it's position is detected by the fence.

The `detect` may be `enter | exit | cross`.

- `enter` is when an object that **was not** previously in the fence has entered the area.
- `exit` is when an object that **was** previously in the fence has exited the area.
- `cross` is when an object that **was not** previously in the fence has entered and exited the area.


## Network protocols

It's recommended to use the [native interface](#native-interface), but there are times when only HTTP is available or when you need to test from a remote terminal. In those cases we provide an HTTP and telnet options.

### HTTP
One of the simplest ways to call a tile38 command is to use HTTP. From the command line you can use [curl](https://curl.haxx.se/). For example:

```
# call with request in the body
curl --data "set fleet truck3 point 33.4762 -112.10923" localhost:9851

# call with request in the url path
curl localhost:9851/set+fleet+truck3+point+33.4762+-112.10923
```

### Websockets
Websockets can be used when you need to Geofence and keep the connection alive. It works just like the HTTP example above, with the exception that the connection stays alive and the data is sent from the server as text websocket messages.

### Telnet
There is the option to use a plain telnet connection.

```
telnet localhost 9851
set fleet truck3 point 33.4762 -112.10923
{"ok":true,"elapsed":"18.73µs"}
```

### Native interface
The native interface is very simple. A single message is composed of a '$' + TEXT_DATA_SIZE + SPACE + DATA + CRLF.

So the request message:
```
get fleet truck1
```

Should be sent to the server as (without quotes):

```c
"$16 get fleet truck1\r\n"
```

The server responds will always respond in JSON, and will include the top level member `ok`. When `ok` is `false` there will also be an accompanied `err` member describing the problem. In nearly every response there will also be an `elapsed` member that is the duration of time that it took to process the request on the server. For more information on this string please refer to the [time.Duration](https://golang.org/pkg/time/#Duration) Go documentation.

So the response message:
```json
{"ok":true,"elapsed":"37.829µs"}
```
Will be sent to the client as (without quotes):

```c
"$32 {"ok":true,"elapsed":"37.829µs"}\r\n"
```

## Clients
Currently we have only one native client written in Go. Though is should be trivial to write one in your language of choice.

- [Go](https://github.com/tidwall/tile38/tree/master/client)


## Commands
This a the full list of commands availble to Tile38. 

### Crud
```md
GET key id [OBJECT|POINT|BOUNDS|(HASH precision)]
summary: Get the object of an id

SET key string [FIELD name value ...] (OBJECT geojson)|(POINT lat lon [z])|(BOUNDS minlat minlon maxlat maxlon)|(HASH geohash)
summary: Sets the value of an id

FSET key id field value
summary: Set the value for a single field of an id

DEL key id
summary: Delete an id from a key

DROP key
summary: Remove a key from the database

KEYS pattern
summary: Finds all keys matching the given pattern

STATS key [key ...]
summary: Show stats for one or more keys
```


### Search
```md
INTERSECTS key [CURSOR start] [LIMIT count] [SPARSE spread] [MATCH pattern] [WHERE field min max ...] [NOFIELDS] [FENCE] [COUNT|IDS|OBJECTS|POINTS|BOUNDS|(HASHES precision)] (GET key id)|(BOUNDS minlat minlon maxlat maxlon)|(OBJECT geojson)|(TILE x y z)|(QUADKEY quadkey)|(HASH precision)
summary: Searches for ids that are nearby a point

NEARBY key [CURSOR start] [LIMIT count] [SPARSE spread] [MATCH pattern] [WHERE field min max ...] [NOFIELDS] [FENCE] [COUNT|IDS|OBJECTS|POINTS|BOUNDS|(HASHES precision)] POINT lat lon meters
summary: Searches for ids that are nearby a point

WITHIN key [CURSOR start] [LIMIT count] [SPARSE spread] [MATCH pattern] [WHERE field min max ...] [NOFIELDS] [FENCE] [COUNT|IDS|OBJECTS|POINTS|BOUNDS|(HASHES precision)] (GET key id)|(BOUNDS minlat minlon maxlat maxlon)|(OBJECT geojson)|(TILE x y z)|(QUADKEY quadkey)|(HASH precision)
summary: Searches for ids that are nearby a point

SCAN key [CURSOR start] [LIMIT count] [MATCH pattern] [WHERE field min max ...] [NOFIELDS] [COUNT|IDS|OBJECTS|POINTS|BOUNDS|(HASHES precision)]
summary: Incrementally iterate though a key
```

### Server

```md
PING
summary: Ping the server

SERVER
summary: Show server stats and details

FLUSHDB
summary: Removes all keys

GC
summary: Forces a garbage collection

READONLY yes|no
summary: Turns on or off readonly mode
```

### Replication
```md
FOLLOW host port
summary: Follows a leader host

AOFSHRINK
summary: Shrinks the aof in the background

AOF pos
summary: Downloads the AOF start from pos and keeps the connection alive

AOFMD5 pos size
summary: Performs a checksum on a portion of the aof
```

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Tile38 source code is available under the MIT License.






