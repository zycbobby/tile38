# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [1.5.1] - 2016-10-19
### Fixed
- #67: Call the EXPIRE command hangs the server
- #64: Missing points in 'Nearby' queries

## [1.5.0] - 2016-10-03
### Added
- #61: Optimized queries on 3d objects
- #60: Added [NX|XX] keywords to SET command
- #29: Generalized hook interface
- GRPC geofence hook support 

### Fixed
- #62: Potential Replace Bug Corrupting the Index
- #57: CRLF codes in info after bump from 1.3.0 to 1.4.2

## [1.4.2] - 2016-08-26
### Fixed
- #49. Allow fragmented pipeline requests
- #51: Allow multispace delim in native proto
- #50: MATCH with slashes 
- #43: Linestring nearby search correction

## [1.4.1] - 2016-08-26
### Added
- #34: Added "BOUNDS key" command

### Fixed
- #38: Allow for nginx support
- #39: Reset requirepass 

## [1.3.0] - 2016-07-22
### Added
- New EXPIRE, PERSISTS, TTL commands. New EX keyword to SET command
- Support for plain strings using `SET ... STRING value.` syntax
- New SEARCH command for finding strings
- Scans can now order descending

### Fixed
- #28: fix windows cli issue

## [1.2.0] - 2016-05-24
### Added
- #17: Roaming Geofences for NEARBY command
- #15: maxmemory config setting

## [1.1.4] - 2016-04-19
### Fixed
- #12: Issue where a newline was being added to HTTP POST requests
- #13: OBJECT keyword not accepted for WITHIN command
- Panic on missing key for search requests

## [1.1.2] - 2016-04-12
### Fixed
- A glob suffix wildcard can result in extra hits
- The native live geofence sometimes fails connections

## [1.1.0] - 2016-04-02
### Added
- Resp client support. All major programming languages now supported
- Added WITHFIELDS option to GET
- Added OUTPUT command to allow for outputing JSON when using RESP
- Added DETECT option to geofences

### Changes
- New AOF file structure.
- Quicker and safer AOFSHRINK.

### Deprecation Warning
- Native protocol support is being deprecated in a future release in favor of RESP
