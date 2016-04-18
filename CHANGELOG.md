# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [1.1.3] - 2016-04-18
### Fixed
- #12: Issue where a newline was being added to HTTP POST requests.

## [1.1.2] - 2016-04-12
### Fixed
- A glob suffix wildcard can result in extra hits.
- The native live geofence sometimes fails connections.

## [1.1.0] - 2016-04-02
### Added
- Resp client support. All major programming languages now supported.
- Added WITHFIELDS option to GET.
- Added OUTPUT command to allow for outputing JSON when using RESP.
- Added DETECT option to geofences.

### Changes
- New AOF file structure.
- Quicker and safer AOFSHRINK.

### Deprecation Warning
- Native protocol support is being deprecated in a future release in favor of RESP.
