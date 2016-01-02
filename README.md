# aedes-persistence-level&nbsp;&nbsp;[![Build Status](https://travis-ci.org/mcollina/aedes-persistence-level.svg)](https://travis-ci.org/mcollina/aedes-persistence-level)

[Aedes][aedes] [persistence][persistence], backed by [levelup][levelup].

See [aedes-persistence][persistence] for the full API, and [Aedes][aedes] for usage.

## Install

```
npm i aedes aedes-persistence-level level --save
```

## API

<a name="constructor"></a>
### aedesPersistencelevel(db)

Creates a new instance of aedes-persistence-level.
The first parameter is an instance of [levelup][levelup].

Example:

```js
var level = require('level')
var aedesPersistencelevel = require('aedes-perisistence-level')

// instantiate a persistence instance
aedesPersistencelevel(level('./mydb'))
```

## License

MIT

[aedes]: https://github.com/mcollina/aedes
[persistence]: https://github.com/mcollina/aedes-persistence
[levelup]: http://npm.im/levelup
