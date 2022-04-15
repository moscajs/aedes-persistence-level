# aedes-persistence-level

![.github/workflows/ci.yml](https://github.com/moscajs/aedes-persistence-level/workflows/.github/workflows/ci.yml/badge.svg)

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
const { Level } = require('level') // Level >= 8.0.0 is required 
const aedesPersistencelevel = require('aedes-persistence-level')

// instantiate a persistence instance
aedesPersistencelevel(new Level('./mydb'))
```

## License

MIT

[aedes]: https://github.com/mcollina/aedes
[persistence]: https://github.com/mcollina/aedes-persistence
[levelup]: http://npm.im/levelup
