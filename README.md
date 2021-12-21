# Hyperblobee

:construction: **This is currently experimental and is unstable**

![Test on Node.js](https://github.com/gmaclennan/hyperblobee/workflows/Test%20on%20Node.js/badge.svg)

A simple blob store for [Hypercore](https://github.com/hypercore-protocol/hypercore-next) built on [Hyperbee](https://github.com/hypercore-protocol/hyperbee), heavily inspired by [andrewosh/hyperblobs](https://github.com/andrewosh/hyperblobs).

Each blob is identified by unique (utf-8) key.

```js
const db = new Hyperbee(core)
const blobs = new Hyperblobee(db)

const key = 'my-first-file'
await blobs.put(key, Buffer.from('hello world', 'utf-8'))
await blobs.get(key) // Buffer.from('hello world', 'utf-8')
```

If the blob is large, there's a Streams interface (`createReadStream` and `createWriteStream`) too.

## Installation

```
npm i hyperblobee
```

## API

`const Hyperblobee = require('hyperblobee')`

#### `const blobs = new Hyperblobee(db, opts)`

Create a new blob store wrapping a single Hyperbee instance.

Options can include:

```js
{
  blockSize: 64KB // The block size that will be used when storing large blobs.
}
```

#### `await blobs.put(key, blob, opts)`

Store a new blob at `key`. If the blob is large, it will be chunked according to `opts.blockSize` (default 64KB).

Options can include:

```js
{
  blockSize: 64KB, // The block size that will be used when storing large blobs.
}
```

#### `const stream = blobs.createReadStream(key, opts)`

Create a Readable stream that will yield the `key` blob.

Options match the `get` options.

#### `const stream = blobs.createWriteStream(key, opts)`

Create a Writable stream that will save a blob with `key`.

## License

MIT
