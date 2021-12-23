// @ts-check
const mutexify = require('mutexify')

const { BlobReadStream, BlobWriteStream } = require('./lib/streams')

const DEFAULT_BLOCK_SIZE = 1024 * 512 // 512KB
const DEFAULT_BUFFER_SIZE = 1024 * 1024 * 10 // 10MB

/**
 * @typedef {Object} Options
 * @property {number} [blockSize] The block size that will be used when storing large blobs.
 * @property {number} [bufferSize] The size of the buffer used when writing large blobs.
 */

module.exports = class Hyperblobbee {
  /** @private */
  _blockSize
  /** @private */
  _bufferSize
  /** @private */
  _lock
  /** @private */
  _db

  /**
   * @param {any} db Hyperbee instance
   * @param {Pick<Options, 'blockSize' | 'bufferSize'>} [opts]
   */
  constructor(db, opts = {}) {
    this._blockSize = opts.blockSize || DEFAULT_BLOCK_SIZE
    this._bufferSize = opts.bufferSize || DEFAULT_BUFFER_SIZE
    this._lock = mutexify()
    this._db = db.sub(Buffer.alloc(0), {
      sep: Buffer.alloc(0),
      valueEncoding: 'binary',
      keyEncoding: 'binary',
    })
  }

  get blockSize() {
    return this._blockSize
  }

  get bufferSize() {
    return this._bufferSize
  }

  /**
   * @returns {any} Hyperbee instance
   */
  get db() {
    return this._db
  }

  get locked() {
    return this._lock.locked
  }

  /**
   * @param {string} key
   * @param {Buffer} blob
   * @param {Options} [opts]
   */
  async put(key, blob, opts = {}) {
    if (!opts.blockSize) opts.blockSize = this._blockSize

    const stream = this.createWriteStream(key, opts)
    for (let i = 0; i < blob.length; i += opts.blockSize) {
      stream.write(blob.slice(i, i + opts.blockSize))
    }
    stream.end()

    return new Promise((resolve, reject) => {
      stream.once('error', reject)
      stream.once('close', () => resolve(stream))
    })
  }

  /**
   * @param {string} key
   * @param {Options} [opts]
   */
  async get(key, opts) {
    const res = []
    for await (const block of this.createReadStream(key, opts)) {
      res.push(block)
    }
    return Buffer.concat(res)
  }

  /**
   * @param {string} key
   * @param {Options} [opts] - TODO: Fix this type, shouldn't include `blockSize`
   */
  createReadStream(key, opts) {
    return new BlobReadStream(this._db, key, opts)
  }

  /**
   * @param {string} key
   * @param {Options} [opts]
   */
  createWriteStream(key, opts = {}) {
    return new BlobWriteStream(this._db, key, this._lock, {
      ...opts,
      blockSize: opts.blockSize || this._blockSize,
      bufferSize: opts.bufferSize || this._bufferSize,
    })
  }
}
