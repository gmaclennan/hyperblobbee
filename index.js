// @ts-check
const mutexify = require('mutexify')

const { BlobReadStream, BlobWriteStream } = require('./lib/streams')

const DEFAULT_BLOCK_SIZE = 2 ** 16

/**
 * @typedef {Object} Options
 * @property {number} [blockSize=2**16] The block size that will be used when storing large blobs.
 */

module.exports = class Hyperblobee {
  /** @private */
  _blockSize
  /** @private */
  _lock
  /** @private */
  _db

  /**
   * @param {any} db Hyperbee instance
   * @param {Pick<Options, 'blockSize'>} [opts]
   */
  constructor(db, opts = {}) {
    this._blockSize = opts.blockSize || DEFAULT_BLOCK_SIZE
    this._lock = mutexify()
    this._db = db.sub(Buffer.alloc(0), {
      sep: Buffer.alloc(0),
      valueEncoding: 'binary',
      keyEncoding: 'utf-8',
    })
  }

  get blockSize() {
    return this._blockSize
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
    const blockSize = opts.blockSize || this._blockSize

    const stream = this.createWriteStream(key, opts)
    for (let i = 0; i < blob.length; i += blockSize) {
      stream.write(blob.slice(i, i + blockSize))
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
   * @param {Options} [opts]
   */
  createReadStream(key, opts) {
    return new BlobReadStream(this._db, key, opts)
  }

  /**
   * @param {string} key
   * @param {Options} [opts]
   */
  createWriteStream(key, opts) {
    return new BlobWriteStream(this._db, key, this._lock, opts)
  }
}
