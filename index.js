// @ts-check
const mutexify = require('mutexify')

const { BlobReadStream, BlobWriteStream } = require('./lib/streams')

const DEFAULT_BLOCK_SIZE = 2 ** 16

/**
 * @typedef {Object} Options
 * @property {number} [blockSize=2**16] The block size that will be used when storing large blobs.
 * @property {number} [start=0] Relative offset to start within the blob
 * @property {number} [end=blob.length - 1] End offset within the blob (inclusive)
 * @property {number} [length=blob.length] Number of bytes to read.
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
   * @param {string} id
   * @param {Buffer} blob
   * @param {Options} [opts]
   */
  async put(id, blob, opts = {}) {
    const blockSize = opts.blockSize || this._blockSize

    const stream = this.createWriteStream(id, opts)
    for (let i = 0; i < blob.length; i += blockSize) {
      stream.write(blob.slice(i, i + blockSize))
    }
    stream.end()

    return new Promise((resolve, reject) => {
      stream.once('error', reject)
      stream.once('close', () => resolve(stream.id))
    })
  }

  /**
   * @param {string} id
   * @param {Options} [opts]
   */
  async get(id, opts) {
    const res = []
    for await (const block of this.createReadStream(id, opts)) {
      res.push(block)
    }
    return Buffer.concat(res)
  }

  /**
   * @param {string} id
   * @param {Options} [opts]
   */
  createReadStream(id, opts) {
    return new BlobReadStream(this._db, id, opts)
  }

  /**
   * @param {string} id
   * @param {Options} [opts]
   */
  createWriteStream(id, opts) {
    return new BlobWriteStream(this._db, id, this._lock, opts)
  }
}
