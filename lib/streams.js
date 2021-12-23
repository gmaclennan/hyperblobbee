// @ts-check
const { Transform, Writable } = require('streamx')
const lexi = require('lexicographic-integer')

/** @typedef {ReturnType<import('mutexify')>} Lock */
/** @typedef {(err?: Error | null, value?: Buffer) => void} Callback */
/** @typedef {import('streamx').Readable} Readable */

const SEP = Buffer.from([0])

class BlobWriteStream extends Writable {
  /** @private */
  _db
  /** @private */
  _lock
  /**
   * @type {Parameters<Lock>[0] | null}
   * @private */
  _release
  /**
   * @type {Buffer[]}
   * @private
   */
  _buffered
  /** @private */
  _seq
  /** @private */
  _key

  /**
   * @param {any} db Hyperbee instance
   * @param {string} key
   * @param {Lock} lock
   * @param {Object} opts
   * @param {number} opts.blockSize Size (bytes) of each block in storage
   * @param {number} opts.bufferSize Size (bytes) to buffer before writing in a batch. A larger number will result in faster writes (larger batches) but greater memory usage.
   */
  constructor(db, key, lock, { blockSize, bufferSize, ...opts }) {
    super(opts)
    this._db = db
    this._key = Buffer.from(key)
    this._lock = lock
    this._release = null
    this._buffered = []
    this._bufferedBytes = 0
    this._bufferSize = bufferSize
    this._blockSize = blockSize
    this._seq = 0
  }

  /** @param {Callback} cb */
  _open(cb) {
    this._db.ready().then(
      () => {
        this._lock((release) => {
          this._release = release
          this._batch = this._db.batch()
          return cb(null)
        })
      },
      (/** @type {Error} */ err) => cb(err)
    )
  }

  /** @param {Callback} cb */
  _final(cb) {
    this._append(async (/** @type {Error} */ err) => {
      if (err) return cb(err)
      if (this._bufferedBytes > this._blockSize) {
        // this shouldn't happen - means there is a bug in the chunking code
        return cb(new Error('BUG: _final() called with unprocessed data'))
      }
      if (this._buffered.length) {
        const key = this._getNextId()
        const block = Buffer.concat(this._buffered, this._bufferedBytes)
        await this._batch.put(key, block)
      }
      await this._batch.flush()
      this._batch = null
      this._buffered = []
      this._release && this._release()
      return cb(null)
    })
  }

  /** @param {Callback} cb */
  async _append(cb) {
    if (!this._buffered.length) return cb(null)
    while (this._bufferedBytes >= this._blockSize) {
      this._bufferedBytes -= this._blockSize

      // Assemble the buffers that will compose the final block
      const blockBufs = []
      let blockBufsBytes = 0
      while (blockBufsBytes < this._blockSize) {
        const b = this._buffered.shift()
        if (!b) break

        if (blockBufsBytes + b.length <= this._blockSize) {
          blockBufs.push(b)
          blockBufsBytes += b.length
        } else {
          // If the last buffer is larger than needed for the block, just
          // use the needed part
          const neededSize = this._blockSize - blockBufsBytes
          blockBufs.push(b.slice(0, neededSize))
          blockBufsBytes += neededSize
          this._buffered.unshift(b.slice(neededSize))
        }
      }
      const key = this._getNextId()
      await this._batch.put(key, Buffer.concat(blockBufs, this._blockSize))
    }

    if (this._batch.length < this._bufferSize / this._blockSize) return cb(null)

    this._batch.flush().then(
      () => {
        this._batch = this._db.batch()
        return cb(null)
      },
      (/** @type {Error} */ err) => {
        this._batch = null
        this._buffered = []
        return cb(err)
      }
    )
  }

  _getNextId() {
    return encodeBlockKey(this._key, this._seq++)
  }

  /**
   * @param {Buffer} data
   * @param {Callback} cb
   */
  _write(data, cb) {
    this._buffered.push(data)
    this._bufferedBytes += data.length
    if (this._bufferedBytes >= this._bufferSize) return this._append(cb)
    return cb(null)
  }
}

class BlobReadStream extends Transform {
  /**
   * @param {any} db Hyperbee instance
   * @param {string} key
   * @param {any} [opts] Options passed to streamx.Readable
   */
  constructor(db, key, opts = {}) {
    super(opts)
    const rs = db.createReadStream({
      gt: Buffer.concat([Buffer.from(key), SEP]),
      lt: Buffer.concat([Buffer.from(key), SEP, Buffer.from([0xff])]),
      limit: -1,
    })
    rs.pipe(this)
  }

  /**
   * @param {{ seq: number, value: Buffer, key: string }} chunk
   * @param {Callback} cb
   */
  _transform({ value }, cb) {
    cb(null, value)
  }
}

module.exports = {
  BlobReadStream,
  BlobWriteStream,
}

/**
 * @param {Buffer} key
 * @param {number} seq
 */
function encodeBlockKey(key, seq) {
  return Buffer.concat([key, SEP, Buffer.from(lexi.pack(seq))])
}
