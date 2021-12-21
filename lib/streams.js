// @ts-check
const { Transform, Writable } = require('streamx')

/** @typedef {ReturnType<import('mutexify')>} Lock */
/** @typedef {(err?: Error | null) => void} Callback */
/** @typedef {import('streamx').Readable} Readable */

const SEP = '/'

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
  _batch
  /** @private */
  _seq
  /** @private */
  _key

  /**
   * @param {any} db Hyperbee instance
   * @param {string} key
   * @param {Lock} lock
   * @param {any} [opts] Options passed to streamx.Writable
   */
  constructor(db, key, lock, opts) {
    super(opts)
    this._db = db
    this._key = key
    this._lock = lock
    this._release = null
    this._batch = []
    this._seq = 0
  }

  /** @param {Callback} cb */
  _open(cb) {
    this._db.ready().then(
      () => {
        this._lock((release) => {
          this._release = release
          return cb(null)
        })
      },
      (/** @type {Error} */ err) => cb(err)
    )
  }

  /** @param {Callback} cb */
  _final(cb) {
    this._append((/** @type {Error} */ err) => {
      if (err) return cb(err)
      this._release && this._release()
      return cb(null)
    })
  }

  /** @param {Callback} cb */
  async _append(cb) {
    if (!this._batch.length) return cb(null)
    const batch = this._db.batch()
    for (this._seq; this._seq < this._batch.length; this._seq++) {
      const key = `${this._key}${SEP}${this._seq}`
      await batch.put(key, this._batch[this._seq])
    }
    return batch.flush().then(
      () => {
        this._batch = []
        return cb(null)
      },
      (/** @type {Error} */ err) => {
        this._batch = []
        return cb(err)
      }
    )
  }

  /**
   * @param {Buffer} data
   * @param {Callback} cb
   */
  _write(data, cb) {
    this._batch.push(data)
    if (this._batch.length >= 16) return this._append(cb)
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
    super({
      ...opts,
      transform({ seq, key, value }, cb) {
        cb(null, value)
      },
    })
    const rs = db.createReadStream({
      gt: `${key}${SEP}`,
      lt: `${key}${SEP}\xff`,
      limit: -1,
    })
    rs.pipe(this)
  }
}

module.exports = {
  BlobReadStream,
  BlobWriteStream,
}
