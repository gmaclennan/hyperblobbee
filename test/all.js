const test = require('tape')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const ram = require('random-access-memory')
const { once } = require('events')
const Hyperblobbee = require('..')
const lexi = require('lexicographic-integer')

test('can get/put a large blob', async (t) => {
  const core = new Hypercore(ram)
  const db = new Hyperbee(core)
  const blobs = new Hyperblobbee(db)

  // Test a blob larger than the buffer size (defaults to 10MB)
  // Test our chunking by ensuring the blob will not divide evenly
  const buf = Buffer.alloc(blobs.bufferSize * 9.5).fill('abcdefg')
  const id = 'foo'
  await blobs.put(id, buf)
  const result = await blobs.get(id)
  t.true(result.equals(buf))
  t.end()
})

test('can get/put a small blob', async (t) => {
  const core = new Hypercore(ram)
  const db = new Hyperbee(core)
  const blobs = new Hyperblobbee(db)

  // Check something smaller than the block size works
  const buf = Buffer.alloc(blobs.blockSize / 2).fill('abcdefg')
  const id = 'foo'
  await blobs.put(id, buf)
  const result = await blobs.get(id)
  t.true(result.equals(buf))

  t.end()
})

test('can put/get two blobs in one core', async (t) => {
  const core = new Hypercore(ram)
  const db = new Hyperbee(core)
  const blobs = new Hyperblobbee(db)

  {
    const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
    const id = 'foo'
    await blobs.put(id, buf)
    const res = await blobs.get(id)
    t.true(res.equals(buf))
  }

  {
    const buf = Buffer.alloc(5 * blobs.blockSize).fill('hijklmn')
    const id = 'bar'
    await blobs.put(id, buf)
    const res = await blobs.get(id)
    t.true(res.equals(buf))
  }

  t.end()
})

test("block size isn't affected by chunk size of streams", async (t) => {
  const core = new Hypercore(ram)
  const db = new Hyperbee(core)
  const blockSize = 2 ** 16
  const blobs = new Hyperblobbee(db, { blockSize })

  const buf = Buffer.alloc(5 * blockSize).fill('abcdefg')

  // Write chunks to the stream that are smaller and larger than blockSize
  for (const chunkSize of [blockSize / 2, blockSize * 2]) {
    const id = String(chunkSize)
    const ws = blobs.createWriteStream(id)
    for (let i = 0; i < buf.length; i += chunkSize) {
      const chunk = buf.slice(i, i + chunkSize)
      ws.write(chunk)
    }
    ws.end()
    await once(ws, 'finish')
    const { value } = await blobs.db.get(
      Buffer.concat([
        Buffer.from(id),
        Buffer.from([0]),
        Buffer.from(lexi.pack(0)),
      ])
    )
    t.equals(value.length, blockSize)
  }

  t.end()
})

test.skip('can seek to start/length within one blob, one block', async (t) => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobbee(core)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, length: 2 })
  t.true(result.toString('utf-8'), 'cd')

  t.end()
})

test.skip('can seek to start/length within one blob, multiple blocks', async (t) => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobbee(core, { blockSize: 10 })

  const buf = Buffer.concat([
    Buffer.alloc(10).fill('a'),
    Buffer.alloc(10).fill('b'),
  ])
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 8, length: 4 })
  t.true(result.toString('utf-8'), 'aabb')

  t.end()
})

test.skip('can seek to start/length within one blob, multiple blocks, multiple blobs', async (t) => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobbee(core, { blockSize: 10 })

  {
    const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
    const id = await blobs.put(buf)
    const res = await blobs.get(id)
    t.true(res.equals(buf))
  }

  const buf = Buffer.concat([
    Buffer.alloc(10).fill('a'),
    Buffer.alloc(10).fill('b'),
  ])
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 8, length: 4 })
  t.true(result.toString('utf-8'), 'aabb')

  t.end()
})

test.skip('can seek to start/end within one blob', async (t) => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobbee(core)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, end: 4 }) // inclusive
  t.true(result.toString('utf-8'), 'cd')

  t.end()
})

test.skip('basic seek', async (t) => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobbee(core)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, end: 4 }) // inclusive
  t.true(result.toString('utf-8'), 'cd')

  t.end()
})
