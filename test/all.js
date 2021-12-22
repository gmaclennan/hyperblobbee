const test = require('tape')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const ram = require('random-access-memory')
const { Writable } = require('streamx')
const Hyperblobee = require('..')

test('can get/put a large blob', async (t) => {
  const core = new Hypercore(ram)
  const db = new Hyperbee(core)
  const blobs = new Hyperblobee(db)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = 'foo'
  await blobs.put(id, buf)
  const result = await blobs.get(id)
  t.true(result.equals(buf))

  t.end()
})

test('can put/get two blobs in one core', async (t) => {
  const core = new Hypercore(ram)
  const db = new Hyperbee(core)
  const blobs = new Hyperblobee(db)

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

test('block size is respected for streams', async (t) => {
  const core = new Hypercore(ram)
  const db = new Hyperbee(core)
  const blobs = new Hyperblobee(db)
  const blockSize = 2 ** 16

  const buf = Buffer.alloc(5 * blockSize).fill('abcdefg')
  const id = 'foo'
  const ws = blobs.createWriteStream(id)
  for (let i = 0; i < buf.length; i += blockSize * 0.75) {
    ws.write(buf.slice(i, i + blockSize))
  }
  ws.end()
  
  
  const result = await blobs.db.get(id + '/0')
  t.equals(result.length, blockSize)

  t.end()
})

test.skip('can seek to start/length within one blob, one block', async (t) => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobee(core)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, length: 2 })
  t.true(result.toString('utf-8'), 'cd')

  t.end()
})

test.skip('can seek to start/length within one blob, multiple blocks', async (t) => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobee(core, { blockSize: 10 })

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
  const blobs = new Hyperblobee(core, { blockSize: 10 })

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
  const blobs = new Hyperblobee(core)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, end: 4 }) // inclusive
  t.true(result.toString('utf-8'), 'cd')

  t.end()
})

test.skip('basic seek', async (t) => {
  const core = new Hypercore(ram)
  const blobs = new Hyperblobee(core)

  const buf = Buffer.alloc(5 * blobs.blockSize).fill('abcdefg')
  const id = await blobs.put(buf)
  const result = await blobs.get(id, { start: 2, end: 4 }) // inclusive
  t.true(result.toString('utf-8'), 'cd')

  t.end()
})
