const bench = require('@stdlib/bench')
const Hyperdrive = require('hyperdrive')
const Hypercore10 = require('hypercore')
const Hyperbee = require('hyperbee')
const Hyperblobs = require('hyperblobs')
const { once } = require('events')
const { promisify } = require('util')
const Hyperblobbee = require('..')
const tempy = require('tempy')
const del = require('del')

function cleanup(dir) {
  return del(dir, { force: true }).catch((e) =>
    console.log('Error deleting directory', e)
  )
}

function toMb(bytes) {
  return (bytes / 1024 / 1024).toFixed(1) + 'MB'
}

Error.stackTraceLimit = 50

module.exports = (fileSize) => {
  bench(`Hyperblobee: Read ${toMb(fileSize)} file`, async (b) => {
    // setup
    const dir = tempy.directory()
    const core = new Hypercore10(dir)
    const db = new Hyperbee(core)
    const blobs = new Hyperblobbee(db)
    await db.ready()
    const buf = Buffer.alloc(fileSize).fill('abcdefg')
    for (let i = 0; i < 10; i++) {
      await blobs.put('foo' + i, buf)
    }

    // task
    b.tic()
    for (let j = 0; j < b.iterations; j++) {
      const id = 'foo' + (j % 10)
      await blobs.get(id)
    }
    b.toc()
    await core.close()
    await cleanup(dir)
    b.end()
  })

  bench(`Hyperblobs: Read ${toMb(fileSize)} file`, async (b) => {
    // setup
    const dir = tempy.directory()
    const core = new Hypercore10(dir)
    // Use same default block size as hyperblobbee (512KB)
    const blobs = new Hyperblobs(core, { blockSize: 1024 * 512 })
    await core.ready()
    const buf = Buffer.alloc(fileSize).fill('abcdefg')

    // task
    const ids = []
    for (let i = 0; i < 10; i++) {
      const id = await blobs.put(buf)
      ids.push(id)
    }

    // task
    b.tic()
    for (let j = 0; j < b.iterations; j++) {
      await blobs.get(ids[j % 10])
    }
    b.toc()

    // cleanup
    await core.close()
    await cleanup(dir)
    b.end()
  })

  bench(`Hyperdrive: Read ${toMb(fileSize)} file`, async (b) => {
    // setup
    const dir = tempy.directory()
    const drive = new Hyperdrive(dir)
    const buf = Buffer.alloc(fileSize).fill('abcdefg')
    await once(drive, 'ready')
    for (let i = 0; i < 10; i++) {
      await promisify(drive.writeFile).call(drive, 'foo' + i, buf)
    }

    // task
    b.tic()
    for (let j = 0; j < b.iterations; j++) {
      await promisify(drive.readFile).call(drive, 'foo' + (j % 10))
    }
    b.toc()

    // cleanup
    await promisify(drive.close).call(drive)
    await cleanup(dir)
    b.end()
  })
}
