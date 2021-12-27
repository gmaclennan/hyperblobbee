const FILE_SIZES = [
  1024 * 1024, // 1MB
  10 * 1024 * 1024, // 10MB
  100 * 1024 * 1024, // 100MB
]

for (const fileSize of FILE_SIZES) {
  require('./write')(fileSize)
  require('./read')(fileSize)
}
