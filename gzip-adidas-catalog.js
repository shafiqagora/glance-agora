const fs = require('fs')
const zlib = require('zlib')
const path = require('path')

// Input and output file paths
const inputFile = 'adidas-catalog.jsonl'
const outputFile = 'adidas-catalog.jsonl.gz'

console.log('Starting gzip compression...')
console.log(`Input file: ${inputFile}`)
console.log(`Output file: ${outputFile}`)

// Check if input file exists
if (!fs.existsSync(inputFile)) {
  console.error(`Error: Input file '${inputFile}' does not exist.`)
  process.exit(1)
}

// Get input file size for progress tracking
const inputStats = fs.statSync(inputFile)
const inputSize = inputStats.size
console.log(`Input file size: ${(inputSize / 1024 / 1024).toFixed(2)} MB`)

// Create read stream, gzip transform, and write stream
const readStream = fs.createReadStream(inputFile)
const gzipStream = zlib.createGzip({ level: 6 }) // Good balance of compression and speed
const writeStream = fs.createWriteStream(outputFile)

// Track progress
let bytesProcessed = 0
readStream.on('data', (chunk) => {
  bytesProcessed += chunk.length
  const progress = ((bytesProcessed / inputSize) * 100).toFixed(1)
  process.stdout.write(`\rProgress: ${progress}%`)
})

// Handle completion
writeStream.on('finish', () => {
  const outputStats = fs.statSync(outputFile)
  const outputSize = outputStats.size
  const compressionRatio = ((1 - outputSize / inputSize) * 100).toFixed(1)

  console.log('\n✅ Compression completed successfully!')
  console.log(`Output file size: ${(outputSize / 1024 / 1024).toFixed(2)} MB`)
  console.log(`Compression ratio: ${compressionRatio}%`)
  console.log(
    `Saved: ${((inputSize - outputSize) / 1024 / 1024).toFixed(2)} MB`
  )
})

// Handle errors
readStream.on('error', (err) => {
  console.error('\n❌ Error reading input file:', err.message)
  process.exit(1)
})

gzipStream.on('error', (err) => {
  console.error('\n❌ Error during compression:', err.message)
  process.exit(1)
})

writeStream.on('error', (err) => {
  console.error('\n❌ Error writing output file:', err.message)
  process.exit(1)
})

// Start the compression pipeline
readStream.pipe(gzipStream).pipe(writeStream)
