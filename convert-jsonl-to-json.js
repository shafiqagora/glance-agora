#!/usr/bin/env node

const fs = require('fs')
const readline = require('readline')
const path = require('path')

/**
 * Convert JSONL file to JSON array format
 * @param {string} inputFile - Path to the input JSONL file
 * @param {string} outputFile - Path to the output JSON file (optional)
 */
async function convertJsonlToJson(inputFile, outputFile) {
  try {
    // Validate input file exists
    if (!fs.existsSync(inputFile)) {
      throw new Error(`Input file does not exist: ${inputFile}`)
    }

    // Generate output filename if not provided
    if (!outputFile) {
      const parsedPath = path.parse(inputFile)
      outputFile = path.join(parsedPath.dir, `${parsedPath.name}.json`)
    }

    console.log(`Converting ${inputFile} to ${outputFile}...`)

    const fileStream = fs.createReadStream(inputFile)
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    })

    const jsonArray = []
    let lineCount = 0
    let errorCount = 0

    // Process each line
    for await (const line of rl) {
      lineCount++

      // Skip empty lines
      if (line.trim() === '') {
        continue
      }

      try {
        const jsonObject = JSON.parse(line)
        jsonArray.push(jsonObject)
      } catch (parseError) {
        errorCount++
        console.warn(
          `Warning: Failed to parse line ${lineCount}: ${parseError.message}`
        )
        console.warn(`Line content: ${line.substring(0, 100)}...`)
      }
    }

    // Write the JSON array to output file
    const jsonString = JSON.stringify(jsonArray, null, 2)
    fs.writeFileSync(outputFile, jsonString, 'utf8')

    console.log(`✅ Conversion completed successfully!`)
    console.log(`📊 Statistics:`)
    console.log(`   - Total lines processed: ${lineCount}`)
    console.log(`   - Valid JSON objects: ${jsonArray.length}`)
    console.log(`   - Parse errors: ${errorCount}`)
    console.log(`   - Output file: ${outputFile}`)
    console.log(
      `   - Output file size: ${(
        fs.statSync(outputFile).size /
        1024 /
        1024
      ).toFixed(2)} MB`
    )
  } catch (error) {
    console.error(`❌ Error during conversion: ${error.message}`)
    process.exit(1)
  }
}

// CLI usage
if (require.main === module) {
  const args = process.argv.slice(2)

  if (args.length === 0) {
    console.log(`
Usage: node convert-jsonl-to-json.js <input-file> [output-file]

Examples:
  node convert-jsonl-to-json.js cataloglulu.jsonl
  node convert-jsonl-to-json.js cataloglulu.jsonl cataloglulu-converted.json

If output-file is not specified, it will be generated automatically by replacing
the .jsonl extension with .json
        `)
    process.exit(1)
  }

  const inputFile = args[0]
  const outputFile = args[1]

  convertJsonlToJson(inputFile, outputFile)
}

module.exports = { convertJsonlToJson }
