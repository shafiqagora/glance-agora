const fs = require('fs')
const path = require('path')
const zlib = require('zlib')

const Outputdir = path.join(__dirname, './validationOutput/')

const DATA = require('./jcrewnewdata.json')
const { filterValidProducts } = require('./validate-catalog')

const main = async () => {
  // Filter out invalid products
  const filterResults = filterValidProducts(DATA.products)
  const finalProducts = filterResults.validProducts

  console.log(
    `📊 Validation: ${filterResults.validCount} valid, ${filterResults.invalidCount} invalid out of ${filterResults.totalCount} total products`
  )

  const jsonFilePath = path.join(Outputdir, 'catalog.json')
  const jsonlFilePath = path.join(Outputdir, 'catalog.jsonl')
  const gzippedFilePath = `${jsonlFilePath}.gz`
  fs.writeFileSync(jsonFilePath, JSON.stringify(finalProducts, null, 2), 'utf8')
  const jsonlContent = finalProducts
    .map((product) => JSON.stringify(product))
    .join('\n')
  fs.writeFileSync(jsonlFilePath, jsonlContent, 'utf8')

  const jsonlBuffer = fs.readFileSync(jsonlFilePath)
  const gzippedBuffer = zlib.gzipSync(jsonlBuffer)
  fs.writeFileSync(gzippedFilePath, gzippedBuffer)

  console.log('Generated jsonl, json and gz file after validaton')
}

main()
