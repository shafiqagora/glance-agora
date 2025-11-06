require('dotenv').config()
const axios = require('axios')
const sanitizeHtml = require('sanitize-html')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const fs = require('fs')
const zlib = require('zlib')
const path = require('path')
const { connectDB, disconnectDB } = require('./database/connection')
const Product = require('./models/Product')
const Store = require('./models/Store')
const mongoose = require('mongoose')
let stores = require('./shopifyBatch6.json')
const _ = require('lodash')
const {
  useDigitalOceanAI,
  cleanAndTruncate,
  fetchProductVariants,
  getDomainName,
  calculateDiscount,
  extractSize,
  extractColor,
  determineProductDetails,
  retryRequestWithProxyRotation,
  createAxiosInstance,
  determineProductDetailsNYDJ,
  retryPuppeteerWithProxyRotation,
} = require('./utils/helper')

axios.defaults.timeout = 180000

async function generateCSV(products, storeData, correctUrl) {
  const countryCode = storeData.country
  const BATCH_SIZE = 5000
  const allProductIds = []
  let allFormattedProducts = []

  console.log(
    `Processing ${products.length} products in batches of ${BATCH_SIZE}...`
  )
  products = _.uniqBy(products, 'id')
  // Split products into batches
  for (let i = 0; i < products.length; i += BATCH_SIZE) {
    const batch = products.slice(i, i + BATCH_SIZE)
    const batchNumber = Math.floor(i / BATCH_SIZE) + 1
    const totalBatches = Math.ceil(products.length / BATCH_SIZE)

    console.log(
      `\n📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} products)...`
    )

    const formattedProducts = []

    // Format products in current batch
    for (const product of batch) {
      // console.log(`Processing product: ${product.title}`)

      // Fetch variants for this product
      const productUrl = `${correctUrl}/products/${product.handle}`
      const variants = product.variants

      // If no variants found, create a default variant from the product data
      let variantsToProcess = variants

      const formattedProduct = {
        parent_product_id: product.id.toString(),
        name: product.title,
        description: sanitizeHtml(product.body_html || '', {
          allowedTags: [],
          allowedAttributes: {},
        })
          .replace(/&amp;/g, '&')
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>')
          .replace(/&quot;/g, '"')
          .replace(/&#39;/g, "'")
          .replace(/&nbsp;/g, ' ')
          .replace(/[\u0000-\u001F\u007F-\u009F]/g, '') // Remove control characters
          .replace(/[^\x00-\x7F]/g, '') // Remove non-ASCII characters
          .replace(/\s+/g, ' ') // Normalize whitespace
          .trim(),
        category: '',
        retailer_domain: getDomainName(correctUrl),
        brand: product.vendor,
        gender: '',
        materials: '',
        return_policy_link: storeData.returnPolicy,
        return_policy: '',
        size_chart: '',
        available_bank_offers: '',
        available_coupons: '',
        variants: [],
        operation_type: 'INSERT',
        source: 'shopify',
        _id: new mongoose.Types.ObjectId(),
      }

      for (const variant of variantsToProcess) {
        const originalPrice = parseFloat(
          variant.compare_at_price || variant.price || 0
        )
        const sellingPrice = parseFloat(variant.price || 0)
        const salePrice =
          variant.compare_at_price && variant.compare_at_price > sellingPrice
            ? sellingPrice
            : null
        const finalPrice = sellingPrice
        const discount = calculateDiscount(originalPrice, finalPrice)
        const isOnSale =
          variant.compare_at_price && variant.compare_at_price > sellingPrice
            ? true
            : false
        const isInStock = variant.available || false

        // Get variant image or fallback to product images
        let imageUrl = ''
        let alternateImages = []

        if (variant.featured_image && variant.featured_image.src) {
          imageUrl = variant.featured_image.src
        } else if (product.images && product.images.length > 0) {
          imageUrl = product.images[0].src
        }

        // Collect alternate images
        if (product.images && product.images.length > 1) {
          alternateImages = product.images
            .slice(1)
            .map((img) => img.src)
            .filter((src) => src !== imageUrl)
        }
        const extractedSize = extractSize(variant, product.options)
        const extractedColor = extractColor(variant, product.options)

        const formattedVariant = {
          variant_description: '',
          price_currency: storeData.currency,
          original_price: originalPrice,
          link_url: `${productUrl}?variant=${variant.id}`,
          deeplink_url: `${productUrl}?variant=${variant.id}`,
          image_url: imageUrl,
          alternate_image_urls: alternateImages,
          is_on_sale: isOnSale,
          is_in_stock: isInStock,
          size: extractedSize,
          color: extractedColor,
          mpn: uuidv5(
            `${product.id}-${extractedColor || 'NO_COLOR'}`,
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
          ),
          ratings_count: 0,
          average_ratings: 0,
          review_count: 0,
          selling_price: sellingPrice,
          sale_price: salePrice,
          final_price: finalPrice,
          discount: discount,
          operation_type: 'INSERT',
          variant_id: variant.id.toString(),
        }

        formattedProduct.variants.push(formattedVariant)
      }

      formattedProducts.push(formattedProduct)
    }

    // Insert current batch to MongoDB
    let batchProductIds = formattedProducts.map((item) => item._id.toString())
    if (formattedProducts.length > 0) {
      try {
        await Product.insertMany(formattedProducts)
        console.log(
          `✅ Successfully inserted batch ${batchNumber} (${batchProductIds.length} products) to MongoDB`
        )

        // Add to overall collections
        allProductIds.push(...batchProductIds)
        allFormattedProducts.push(...formattedProducts)
      } catch (error) {
        console.error(
          `❌ Error inserting batch ${batchNumber} to MongoDB:`,
          error.message
        )
        throw error
      }
    }

    // Add small delay between batches to avoid overwhelming the system
    if (batchNumber < totalBatches) {
      console.log('Waiting 2 seconds before processing next batch...')
      await new Promise((resolve) => setTimeout(resolve, 1000))
    }
  }

  console.log(
    `\n✅ All batches processed! Total: ${allFormattedProducts.length} products`
  )

  // Create store entry after all products are processed
  try {
    const storeResult = await createStoreEntry(
      storeData,
      correctUrl,
      allProductIds
    )
    console.log(`✅ Store entry created: ${storeResult.name}`)
  } catch (error) {
    console.error(`❌ Error creating store entry:`, error.message)
  }

  // Create directory structure: countryCode/retailername-countryCode/
  const cleanBrandName = (storeData.name || getDomainName(correctUrl))
    .replace(/[^a-zA-Z0-9.-]/g, '-')
    .toLowerCase()
  const dirPath = path.join(
    __dirname,
    'output',
    countryCode,
    `${cleanBrandName}-${countryCode}`
  )

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }

  allFormattedProducts = allFormattedProducts.map((item) => {
    const { _id, ...itemWithoutId } = item
    return itemWithoutId
  })

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, 'catalog.json')
  const catalogData = {
    store_info: {
      name: storeData.name || getDomainName(correctUrl),
      domain: getDomainName(correctUrl),
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: allFormattedProducts.length,
      crawled_at: new Date().toISOString(),
    },
    products: allFormattedProducts,
  }

  fs.writeFileSync(jsonFilePath, JSON.stringify(catalogData, null, 2), 'utf8')
  console.log(`JSON file generated: ${jsonFilePath}`)

  // Create JSONL file (each product on a separate line)
  const jsonlFilePath = path.join(dirPath, 'catalog.jsonl')
  const jsonlContent = allFormattedProducts
    .map((product) => JSON.stringify(product))
    .join('\n')
  fs.writeFileSync(jsonlFilePath, jsonlContent, 'utf8')
  console.log(`JSONL file generated: ${jsonlFilePath}`)

  // Gzip the JSONL file
  const gzippedFilePath = `${jsonlFilePath}.gz`
  const jsonlBuffer = fs.readFileSync(jsonlFilePath)
  const gzippedBuffer = zlib.gzipSync(jsonlBuffer)
  fs.writeFileSync(gzippedFilePath, gzippedBuffer)
  console.log(`Gzipped JSONL file generated: ${gzippedFilePath}`)

  console.log(`\n📊 MongoDB Results: ${allProductIds.length} products inserted`)

  return { jsonPath: gzippedFilePath, productIds: allProductIds }
}

// Create store entry in MongoDB
async function createStoreEntry(storeData, correctUrl, productIds) {
  try {
    // Check if store already exists
    const existingStore = await Store.findOne({
      storeUrl: correctUrl,
    })

    if (existingStore) {
      console.log(
        `Store ${storeData.name} already exists, updating with new products...`
      )

      // Add new product IDs to existing store (avoid duplicates)
      const newProductIds = productIds.filter(
        (id) => !existingStore.products.includes(id)
      )
      if (newProductIds.length > 0) {
        existingStore.products.push(...newProductIds)
        existingStore.isScrapped = true
        await existingStore.save()
        console.log(`Updated store with ${newProductIds.length} new products`)
      }

      return existingStore
    }

    // Create new store entry
    const storeEntry = new Store({
      name: storeData.name,
      storeUrl: correctUrl,
      city: storeData.city,
      state: storeData.province || storeData.state,
      country: storeData.country,
      products: productIds,
      isScrapped: true,
      storeType: 'shopify',
      returnPolicy: storeData.returnPolicy,
    })

    await storeEntry.save()
    console.log(`✅ Created new store entry: ${storeData.name}`)
    return storeEntry
  } catch (error) {
    console.error(
      `❌ Error creating store entry for ${storeData.name}:`,
      error.message
    )
    throw error
  }
}

// Function to find the correct URL format
async function findCorrectUrl(inputUrl) {
  console.log(`🔍 Finding correct URL format for: ${inputUrl}`)

  // Clean the input URL
  let baseUrl = inputUrl.replace(/^https?:\/\//, '').replace(/\/$/, '')

  // Generate possible URL variations
  const urlVariations = [
    `https://${baseUrl}`,
    `https://www.${baseUrl}`,
    `http://${baseUrl}`,
    `http://www.${baseUrl}`,
  ]

  // If the baseUrl already starts with www, also try without www
  if (baseUrl.startsWith('www.')) {
    const withoutWww = baseUrl.replace(/^www\./, '')
    urlVariations.push(`https://${withoutWww}`)
    urlVariations.push(`http://${withoutWww}`)
  }

  // Remove duplicates
  const uniqueUrls = [...new Set(urlVariations)]

  console.log(`Testing ${uniqueUrls.length} URL variations...`)

  // Test each URL variation
  for (const testUrl of uniqueUrls) {
    try {
      console.log(`Testing: ${testUrl}`)

      const isWorking = await retryPuppeteerWithProxyRotation(
        async (browser) => {
          const page = await browser.newPage()

          await page.setUserAgent(
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
          )

          await page.setExtraHTTPHeaders({
            Accept: 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
          })

          try {
            // Test if the products.json endpoint works (this is the most reliable test for Shopify stores)
            const response = await page.goto(
              `${testUrl}/products.json?limit=1`,
              {
                waitUntil: 'networkidle2',
                timeout: 30000,
              }
            )

            if (response.status() === 200) {
              const responseBody = await response.text()
              const data = JSON.parse(responseBody)

              // Check if it's a valid Shopify response
              if (data && data.products !== undefined) {
                console.log(`✅ Working URL found: ${testUrl}`)
                return true
              }
            }

            return false
          } catch (error) {
            console.log(`❌ Failed to test ${testUrl}: ${error.message}`)
            return false
          }
        },
        3,
        2000,
        'US' // Default to US for URL testing
      )

      if (isWorking) {
        return testUrl
      }
    } catch (error) {
      console.log(`❌ Error testing ${testUrl}: ${error.message}`)
      continue
    }
  }

  console.log(`❌ No working URL format found for ${inputUrl}`)
  return null
}

const main = async (SITE_URL, returnPolicy = '') => {
  try {
    // Connect to MongoDB

    // Find the correct URL format
    let correctUrl = await findCorrectUrl(SITE_URL)
    if (!correctUrl) {
      console.log(`❌ Could not find working URL format for ${SITE_URL}`)
      return false
    }

    let DOMAIN = getDomainName(correctUrl)
    console.log(`✅ Using URL: ${correctUrl}`)

    let allProducts = []
    let loopCondition = true
    let pageCounter = 1
    let storeData = {}

    try {
      const metaData = await retryPuppeteerWithProxyRotation(
        async (browser) => {
          const page = await browser.newPage()

          await page.setUserAgent(
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
          )

          await page.setExtraHTTPHeaders({
            Accept: 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
          })

          const response = await page.goto(`${correctUrl}/meta.json`, {
            waitUntil: 'networkidle2',
            timeout: 60000,
          })

          const responseBody = await response.text()
          return JSON.parse(responseBody)
        },
        3,
        2000,
        'US' // Default to US for metadata fetching, will be updated after we get the country
      )

      storeData = {
        ...metaData,
        region: metaData.country,
        returnPolicy,
        url: correctUrl,
      }
    } catch (err) {
      console.log('Could not fetch store metadata, using defaults')

      return
    }
    if (storeData.country !== 'US' && storeData.country !== 'IN') {
      console.log('Skipping non-US/IN store')
      return false
    }

    console.log(`Starting to crawl ${storeData.name || DOMAIN}...`)

    // Check if store already exists in database
    const existingStore = await Store.findOne({
      storeUrl: storeData.name,
    })

    if (existingStore) {
      console.log(
        `Store ${existingStore.name} (${correctUrl}) already exists and has been scrapped. Skipping...`
      )
      return { skipped: true, reason: 'Already exists in database' }
    }

    // Fetch all products
    do {
      try {
        const productsData = await retryPuppeteerWithProxyRotation(
          async (browser) => {
            const page = await browser.newPage()

            await page.setUserAgent(
              'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )

            await page.setExtraHTTPHeaders({
              Accept: 'application/json, text/plain, */*',
              'Accept-Language': 'en-US,en;q=0.9',
              'Accept-Encoding': 'gzip, deflate, br',
            })

            const response = await page.goto(
              `${correctUrl}/products.json?page=${pageCounter}&limit=250`,
              {
                waitUntil: 'networkidle2',
                timeout: 60000,
              }
            )

            const responseBody = await response.text()
            return JSON.parse(responseBody)
          },
          3,
          2000,
          storeData.country || 'US' // Use store country or default to US
        )

        const products = productsData?.products

        if (!products || products.length === 0) {
          console.log(`Ending at page ${pageCounter}`)
          loopCondition = false
          break
        } else {
          allProducts.push(...products)

          console.log(
            `Fetched ${products.length} products from page ${pageCounter}`
          )
        }
      } catch (err) {
        console.log(`Error fetching page ${pageCounter}:`, err.message)
        console.log(
          `${correctUrl}/products.json?page=${pageCounter}`,
          'Request failed'
        )
        // Continue to next page or stop based on error type
        if (
          err.message &&
          (err.message.includes('404') || err.message.includes('Not Found'))
        ) {
          console.log('Reached end of products (404)')
          loopCondition = false
          break
        }
      }

      pageCounter += 1
    } while (loopCondition)

    if (allProducts.length === 0) {
      console.log(`No products found for ${SITE_URL}`)
      return false
    }

    console.log(`Total products found: ${allProducts.length}`)

    // Generate CSV and JSON
    const result = await generateCSV(allProducts, storeData, correctUrl)

    console.log(`Successfully generated files for ${storeData.name || DOMAIN}`)
    console.log(`JSON saved at: ${result.jsonPath}`)

    return { result, storeData }
  } catch (e) {
    console.error('Error in scraping Shopify store:', e)
    return false
  } finally {
    // Disconnect from MongoDB
  }
}

// Process all stores from the JSON file
async function processAllStores() {
  console.log(`Starting to process ${stores.length} stores...`)
  await connectDB()

  const results = {
    successful: [],
    failed: [],
    skipped: [],
    total: stores.length,
    mongodb_summary: {
      total_inserted: 0,
      total_skipped: 0,
      total_errors: 0,
    },
  }

  for (let i = 0; i < stores.length; i++) {
    const store = stores[i]
    console.log(
      `\n[${i + 1}/${stores.length}] Processing ${store.brandName} (${
        store.url
      })...`
    )

    try {
      // Map region to country code

      const storeResult = await main(store.url, store.returnPolicy)

      if (storeResult) {
        results.successful.push({
          brandName: storeResult.storeData.name,
          url: storeResult.storeData.url,
          region: storeResult.storeData.region,
          jsonPath: storeResult.result.jsonPath,
        })
        console.log(`✅ Successfully processed ${store.brandName}`)
      } else {
        results.failed.push({
          brandName: store.brandName,
          url: store.url,
          region: store.region,
          error: 'Processing failed',
        })
        console.log(`❌ Failed to process ${store.brandName}`)
      }
    } catch (error) {
      results.failed.push({
        brandName: store.brandName,
        url: store.url,
        region: store.region,
        error: error.message,
      })
      console.log(`❌ Error processing ${store.brandName}: ${error.message}`)
    }

    // Add a small delay between requests to be respectful
    await new Promise((resolve) => setTimeout(resolve, 2000))
  }

  // Generate summary report
  console.log('\n' + '='.repeat(80))
  console.log('PROCESSING SUMMARY')
  console.log('='.repeat(80))
  console.log(`Total stores: ${results.total}`)
  console.log(`Successful: ${results.successful.length}`)
  console.log(`Skipped: ${results.skipped.length}`)
  console.log(`Failed: ${results.failed.length}`)

  if (results.successful.length > 0) {
    console.log('\n✅ Successfully processed stores:')
    results.successful.forEach((store) => {
      console.log(`  - ${store.brandName} (${store.region}):`)
      console.log(`    JSON: ${store.jsonPath}`)
    })
  }

  if (results.failed.length > 0) {
    console.log('\n❌ Failed stores:')
    results.failed.forEach((store) => {
      console.log(`  - ${store.brandName} (${store.region}): ${store.error}`)
    })
  }

  // Save results to JSON file
  const resultsPath = path.join(__dirname, 'processing-results.json')
  fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2))
  console.log(`\nDetailed results saved to: ${resultsPath}`)

  disconnectDB()
  return results
}

// Export the main function and processAllStores
module.exports = {
  main,
  processAllStores,
  getDomainName,
  calculateDiscount,
  extractSize,
  extractColor,
  determineProductDetails,
  fetchProductVariants,
  cleanAndTruncate,
}

// If run directly from command line
if (require.main === module) {
  const args = process.argv.slice(2)

  console.log('Processing all stores from shopifyStores.json...')
  processAllStores()
    .then((results) => {
      console.log('\nAll stores processed!')
      if (results.failed.length > 0) {
        console.log(
          `⚠️  ${results.failed.length} stores failed. Check processing-results.json for details.`
        )
        process.exit(1)
      } else {
        console.log('🎉 All stores processed successfully!')
      }
    })
    .catch((error) => {
      console.error('Error processing stores:', error)
      process.exit(1)
    })
}
